package broadcaster

import (
	"errors"
	"fmt"
	"log/slog"

	sdk "github.com/cosmos/cosmos-sdk/types"
)

// MessageIndexError is an error that carries the index of the failing message
// within a batched transaction. Implementations of BroadcastFunc should return
// errors satisfying this interface so the retry logic can isolate the bad message.
type MessageIndexError interface {
	error
	MessageIndex() int
}

// BroadcastError is a concrete implementation of MessageIndexError.
type BroadcastError struct {
	Err      error
	MsgIndex int
}

func (e *BroadcastError) Error() string {
	return fmt.Sprintf("message %d: %v", e.MsgIndex, e.Err)
}

func (e *BroadcastError) MessageIndex() int {
	return e.MsgIndex
}

func (e *BroadcastError) Unwrap() error {
	return e.Err
}

// Option configures a Broadcaster.
type Option func(*broadcasterConfig)

type broadcasterConfig struct {
	maxRetries int
}

func defaultConfig() broadcasterConfig {
	return broadcasterConfig{maxRetries: 5}
}

// WithMaxRetries sets the maximum number of retry attempts when a batched
// transaction fails with a MessageIndexError. Negative values are ignored.
func WithMaxRetries(n int) Option {
	return func(c *broadcasterConfig) {
		if n >= 0 {
			c.maxRetries = n
		}
	}
}

// requestSpan maps a TransactionRequest to its range in the flattened message slice.
type requestSpan struct {
	reqIndex int
	start    int // inclusive
	end      int // exclusive
}

// buildRequestSpans walks the requests and records the start/end offset of each
// request's messages within the flattened slice.
func buildRequestSpans(reqs []TransactionRequest) []requestSpan {
	spans := make([]requestSpan, len(reqs))
	offset := 0
	for i, req := range reqs {
		spans[i] = requestSpan{
			reqIndex: i,
			start:    offset,
			end:      offset + len(req.Messages),
		}
		offset += len(req.Messages)
	}
	return spans
}

// findOwningRequest returns the index into spans of the request that owns
// the given flattened message index. Returns -1 if out of range.
func findOwningRequest(spans []requestSpan, msgIdx int) int {
	for i, s := range spans {
		if msgIdx >= s.start && msgIdx < s.end {
			return i
		}
	}
	return -1
}

// broadcastWithRetry broadcasts msgs and, on failure, parses the failing message
// index, removes the owning request, and retries with the survivors.
func (b *Broadcaster) broadcastWithRetry(msgs []sdk.Msg, reqs []TransactionRequest) {
	b.mu.Lock()
	b.broadcasting = true
	b.mu.Unlock()

	defer func() {
		b.mu.Lock()
		b.broadcasting = false
		b.mu.Unlock()
	}()

	spans := buildRequestSpans(reqs)

	// activeReqs tracks which request indices are still in play.
	activeReqs := make(map[int]struct{}, len(reqs))
	for i := range reqs {
		activeReqs[i] = struct{}{}
	}

	for attempt := 0; ; attempt++ {
		b.logger.InfoContext(b.ctx, "broadcasting grouped transaction",
			"broadcast_context", broadcastLogContext{
				accountKey: b.accountKey,
				msgCount:   len(msgs),
				reqCount:   len(activeReqs),
			},
		)

		txResp, err := b.broadcastFunc(b.ctx, msgs)

		if err == nil {
			// Success — notify all active requesters.
			result := TransactionResult{Response: txResp}
			sendResult(b.ctx, b.logger, reqs, activeReqs, result)

			b.logger.InfoContext(b.ctx, "Transaction broadcast successful",
				"broadcast_context", broadcastLogContext{
					accountKey: b.accountKey,
					msgCount:   len(msgs),
					reqCount:   len(activeReqs),
				},
			)
			return
		}

		// Failure path.
		b.logger.ErrorContext(b.ctx, "Transaction broadcast failed",
			"error", err.Error(),
			"attempt", attempt+1,
			"broadcast_context", broadcastLogContext{
				accountKey: b.accountKey,
				msgCount:   len(msgs),
				reqCount:   len(activeReqs),
			},
		)

		var mie MessageIndexError
		if !errors.As(err, &mie) {
			// No message index — fail everyone (same as old behavior).
			sendResult(b.ctx, b.logger, reqs, activeReqs, TransactionResult{Error: err})
			return
		}

		ownerIdx := findOwningRequest(spans, mie.MessageIndex())
		if ownerIdx < 0 {
			// Defensive: index out of range — fail everyone.
			sendResult(b.ctx, b.logger, reqs, activeReqs, TransactionResult{Error: err})
			return
		}

		// Fail the bad request.
		sendResultSingle(b.ctx, b.logger, reqs[spans[ownerIdx].reqIndex], TransactionResult{Error: err})
		delete(activeReqs, spans[ownerIdx].reqIndex)

		if len(activeReqs) == 0 {
			return
		}

		// Rebuild msgs and spans from survivors.
		msgs, spans = rebuildFromActive(reqs, activeReqs)

		if attempt >= b.config.maxRetries {
			exhaustedErr := fmt.Errorf("retries exhausted after %d attempts: %w", attempt+1, err)
			sendResult(b.ctx, b.logger, reqs, activeReqs, TransactionResult{Error: exhaustedErr})
			return
		}

		// Check context cancellation between retries.
		select {
		case <-b.ctx.Done():
			sendResult(b.ctx, b.logger, reqs, activeReqs, TransactionResult{Error: b.ctx.Err()})
			return
		default:
		}
	}
}

// rebuildFromActive constructs new msgs and spans slices from only the active requests.
func rebuildFromActive(reqs []TransactionRequest, activeReqs map[int]struct{}) ([]sdk.Msg, []requestSpan) {
	var msgs []sdk.Msg
	var spans []requestSpan
	offset := 0
	for i, req := range reqs {
		if _, ok := activeReqs[i]; !ok {
			continue
		}
		spans = append(spans, requestSpan{
			reqIndex: i,
			start:    offset,
			end:      offset + len(req.Messages),
		})
		msgs = append(msgs, req.Messages...)
		offset += len(req.Messages)
	}
	return msgs, spans
}

// sendResult sends a TransactionResult to every active requester (non-blocking).
func sendResult(ctx interface{ Done() <-chan struct{} }, logger *slog.Logger, reqs []TransactionRequest, activeReqs map[int]struct{}, result TransactionResult) {
	for idx := range activeReqs {
		sendResultSingle(ctx, logger, reqs[idx], result)
	}
}

// sendResultSingle sends a TransactionResult to a single requester (non-blocking).
func sendResultSingle(_ interface{ Done() <-chan struct{} }, logger *slog.Logger, req TransactionRequest, result TransactionResult) {
	select {
	case req.ResultCh <- result:
	default:
		logger.Warn("failed to send result to requester")
	}
}
