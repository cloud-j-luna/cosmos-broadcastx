// Package broadcaster enhances the Cosmos SDK broadcasts.
package broadcaster

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	sdk "github.com/cosmos/cosmos-sdk/types"
)

// BroadcastFunc is a function that broadcasts messages to the blockchain.
// It takes a context and messages, and returns the transaction response and any error.
type BroadcastFunc func(ctx context.Context, msgs []sdk.Msg) (interface{}, error)

// AccountKey uniquely identifies an account configuration for transaction broadcasting.
// Transactions are grouped and queued per account to ensure only one transaction
// per account is broadcast per block.
type AccountKey struct {
	Address string
	ChainID string
	NodeURI string
}

// TransactionRequest represents a request to broadcast messages.
type TransactionRequest struct {
	Messages []sdk.Msg
	ResultCh chan<- TransactionResult
}

// TransactionResult contains the result of a broadcast transaction.
type TransactionResult struct {
	Response interface{}
	Error    error
}

type broadcastLogContext struct {
	accountKey AccountKey
	msgCount   int
	reqCount   int
}

// Broadcaster manages transaction queuing and broadcasting for accounts.
// It ensures that only one transaction per account is broadcast at a time,
// and groups multiple messages from the same account into a single transaction.
type Broadcaster struct {
	broadcastFunc BroadcastFunc
	accountKey    AccountKey
	fromAddress   string
	queue         chan TransactionRequest
	wg            sync.WaitGroup
	ctx           context.Context
	cancel        context.CancelFunc
	logger        *slog.Logger
	mu            sync.Mutex
	pending       []sdk.Msg
	pendingReqs   []TransactionRequest
	broadcasting  bool
}

// NewBroadcaster creates a new broadcaster for the given account.
// The broadcaster uses a background context for long-running operations,
// independent of the Terraform operation context to avoid premature cancellation.
// The logger parameter accepts an slog.Logger.
// The broadcastFunc is called to broadcast messages to the blockchain.
// The fromAddress is the address of the account that will sign transactions.
func NewBroadcaster(ctx context.Context, broadcastFunc BroadcastFunc, accountKey AccountKey, fromAddress string, logger *slog.Logger) *Broadcaster {
	// Use background context so broadcasts aren't canceled when Terraform operations complete
	// We still track the original context for shutdown signals
	bctx, cancel := context.WithCancel(context.Background())

	// Use default logger if none provided
	if logger == nil {
		logger = slog.Default()
	}

	b := &Broadcaster{
		broadcastFunc: broadcastFunc,
		accountKey:    accountKey,
		fromAddress:   fromAddress,
		queue:         make(chan TransactionRequest, 100),
		ctx:           bctx,
		cancel:        cancel,
		logger:        logger,
	}
	b.wg.Add(1)
	go b.run()
	return b
}

// Broadcast queues messages for broadcasting. If there are already pending messages
// for this account, the new messages will be grouped with them and broadcast together.
// Returns a channel that will receive the transaction result.
func (b *Broadcaster) Broadcast(ctx context.Context, msgs []sdk.Msg) <-chan TransactionResult {
	resultCh := make(chan TransactionResult, 1)

	// Check context cancellation first to ensure we respect it
	select {
	case <-ctx.Done():
		resultCh <- TransactionResult{Error: ctx.Err()}
		close(resultCh)
		return resultCh
	case <-b.ctx.Done():
		resultCh <- TransactionResult{Error: b.ctx.Err()}
		close(resultCh)
		return resultCh
	default:
		// Context is not canceled, proceed to queue
	}

	// Try to queue with a non-blocking send
	select {
	case b.queue <- TransactionRequest{Messages: msgs, ResultCh: resultCh}:
		// Successfully queued
	default:
		// Queue is full - this shouldn't happen often, but handle it
		resultCh <- TransactionResult{Error: fmt.Errorf("broadcaster queue is full")}
		close(resultCh)
		return resultCh
	}

	return resultCh
}

// run processes the transaction queue. It waits for the first transaction request,
// then transitions to continuous broadcasting mode to drain the queue efficiently.
// This design allows the broadcaster to be idle (blocking on the queue) when there's
// no traffic, but switch to active polling mode when messages start arriving.
func (b *Broadcaster) run() {
	defer b.wg.Done()

	for {
		select {
		case <-b.ctx.Done():
			b.flushPending()
			return
		case req := <-b.queue:
			// First message arrived - add to pending and immediately start continuous broadcasting
			b.mu.Lock()
			b.pending = append(b.pending, req.Messages...)
			b.pendingReqs = append(b.pendingReqs, req)
			b.mu.Unlock()

			// Switch to active mode: drain the queue with overlapped collection/broadcast
			b.continueBroadcasting()
			// When continueBroadcasting returns, queue is empty - go back to blocking wait
		}
	}
}

// continueBroadcasting actively drains the queue, broadcasting batches with overlapped
// collection. This method runs in a loop until the queue is empty, optimizing throughput
// by collecting the next batch while broadcasting the current batch.
//
// The workflow for each iteration:
// 1. Collect pending messages from the queue (with 50ms timeout)
// 2. If messages collected, start a goroutine to collect the NEXT batch in parallel
// 3. Broadcast the current batch (blocks until transaction confirms)
// 4. Retrieve the next batch that was collected during step 3
// 5. If next batch has messages, continue the loop; otherwise return to idle mode
//
// This overlapping design minimizes idle time between broadcasts, ensuring back-to-back
// transactions are sent as quickly as the blockchain can process them.
func (b *Broadcaster) continueBroadcasting() {
	var currentMsgs []sdk.Msg
	var currentReqs []TransactionRequest

	for {
		// Collect current batch of messages (if we don't already have one from previous iteration)
		if len(currentMsgs) == 0 {
			currentMsgs, currentReqs = b.collectBatch()

			// If no messages were collected, queue is empty - return to idle mode
			if len(currentMsgs) == 0 {
				return
			}
		}

		b.logger.InfoContext(b.ctx, "broadcasting batch",
			"message_count", len(currentMsgs),
			"request_count", len(currentReqs))

		// Start collecting next batch in parallel while we broadcast current batch
		nextBatchCh := make(chan struct {
			msgs []sdk.Msg
			reqs []TransactionRequest
		}, 1)

		go func() {
			msgs, reqs := b.collectBatch()
			nextBatchCh <- struct {
				msgs []sdk.Msg
				reqs []TransactionRequest
			}{msgs, reqs}
		}()

		// Broadcast current batch (blocks until transaction is included in a block)
		b.broadcastGrouped(currentMsgs, currentReqs)

		// Retrieve the next batch that was collected during broadcast
		select {
		case nextBatch := <-nextBatchCh:
			if len(nextBatch.msgs) == 0 {
				// No more messages arrived - return to idle mode
				return
			}
			// Next batch has messages - set them as current and continue loop to broadcast them
			currentMsgs = nextBatch.msgs
			currentReqs = nextBatch.reqs
		case <-b.ctx.Done():
			return
		}
	}
}

// collectBatch collects messages from the queue with a timeout.
// It drains any pending messages from b.pending first, then actively polls
// the queue for up to 50ms to batch additional messages together.
// Returns the collected messages and their corresponding requests.
func (b *Broadcaster) collectBatch() ([]sdk.Msg, []TransactionRequest) {
	// First, drain any existing pending messages
	b.mu.Lock()
	msgs := b.pending
	reqs := b.pendingReqs
	b.pending = nil
	b.pendingReqs = nil
	b.mu.Unlock()

	// Then poll the queue for a short time to collect additional messages
	timeout := time.After(50 * time.Millisecond)
	collecting := true

	for collecting {
		select {
		case req := <-b.queue:
			msgs = append(msgs, req.Messages...)
			reqs = append(reqs, req)
			// Reset timeout to continue collecting while messages keep arriving
			timeout = time.After(50 * time.Millisecond)
		case <-timeout:
			collecting = false
		case <-b.ctx.Done():
			collecting = false
		}
	}

	return msgs, reqs
}

// broadcastGrouped broadcasts a group of messages and sends results to all waiting requesters.
func (b *Broadcaster) broadcastGrouped(msgs []sdk.Msg, reqs []TransactionRequest) {
	b.mu.Lock()
	b.broadcasting = true
	b.mu.Unlock()

	defer func() {
		b.mu.Lock()
		b.broadcasting = false
		b.mu.Unlock()
	}()

	b.logger.InfoContext(b.ctx, "broadcasting grouped transaction",
		"broadcast_context", broadcastLogContext{
			accountKey: b.accountKey,
			msgCount:   len(msgs),
			reqCount:   len(reqs),
		},
	)

	txResp, err := b.broadcastFunc(b.ctx, msgs)

	result := TransactionResult{
		Response: txResp,
		Error:    err,
	}

	// Send result to all requesters (non-blocking)
	for _, req := range reqs {
		select {
		case req.ResultCh <- result:
			// Successfully sent
		default:
			// Channel is full or closed - shouldn't happen with buffered channel, but handle gracefully
			b.logger.WarnContext(b.ctx, "Failed to send result to requester",
				"account", b.accountKey.Address,
			)
		}
	}

	if err != nil {
		b.logger.ErrorContext(b.ctx, "Transaction broadcast failed",
			"error", err.Error(),
			"broadcast_context", broadcastLogContext{
				accountKey: b.accountKey,
				msgCount:   len(msgs),
				reqCount:   len(reqs),
			},
		)
	} else {
		b.logger.InfoContext(b.ctx, "Transaction broadcast successful",
			"broadcast_context", broadcastLogContext{
				accountKey: b.accountKey,
				msgCount:   len(msgs),
				reqCount:   len(reqs),
			},
		)
	}
}

// flushPending broadcasts any remaining pending messages.
func (b *Broadcaster) flushPending() {
	b.mu.Lock()
	pending := b.pending
	pendingReqs := b.pendingReqs
	b.pending = nil
	b.pendingReqs = nil
	b.mu.Unlock()

	if len(pending) > 0 {
		b.broadcastGrouped(pending, pendingReqs)
	}
}

// Close stops the broadcaster and waits for all operations to complete.
func (b *Broadcaster) Close() error {
	b.cancel()
	b.wg.Wait()
	return nil
}

// FromAddress returns the address of the account that signs transactions.
func (b *Broadcaster) FromAddress() string {
	return b.fromAddress
}

// Registry manages broadcasters for different accounts.
type Registry struct {
	mu           sync.RWMutex
	broadcasters map[AccountKey]*Broadcaster
	logger       *slog.Logger
}

// NewRegistry creates a new broadcaster registry.
// The logger parameter accepts an slog.Logger, which can be created from tflog
// using tflogadapter.New(ctx) if you want Terraform logging integration.
func NewRegistry(logger *slog.Logger) *Registry {
	if logger == nil {
		logger = slog.Default()
	}
	return &Registry{
		broadcasters: make(map[AccountKey]*Broadcaster),
		logger:       logger,
	}
}

// GetOrCreateBroadcaster returns an existing broadcaster for the given account key,
// or creates a new one if it doesn't exist.
func (r *Registry) GetOrCreateBroadcaster(ctx context.Context, broadcastFunc BroadcastFunc, key AccountKey, fromAddress string) *Broadcaster {
	r.mu.Lock()
	defer r.mu.Unlock()

	if b, exists := r.broadcasters[key]; exists {
		return b
	}

	b := NewBroadcaster(ctx, broadcastFunc, key, fromAddress, r.logger)
	r.broadcasters[key] = b
	return b
}

// GetBroadcaster returns an existing broadcaster for the given account key, or nil if it doesn't exist.
func (r *Registry) GetBroadcaster(key AccountKey) *Broadcaster {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.broadcasters[key]
}

// Close closes all broadcasters in the registry.
func (r *Registry) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	var errs []error
	for _, b := range r.broadcasters {
		if err := b.Close(); err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("errors closing broadcasters: %v", errs)
	}
	return nil
}
