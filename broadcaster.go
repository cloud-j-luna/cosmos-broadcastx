package broadcastx

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

// run processes the transaction queue, grouping messages and broadcasting them sequentially.
// While broadcasting one transaction, it continues collecting messages for the next transaction.
func (b *Broadcaster) run() {
	defer b.wg.Done()

	for {
		select {
		case <-b.ctx.Done():
			b.flushPending()
			return
		case req := <-b.queue:
			b.mu.Lock()
			b.pending = append(b.pending, req.Messages...)
			b.pendingReqs = append(b.pendingReqs, req)
			pending := b.pending
			pendingReqs := b.pendingReqs
			b.pending = nil
			b.pendingReqs = nil
			b.mu.Unlock()

			// Start collecting messages for the next batch in parallel with broadcasting
			nextBatchCh := make(chan struct {
				msgs []sdk.Msg
				reqs []TransactionRequest
			}, 1)

			// Start goroutine to collect messages while broadcasting
			go func() {
				// Collect messages that arrive during the broadcast
				// Keep collecting until broadcast completes
				collectedMsgs := []sdk.Msg{}
				collectedReqs := []TransactionRequest{}

				// Wait a bit to see if more messages arrive quickly
				timeout := time.After(50 * time.Millisecond)
				collecting := true

				for collecting {
					select {
					case req := <-b.queue:
						collectedMsgs = append(collectedMsgs, req.Messages...)
						collectedReqs = append(collectedReqs, req)
						// Reset timeout to continue collecting
						timeout = time.After(50 * time.Millisecond)
					case <-timeout:
						collecting = false
					case <-b.ctx.Done():
						collecting = false
					}
				}

				nextBatchCh <- struct {
					msgs []sdk.Msg
					reqs []TransactionRequest
				}{collectedMsgs, collectedReqs}
			}()

			// Broadcast the current batch (this blocks until transaction is included)
			b.broadcastGrouped(pending, pendingReqs)

			// Get the messages that were collected during the broadcast
			select {
			case nextBatch := <-nextBatchCh:
				if len(nextBatch.msgs) > 0 {
					// Broadcast the next batch immediately
					b.broadcastGrouped(nextBatch.msgs, nextBatch.reqs)
					// Continue collecting and broadcasting in a loop
					b.continueBroadcasting()
				}
			case <-b.ctx.Done():
				return
			}
		}
	}
}

// continueBroadcasting continuously collects and broadcasts messages.
// This is called after the initial broadcast to keep processing queued messages.
func (b *Broadcaster) continueBroadcasting() {
	for {
		// Collect messages
		collectedMsgs := []sdk.Msg{}
		collectedReqs := []TransactionRequest{}

		timeout := time.After(50 * time.Millisecond)
		collecting := true

		for collecting {
			select {
			case req := <-b.queue:
				collectedMsgs = append(collectedMsgs, req.Messages...)
				collectedReqs = append(collectedReqs, req)
				timeout = time.After(50 * time.Millisecond)
			case <-timeout:
				collecting = false
			case <-b.ctx.Done():
				return
			}
		}

		if len(collectedMsgs) == 0 {
			// No more messages, return to main loop
			return
		}

		// Start collecting next batch while broadcasting current batch
		nextBatchCh := make(chan struct {
			msgs []sdk.Msg
			reqs []TransactionRequest
		}, 1)

		go func() {
			collectedMsgs := []sdk.Msg{}
			collectedReqs := []TransactionRequest{}

			timeout := time.After(50 * time.Millisecond)
			collecting := true

			for collecting {
				select {
				case req := <-b.queue:
					collectedMsgs = append(collectedMsgs, req.Messages...)
					collectedReqs = append(collectedReqs, req)
					timeout = time.After(50 * time.Millisecond)
				case <-timeout:
					collecting = false
				case <-b.ctx.Done():
					collecting = false
				}
			}

			nextBatchCh <- struct {
				msgs []sdk.Msg
				reqs []TransactionRequest
			}{collectedMsgs, collectedReqs}
		}()

		// Broadcast current batch
		b.broadcastGrouped(collectedMsgs, collectedReqs)

		// Get next batch
		select {
		case nextBatch := <-nextBatchCh:
			if len(nextBatch.msgs) > 0 {
				collectedMsgs = nextBatch.msgs
				collectedReqs = nextBatch.reqs
				// Continue loop to broadcast this batch
				continue
			} else {
				return
			}
		case <-b.ctx.Done():
			return
		}
	}
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

	b.logger.InfoContext(b.ctx, "Broadcasting grouped transaction",
		"account", b.accountKey.Address,
		"chain_id", b.accountKey.ChainID,
		"msg_count", len(msgs),
		"req_count", len(reqs),
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
			"account", b.accountKey.Address,
			"error", err.Error(),
			"msg_count", len(msgs),
		)
	} else {
		b.logger.InfoContext(b.ctx, "Transaction broadcast successful",
			"account", b.accountKey.Address,
			"msg_count", len(msgs),
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
