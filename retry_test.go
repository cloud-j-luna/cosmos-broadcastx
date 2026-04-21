package broadcaster

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"

	sdk "github.com/cosmos/cosmos-sdk/types"
)

// helper to create a broadcaster with a controlled broadcastFunc (no background goroutine).
func newTestBroadcaster(fn BroadcastFunc, opts ...Option) *Broadcaster {
	cfg := defaultConfig()
	for _, o := range opts {
		o(&cfg)
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Broadcaster{
		broadcastFunc: fn,
		accountKey:    AccountKey{Address: "test", ChainID: "test", NodeURI: "test"},
		fromAddress:   "test",
		queue:         make(chan TransactionRequest, 100),
		ctx:           ctx,
		cancel:        cancel,
		logger:        slog.Default(),
		config:        cfg,
	}
}

func makeReqs(msgCounts ...int) ([]sdk.Msg, []TransactionRequest, []chan TransactionResult) {
	var msgs []sdk.Msg
	reqs := make([]TransactionRequest, len(msgCounts))
	chs := make([]chan TransactionResult, len(msgCounts))
	for i, n := range msgCounts {
		ch := make(chan TransactionResult, 1)
		chs[i] = ch
		var reqMsgs []sdk.Msg
		for j := 0; j < n; j++ {
			m := &mockMsg{value: fmt.Sprintf("req%d-msg%d", i, j)}
			reqMsgs = append(reqMsgs, m)
			msgs = append(msgs, m)
		}
		reqs[i] = TransactionRequest{Messages: reqMsgs, ResultCh: ch}
	}
	return msgs, reqs, chs
}

func TestBroadcastWithRetry_SuccessNoRetry(t *testing.T) {
	callCount := 0
	fn := func(ctx context.Context, msgs []sdk.Msg) (interface{}, error) {
		callCount++
		return "ok", nil
	}
	b := newTestBroadcaster(fn)
	defer b.cancel()

	msgs, reqs, chs := makeReqs(1, 1, 1)
	b.broadcastWithRetry(msgs, reqs)

	if callCount != 1 {
		t.Fatalf("expected 1 broadcast call, got %d", callCount)
	}
	for i, ch := range chs {
		res := <-ch
		if res.Error != nil {
			t.Fatalf("req %d: unexpected error: %v", i, res.Error)
		}
		if res.Response != "ok" {
			t.Fatalf("req %d: expected response 'ok', got %v", i, res.Response)
		}
	}
}

func TestBroadcastWithRetry_FailWithMessageIndex_RetrySucceeds(t *testing.T) {
	call := 0
	fn := func(ctx context.Context, msgs []sdk.Msg) (interface{}, error) {
		call++
		if call == 1 {
			// Fail on msg index 1 (belongs to req 1)
			return nil, &BroadcastError{Err: errors.New("bad msg"), MsgIndex: 1}
		}
		return "retry-ok", nil
	}
	b := newTestBroadcaster(fn)
	defer b.cancel()

	msgs, reqs, chs := makeReqs(1, 1, 1)
	b.broadcastWithRetry(msgs, reqs)

	if call != 2 {
		t.Fatalf("expected 2 broadcast calls, got %d", call)
	}

	// req 1 should have failed
	res1 := <-chs[1]
	if res1.Error == nil {
		t.Fatal("req 1 should have failed")
	}

	// req 0 and req 2 should have succeeded on retry
	for _, i := range []int{0, 2} {
		res := <-chs[i]
		if res.Error != nil {
			t.Fatalf("req %d: unexpected error: %v", i, res.Error)
		}
		if res.Response != "retry-ok" {
			t.Fatalf("req %d: expected 'retry-ok', got %v", i, res.Response)
		}
	}
}

func TestBroadcastWithRetry_FailWithoutMessageIndex_NoRetry(t *testing.T) {
	callCount := 0
	fn := func(ctx context.Context, msgs []sdk.Msg) (interface{}, error) {
		callCount++
		return nil, errors.New("generic error")
	}
	b := newTestBroadcaster(fn)
	defer b.cancel()

	msgs, reqs, chs := makeReqs(1, 1)
	b.broadcastWithRetry(msgs, reqs)

	if callCount != 1 {
		t.Fatalf("expected 1 call (no retry), got %d", callCount)
	}
	for i, ch := range chs {
		res := <-ch
		if res.Error == nil {
			t.Fatalf("req %d: expected error", i)
		}
		if res.Error.Error() != "generic error" {
			t.Fatalf("req %d: expected 'generic error', got '%v'", i, res.Error)
		}
	}
}

func TestBroadcastWithRetry_AllMessagesBad(t *testing.T) {
	call := 0
	fn := func(ctx context.Context, msgs []sdk.Msg) (interface{}, error) {
		call++
		// Always fail on msg index 0
		return nil, &BroadcastError{Err: fmt.Errorf("bad msg call %d", call), MsgIndex: 0}
	}
	b := newTestBroadcaster(fn)
	defer b.cancel()

	msgs, reqs, chs := makeReqs(1, 1, 1)
	b.broadcastWithRetry(msgs, reqs)

	// Each call removes one request; 3 requests → 3 calls
	if call != 3 {
		t.Fatalf("expected 3 calls, got %d", call)
	}
	for i, ch := range chs {
		res := <-ch
		if res.Error == nil {
			t.Fatalf("req %d: expected error", i)
		}
	}
}

func TestBroadcastWithRetry_MaxRetriesExhausted(t *testing.T) {
	call := 0
	fn := func(ctx context.Context, msgs []sdk.Msg) (interface{}, error) {
		call++
		return nil, &BroadcastError{Err: errors.New("bad"), MsgIndex: 0}
	}
	// 4 requests, maxRetries=2 → can remove at most 3 (initial + 2 retries)
	b := newTestBroadcaster(fn, WithMaxRetries(2))
	defer b.cancel()

	msgs, reqs, chs := makeReqs(1, 1, 1, 1, 1)
	b.broadcastWithRetry(msgs, reqs)

	// Calls: attempt 0 (remove req0), attempt 1 (remove req1), attempt 2 (remove req2),
	// attempt 3 → exceeds maxRetries → exhausted
	if call != 3 {
		t.Fatalf("expected 3 calls, got %d", call)
	}

	// First 3 requests failed with BroadcastError
	for i := 0; i < 3; i++ {
		res := <-chs[i]
		if res.Error == nil {
			t.Fatalf("req %d: expected error", i)
		}
	}
	// Last 2 requests should get "retries exhausted" error
	for i := 3; i < 5; i++ {
		res := <-chs[i]
		if res.Error == nil {
			t.Fatalf("req %d: expected error", i)
		}
		if !strings.Contains(res.Error.Error(), "retries exhausted") {
			t.Fatalf("req %d: expected 'retries exhausted', got '%v'", i, res.Error)
		}
	}
}

func TestBroadcastWithRetry_SingleMessageBatch(t *testing.T) {
	callCount := 0
	fn := func(ctx context.Context, msgs []sdk.Msg) (interface{}, error) {
		callCount++
		return nil, &BroadcastError{Err: errors.New("fail"), MsgIndex: 0}
	}
	b := newTestBroadcaster(fn)
	defer b.cancel()

	msgs, reqs, chs := makeReqs(1)
	b.broadcastWithRetry(msgs, reqs)

	// Only 1 request → remove it → no survivors → no retry
	if callCount != 1 {
		t.Fatalf("expected 1 call, got %d", callCount)
	}
	res := <-chs[0]
	if res.Error == nil {
		t.Fatal("expected error")
	}
}

func TestBroadcastWithRetry_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	call := 0
	fn := func(_ context.Context, msgs []sdk.Msg) (interface{}, error) {
		call++
		if call == 1 {
			// Cancel context after first failure
			cancel()
			return nil, &BroadcastError{Err: errors.New("bad"), MsgIndex: 0}
		}
		return "ok", nil
	}

	cfg := defaultConfig()
	b := &Broadcaster{
		broadcastFunc: fn,
		accountKey:    AccountKey{Address: "test", ChainID: "test", NodeURI: "test"},
		fromAddress:   "test",
		queue:         make(chan TransactionRequest, 100),
		ctx:           ctx,
		cancel:        cancel,
		logger:        slog.Default(),
		config:        cfg,
	}

	msgs, reqs, chs := makeReqs(1, 1, 1)
	b.broadcastWithRetry(msgs, reqs)

	// req 0 gets BroadcastError
	res0 := <-chs[0]
	if res0.Error == nil {
		t.Fatal("req 0: expected error")
	}

	// req 1 and 2 should get context.Canceled
	for _, i := range []int{1, 2} {
		res := <-chs[i]
		if res.Error == nil {
			t.Fatalf("req %d: expected error", i)
		}
		if !errors.Is(res.Error, context.Canceled) {
			t.Fatalf("req %d: expected context.Canceled, got %v", i, res.Error)
		}
	}
}

func TestBroadcastWithRetry_MultiMessageRequest(t *testing.T) {
	call := 0
	fn := func(ctx context.Context, msgs []sdk.Msg) (interface{}, error) {
		call++
		if call == 1 {
			// Fail on msg index 2, which is the 3rd message in a request that has 3 msgs
			return nil, &BroadcastError{Err: errors.New("bad"), MsgIndex: 2}
		}
		return "ok", nil
	}
	b := newTestBroadcaster(fn)
	defer b.cancel()

	// req0 has 3 msgs (indices 0-2), req1 has 1 msg (index 3)
	msgs, reqs, chs := makeReqs(3, 1)
	b.broadcastWithRetry(msgs, reqs)

	if call != 2 {
		t.Fatalf("expected 2 calls, got %d", call)
	}

	// req 0 (the multi-message request) should fail entirely
	res0 := <-chs[0]
	if res0.Error == nil {
		t.Fatal("req 0 should have failed")
	}

	// req 1 should succeed on retry
	res1 := <-chs[1]
	if res1.Error != nil {
		t.Fatalf("req 1: unexpected error: %v", res1.Error)
	}
	if res1.Response != "ok" {
		t.Fatalf("req 1: expected 'ok', got %v", res1.Response)
	}
}

func TestBroadcastError_Interface(t *testing.T) {
	inner := errors.New("inner error")
	be := &BroadcastError{Err: inner, MsgIndex: 3}

	// Satisfies error
	var _ error = be

	// Satisfies MessageIndexError
	var mie MessageIndexError
	if !errors.As(be, &mie) {
		t.Fatal("BroadcastError should satisfy MessageIndexError")
	}
	if mie.MessageIndex() != 3 {
		t.Fatalf("expected MessageIndex 3, got %d", mie.MessageIndex())
	}

	// Unwrap works
	if !errors.Is(be, inner) {
		t.Fatal("Unwrap should return inner error")
	}

	// Error string
	if be.Error() != "message 3: inner error" {
		t.Fatalf("unexpected error string: %s", be.Error())
	}
}

func TestBuildRequestSpans(t *testing.T) {
	_, reqs, _ := makeReqs(2, 1, 3)
	spans := buildRequestSpans(reqs)

	expected := []requestSpan{
		{reqIndex: 0, start: 0, end: 2},
		{reqIndex: 1, start: 2, end: 3},
		{reqIndex: 2, start: 3, end: 6},
	}

	if len(spans) != len(expected) {
		t.Fatalf("expected %d spans, got %d", len(expected), len(spans))
	}
	for i, s := range spans {
		if s != expected[i] {
			t.Fatalf("span %d: expected %+v, got %+v", i, expected[i], s)
		}
	}
}

func TestFindOwningRequest(t *testing.T) {
	spans := []requestSpan{
		{reqIndex: 0, start: 0, end: 2},
		{reqIndex: 1, start: 2, end: 3},
		{reqIndex: 2, start: 3, end: 6},
	}

	tests := []struct {
		msgIdx int
		want   int
	}{
		{0, 0}, {1, 0},   // req 0
		{2, 1},            // req 1
		{3, 2}, {5, 2},   // req 2
		{6, -1}, {-1, -1}, // out of range
	}

	for _, tt := range tests {
		got := findOwningRequest(spans, tt.msgIdx)
		if got != tt.want {
			t.Errorf("findOwningRequest(spans, %d) = %d, want %d", tt.msgIdx, got, tt.want)
		}
	}
}

func TestOptions_Defaults(t *testing.T) {
	cfg := defaultConfig()
	if cfg.maxRetries != 5 {
		t.Fatalf("expected default maxRetries=5, got %d", cfg.maxRetries)
	}
}

func TestOptions_WithMaxRetries(t *testing.T) {
	cfg := defaultConfig()

	WithMaxRetries(10)(&cfg)
	if cfg.maxRetries != 10 {
		t.Fatalf("expected maxRetries=10, got %d", cfg.maxRetries)
	}

	// Negative values ignored
	WithMaxRetries(-1)(&cfg)
	if cfg.maxRetries != 10 {
		t.Fatalf("negative value should be ignored, got %d", cfg.maxRetries)
	}
}

func TestBroadcastWithRetry_MaxRetriesZero(t *testing.T) {
	call := 0
	fn := func(ctx context.Context, msgs []sdk.Msg) (interface{}, error) {
		call++
		return nil, &BroadcastError{Err: errors.New("bad"), MsgIndex: 0}
	}
	b := newTestBroadcaster(fn, WithMaxRetries(0))
	defer b.cancel()

	msgs, reqs, chs := makeReqs(1, 1, 1)
	b.broadcastWithRetry(msgs, reqs)

	// maxRetries=0: attempt 0 removes req0, then attempt >= maxRetries → exhausted
	if call != 1 {
		t.Fatalf("expected 1 call (no retry), got %d", call)
	}

	// req 0 gets the BroadcastError
	res0 := <-chs[0]
	if res0.Error == nil {
		t.Fatal("req 0: expected error")
	}

	// req 1, 2 get "retries exhausted"
	for _, i := range []int{1, 2} {
		res := <-chs[i]
		if res.Error == nil {
			t.Fatalf("req %d: expected error", i)
		}
		if !strings.Contains(res.Error.Error(), "retries exhausted") {
			t.Fatalf("req %d: expected 'retries exhausted', got '%v'", i, res.Error)
		}
	}
}

// Verify no data races with the sync.Mutex usage.
func TestBroadcastWithRetry_ConcurrentSafe(t *testing.T) {
	var mu sync.Mutex
	callCount := 0
	fn := func(ctx context.Context, msgs []sdk.Msg) (interface{}, error) {
		mu.Lock()
		callCount++
		mu.Unlock()
		return "ok", nil
	}
	b := newTestBroadcaster(fn)
	defer b.cancel()

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			msgs, reqs, chs := makeReqs(1)
			b.broadcastWithRetry(msgs, reqs)
			res := <-chs[0]
			if res.Error != nil {
				t.Errorf("unexpected error: %v", res.Error)
			}
		}()
	}
	wg.Wait()
}
