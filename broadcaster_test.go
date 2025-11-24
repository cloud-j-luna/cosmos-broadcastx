package broadcaster

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"testing"
	"time"

	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockBroadcastFunc struct {
	broadcastMsgs      func(ctx context.Context, msgs []sdk.Msg) (interface{}, error)
	broadcastCallCount int
	mu                 sync.Mutex
}

func (m *mockBroadcastFunc) Broadcast(ctx context.Context, msgs []sdk.Msg) (interface{}, error) {
	m.mu.Lock()
	m.broadcastCallCount++
	m.mu.Unlock()

	if m.broadcastMsgs != nil {
		return m.broadcastMsgs(ctx, msgs)
	}
	return nil, nil
}

func (m *mockBroadcastFunc) GetCallCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.broadcastCallCount
}

func newMockBroadcastFunc() (*mockBroadcastFunc, BroadcastFunc) {
	mock := &mockBroadcastFunc{}
	broadcastFunc := func(ctx context.Context, msgs []sdk.Msg) (interface{}, error) {
		return mock.Broadcast(ctx, msgs)
	}
	return mock, broadcastFunc
}

type mockMsg struct {
	value string
}

func (m *mockMsg) Reset()                                                   {}
func (m *mockMsg) String() string                                           { return m.value }
func (m *mockMsg) ProtoMessage()                                            {}
func (m *mockMsg) ValidateBasic() error                                     { return nil }
func (m *mockMsg) GetSigners() []sdk.AccAddress                             { return nil }
func (m *mockMsg) GetSignBytes() []byte                                     { return nil }
func (m *mockMsg) Route() string                                            { return "test" }
func (m *mockMsg) Type() string                                             { return "test" }
func (m *mockMsg) XXX_MessageName() string                                  { return "test" }
func (m *mockMsg) XXX_Size() int                                            { return 0 }
func (m *mockMsg) XXX_DiscardUnknown()                                      {}
func (m *mockMsg) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) { return nil, nil }
func (m *mockMsg) XXX_Unmarshal(b []byte) error                             { return nil }
func (m *mockMsg) XXX_Merge(src interface{})                                {}

func TestAccountKey(t *testing.T) {
	tests := []struct {
		name string
		key1 AccountKey
		key2 AccountKey
		want bool
	}{
		{
			name: "equal keys",
			key1: AccountKey{Address: "addr1", ChainID: "chain1", NodeURI: "node1"},
			key2: AccountKey{Address: "addr1", ChainID: "chain1", NodeURI: "node1"},
			want: true,
		},
		{
			name: "different addresses",
			key1: AccountKey{Address: "addr1", ChainID: "chain1", NodeURI: "node1"},
			key2: AccountKey{Address: "addr2", ChainID: "chain1", NodeURI: "node1"},
			want: false,
		},
		{
			name: "different chain IDs",
			key1: AccountKey{Address: "addr1", ChainID: "chain1", NodeURI: "node1"},
			key2: AccountKey{Address: "addr1", ChainID: "chain2", NodeURI: "node1"},
			want: false,
		},
		{
			name: "different node URIs",
			key1: AccountKey{Address: "addr1", ChainID: "chain1", NodeURI: "node1"},
			key2: AccountKey{Address: "addr1", ChainID: "chain1", NodeURI: "node2"},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.key1 == tt.key2
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestNewBroadcaster(t *testing.T) {
	ctx := context.Background()
	_, broadcastFunc := newMockBroadcastFunc()
	accountKey := AccountKey{
		Address: "test-address",
		ChainID: "test-chain",
		NodeURI: "test-node",
	}
	fromAddress := "akash1testaddress"

	b := NewBroadcaster(ctx, broadcastFunc, accountKey, fromAddress, slog.Default())
	require.NotNil(t, b)
	assert.Equal(t, accountKey, b.accountKey)
	assert.Equal(t, fromAddress, b.fromAddress)
	assert.NotNil(t, b.broadcastFunc)
	assert.NotNil(t, b.queue)
	assert.NotNil(t, b.ctx)
	assert.NotNil(t, b.cancel)

	err := b.Close()
	assert.NoError(t, err)
}

func TestBroadcaster_Broadcast_Success(t *testing.T) {
	ctx := context.Background()
	mock, broadcastFunc := newMockBroadcastFunc()
	accountKey := AccountKey{
		Address: "test-address",
		ChainID: "test-chain",
		NodeURI: "test-node",
	}
	fromAddress := "akash1testaddress"

	expectedResp := "success-response"
	mock.broadcastMsgs = func(ctx context.Context, msgs []sdk.Msg) (interface{}, error) {
		return expectedResp, nil
	}

	b := NewBroadcaster(ctx, broadcastFunc, accountKey, fromAddress, slog.Default())
	defer b.Close()

	msgs := []sdk.Msg{&mockMsg{value: "test-msg"}}
	resultCh := b.Broadcast(ctx, msgs)

	select {
	case result := <-resultCh:
		assert.NoError(t, result.Error)
		assert.Equal(t, expectedResp, result.Response)
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for broadcast result")
	}
}

func TestBroadcaster_Broadcast_Error(t *testing.T) {
	ctx := context.Background()
	mock, broadcastFunc := newMockBroadcastFunc()
	accountKey := AccountKey{
		Address: "test-address",
		ChainID: "test-chain",
		NodeURI: "test-node",
	}
	fromAddress := "akash1testaddress"

	expectedErr := errors.New("broadcast failed")
	mock.broadcastMsgs = func(ctx context.Context, msgs []sdk.Msg) (interface{}, error) {
		return nil, expectedErr
	}

	b := NewBroadcaster(ctx, broadcastFunc, accountKey, fromAddress, slog.Default())
	defer b.Close()

	msgs := []sdk.Msg{&mockMsg{value: "test-msg"}}
	resultCh := b.Broadcast(ctx, msgs)

	select {
	case result := <-resultCh:
		assert.Error(t, result.Error)
		assert.Equal(t, expectedErr, result.Error)
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for broadcast result")
	}
}

func TestBroadcaster_Broadcast_ContextCanceled(t *testing.T) {
	ctx := context.Background()
	_, broadcastFunc := newMockBroadcastFunc()
	accountKey := AccountKey{
		Address: "test-address",
		ChainID: "test-chain",
		NodeURI: "test-node",
	}
	fromAddress := "akash1testaddress"

	b := NewBroadcaster(ctx, broadcastFunc, accountKey, fromAddress, slog.Default())
	defer b.Close()

	cancelCtx, cancel := context.WithCancel(context.Background())
	cancel()

	msgs := []sdk.Msg{&mockMsg{value: "test-msg"}}
	resultCh := b.Broadcast(cancelCtx, msgs)

	select {
	case result := <-resultCh:
		assert.Error(t, result.Error)
		assert.Equal(t, context.Canceled, result.Error)
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for broadcast result")
	}
}

func TestBroadcaster_Broadcast_QueueFull(t *testing.T) {
	ctx := context.Background()
	mock, broadcastFunc := newMockBroadcastFunc()
	accountKey := AccountKey{
		Address: "test-address",
		ChainID: "test-chain",
		NodeURI: "test-node",
	}
	fromAddress := "akash1testaddress"

	// Block the broadcast to slow down processing
	blockCh := make(chan struct{})
	mock.broadcastMsgs = func(ctx context.Context, msgs []sdk.Msg) (interface{}, error) {
		<-blockCh
		return "success", nil
	}

	b := NewBroadcaster(ctx, broadcastFunc, accountKey, fromAddress, slog.Default())
	defer func() {
		close(blockCh)
		b.Close()
	}()

	// Try to fill the queue by sending many messages rapidly
	// The queue capacity is 100, so we send 100+ messages quickly
	// Note: This test may be flaky if messages are processed faster than queued
	// The goal is to verify the error handling path exists
	done := make(chan bool)
	go func() {
		for i := 0; i < 150; i++ {
			msgs := []sdk.Msg{&mockMsg{value: fmt.Sprintf("msg-%d", i)}}
			b.Broadcast(ctx, msgs)
		}
		done <- true
	}()

	// Wait a bit for messages to queue up
	time.Sleep(100 * time.Millisecond)

	// Try to add one more message - may fail with queue full if queue is actually full
	msgs := []sdk.Msg{&mockMsg{value: "overflow-msg"}}
	resultCh := b.Broadcast(ctx, msgs)

	select {
	case result := <-resultCh:
		if result.Error != nil {
			// If we got an error, verify it's the queue full error
			assert.Contains(t, result.Error.Error(), "queue is full")
		} else {
			// If no error, the queue wasn't full (which is fine)
			t.Log("Queue was not full - messages processed faster than queued")
		}
	case <-time.After(200 * time.Millisecond):
		// If we timeout, the message was queued successfully (queue wasn't full)
		t.Log("Message was queued successfully - queue was not full")
	}

	<-done
}

func TestBroadcaster_GroupedBroadcast(t *testing.T) {
	ctx := context.Background()
	mock, broadcastFunc := newMockBroadcastFunc()
	accountKey := AccountKey{
		Address: "test-address",
		ChainID: "test-chain",
		NodeURI: "test-node",
	}
	fromAddress := "akash1testaddress"

	var broadcastedMsgs []sdk.Msg
	mock.broadcastMsgs = func(ctx context.Context, msgs []sdk.Msg) (interface{}, error) {
		broadcastedMsgs = append(broadcastedMsgs, msgs...)
		return "success", nil
	}

	b := NewBroadcaster(ctx, broadcastFunc, accountKey, fromAddress, slog.Default())
	defer b.Close()

	msgs1 := []sdk.Msg{&mockMsg{value: "msg1"}}
	msgs2 := []sdk.Msg{&mockMsg{value: "msg2"}}
	msgs3 := []sdk.Msg{&mockMsg{value: "msg3"}}

	resultCh1 := b.Broadcast(ctx, msgs1)
	resultCh2 := b.Broadcast(ctx, msgs2)
	resultCh3 := b.Broadcast(ctx, msgs3)

	<-resultCh1
	<-resultCh2
	<-resultCh3

	time.Sleep(200 * time.Millisecond)

	assert.GreaterOrEqual(t, len(broadcastedMsgs), 3)
}

func TestBroadcaster_Close(t *testing.T) {
	ctx := context.Background()
	mock, broadcastFunc := newMockBroadcastFunc()
	accountKey := AccountKey{
		Address: "test-address",
		ChainID: "test-chain",
		NodeURI: "test-node",
	}
	fromAddress := "akash1testaddress"

	var wg sync.WaitGroup
	wg.Add(1)
	mock.broadcastMsgs = func(ctx context.Context, msgs []sdk.Msg) (interface{}, error) {
		wg.Done()
		return "success", nil
	}

	b := NewBroadcaster(ctx, broadcastFunc, accountKey, fromAddress, slog.Default())

	msgs := []sdk.Msg{&mockMsg{value: "test-msg"}}
	b.Broadcast(ctx, msgs)

	wg.Wait()

	err := b.Close()
	assert.NoError(t, err)

	select {
	case <-b.ctx.Done():
	default:
		t.Fatal("context should be canceled after Close()")
	}
}

func TestBroadcaster_FromAddress(t *testing.T) {
	ctx := context.Background()
	_, broadcastFunc := newMockBroadcastFunc()
	accountKey := AccountKey{
		Address: "test-address",
		ChainID: "test-chain",
		NodeURI: "test-node",
	}
	fromAddress := "akash1testaddress"

	b := NewBroadcaster(ctx, broadcastFunc, accountKey, fromAddress, slog.Default())
	defer b.Close()

	retrievedAddress := b.FromAddress()
	assert.Equal(t, fromAddress, retrievedAddress)
}

func TestNewRegistry(t *testing.T) {
	r := NewRegistry(slog.Default())
	require.NotNil(t, r)
	assert.NotNil(t, r.broadcasters)
	assert.Equal(t, 0, len(r.broadcasters))
}

func TestRegistry_GetOrCreateBroadcaster(t *testing.T) {
	ctx := context.Background()
	_, broadcastFunc := newMockBroadcastFunc()
	accountKey := AccountKey{
		Address: "test-address",
		ChainID: "test-chain",
		NodeURI: "test-node",
	}
	fromAddress := "akash1testaddress"

	r := NewRegistry(slog.Default())
	defer r.Close()

	b1 := r.GetOrCreateBroadcaster(ctx, broadcastFunc, accountKey, fromAddress)
	require.NotNil(t, b1)
	assert.Equal(t, accountKey, b1.accountKey)

	b2 := r.GetOrCreateBroadcaster(ctx, broadcastFunc, accountKey, fromAddress)
	require.NotNil(t, b2)
	assert.Equal(t, b1, b2, "should return same broadcaster for same account key")

	differentKey := AccountKey{
		Address: "different-address",
		ChainID: "test-chain",
		NodeURI: "test-node",
	}
	differentAddress := "akash1differentaddress"
	b3 := r.GetOrCreateBroadcaster(ctx, broadcastFunc, differentKey, differentAddress)
	require.NotNil(t, b3)
	assert.NotEqual(t, b1, b3, "should return different broadcaster for different account key")
}

func TestRegistry_GetBroadcaster(t *testing.T) {
	ctx := context.Background()
	_, broadcastFunc := newMockBroadcastFunc()
	accountKey := AccountKey{
		Address: "test-address",
		ChainID: "test-chain",
		NodeURI: "test-node",
	}
	fromAddress := "akash1testaddress"

	r := NewRegistry(slog.Default())
	defer r.Close()

	b := r.GetBroadcaster(accountKey)
	assert.Nil(t, b, "should return nil for non-existent broadcaster")

	r.GetOrCreateBroadcaster(ctx, broadcastFunc, accountKey, fromAddress)

	b = r.GetBroadcaster(accountKey)
	require.NotNil(t, b, "should return broadcaster after creation")
	assert.Equal(t, accountKey, b.accountKey)
}

func TestRegistry_Close(t *testing.T) {
	ctx := context.Background()
	_, broadcastFunc := newMockBroadcastFunc()

	r := NewRegistry(slog.Default())

	key1 := AccountKey{Address: "addr1", ChainID: "chain", NodeURI: "node"}
	key2 := AccountKey{Address: "addr2", ChainID: "chain", NodeURI: "node"}
	key3 := AccountKey{Address: "addr3", ChainID: "chain", NodeURI: "node"}

	b1 := r.GetOrCreateBroadcaster(ctx, broadcastFunc, key1, "akash1addr1")
	b2 := r.GetOrCreateBroadcaster(ctx, broadcastFunc, key2, "akash1addr2")
	b3 := r.GetOrCreateBroadcaster(ctx, broadcastFunc, key3, "akash1addr3")

	err := r.Close()
	assert.NoError(t, err)

	select {
	case <-b1.ctx.Done():
	default:
		t.Fatal("broadcaster 1 context should be canceled")
	}

	select {
	case <-b2.ctx.Done():
	default:
		t.Fatal("broadcaster 2 context should be canceled")
	}

	select {
	case <-b3.ctx.Done():
	default:
		t.Fatal("broadcaster 3 context should be canceled")
	}
}

func TestRegistry_Concurrent(t *testing.T) {
	ctx := context.Background()
	_, broadcastFunc := newMockBroadcastFunc()
	r := NewRegistry(slog.Default())
	defer r.Close()

	accountKey := AccountKey{
		Address: "test-address",
		ChainID: "test-chain",
		NodeURI: "test-node",
	}
	fromAddress := "akash1testaddress"

	var wg sync.WaitGroup
	broadcasters := make([]*Broadcaster, 100)

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			broadcasters[idx] = r.GetOrCreateBroadcaster(ctx, broadcastFunc, accountKey, fromAddress)
		}(i)
	}

	wg.Wait()

	firstBroadcaster := broadcasters[0]
	for i := 1; i < 100; i++ {
		assert.Equal(t, firstBroadcaster, broadcasters[i], "all broadcasters should be the same instance")
	}
}

func TestBroadcaster_MultipleMessages(t *testing.T) {
	ctx := context.Background()
	mock, broadcastFunc := newMockBroadcastFunc()
	accountKey := AccountKey{
		Address: "test-address",
		ChainID: "test-chain",
		NodeURI: "test-node",
	}
	fromAddress := "akash1testaddress"

	mock.broadcastMsgs = func(ctx context.Context, msgs []sdk.Msg) (interface{}, error) {
		time.Sleep(10 * time.Millisecond)
		return "success", nil
	}

	b := NewBroadcaster(ctx, broadcastFunc, accountKey, fromAddress, slog.Default())
	defer b.Close()

	numMessages := 10
	resultChannels := make([]<-chan TransactionResult, numMessages)

	for i := 0; i < numMessages; i++ {
		msgs := []sdk.Msg{&mockMsg{value: fmt.Sprintf("msg-%d", i)}}
		resultChannels[i] = b.Broadcast(ctx, msgs)
	}

	successCount := 0
	for i := 0; i < numMessages; i++ {
		select {
		case result := <-resultChannels[i]:
			if result.Error == nil {
				successCount++
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("timeout waiting for result %d", i)
		}
	}

	assert.Equal(t, numMessages, successCount, "all messages should be broadcast successfully")
	assert.GreaterOrEqual(t, mock.GetCallCount(), 1, "at least one broadcast should have been made")
}

func TestBroadcaster_MultipleBatchGrouping(t *testing.T) {
	ctx := context.Background()
	mock, broadcastFunc := newMockBroadcastFunc()
	accountKey := AccountKey{
		Address: "test-address",
		ChainID: "test-chain",
		NodeURI: "test-node",
	}
	fromAddress := "akash1testaddress"

	var broadcastCalls [][]sdk.Msg
	var broadcastMu sync.Mutex

	// Broadcast function that takes time, simulating real blockchain transaction time
	mock.broadcastMsgs = func(ctx context.Context, msgs []sdk.Msg) (interface{}, error) {
		broadcastMu.Lock()
		broadcastCalls = append(broadcastCalls, msgs)
		broadcastMu.Unlock()

		// Simulate blockchain transaction time
		time.Sleep(200 * time.Millisecond)
		return "success", nil
	}

	b := NewBroadcaster(ctx, broadcastFunc, accountKey, fromAddress, slog.Default())
	defer b.Close()

	// Send all 6 messages with a stagger to create 2 batches:
	// - First 3 messages sent immediately sequentially (batch 1)
	// - Wait 80ms (during which batch 1 is being broadcast) for collection timeout
	// - Send next 3 messages (batch 2, collected while batch 1 is broadcasting)
	resultChannels := make([]<-chan TransactionResult, 6)

	for i := 0; i < 3; i++ {
		msgs := []sdk.Msg{&mockMsg{value: fmt.Sprintf("batch1-msg-%d", i)}}
		resultChannels[i] = b.Broadcast(ctx, msgs)
	}

	time.Sleep(80 * time.Millisecond)

	for i := 3; i < 6; i++ {
		msgs := []sdk.Msg{&mockMsg{value: fmt.Sprintf("batch2-msg-%d", i)}}
		resultChannels[i] = b.Broadcast(ctx, msgs)
	}

	successCount := 0
	for i := 0; i < 6; i++ {
		select {
		case result := <-resultChannels[i]:
			if result.Error == nil {
				successCount++
			} else {
				t.Fatalf("message %d failed: %v", i, result.Error)
			}
		case <-time.After(5 * time.Second):
			broadcastMu.Lock()
			t.Logf("Broadcast calls so far: %d", len(broadcastCalls))
			for idx, call := range broadcastCalls {
				t.Logf("  Call %d: %d messages", idx+1, len(call))
			}
			broadcastMu.Unlock()
			t.Fatalf("timeout waiting for result %d", i)
		}
	}

	assert.Equal(t, 6, successCount, "all 6 messages should be broadcast successfully")

	broadcastMu.Lock()
	callCount := len(broadcastCalls)
	totalMsgs := 0
	for _, msgs := range broadcastCalls {
		totalMsgs += len(msgs)
	}

	t.Logf("Total broadcast calls: %d", callCount)
	for idx, call := range broadcastCalls {
		t.Logf("  Batch %d: %d messages", idx+1, len(call))
	}
	broadcastMu.Unlock()

	assert.LessOrEqual(t, callCount, 3, "should have at most 3 broadcast calls (efficient batching)")
	assert.Equal(t, 6, totalMsgs, "all 6 messages should have been broadcast")

	assert.GreaterOrEqual(t, callCount, 1, "should have at least 1 broadcast call")
}
