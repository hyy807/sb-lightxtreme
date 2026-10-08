package urltest

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	M "github.com/sagernet/sing/common/metadata"
)

type testDialer struct {
	calls   atomic.Int32
	entered chan struct{}
	release chan struct{}
	fail    error
}

func (*testDialer) Type() string           { return "test" }
func (*testDialer) Tag() string            { return "test" }
func (*testDialer) Network() []string      { return []string{"tcp"} }
func (*testDialer) Dependencies() []string { return nil }
func (*testDialer) MultiplexEnabled() bool { return true }
func (*testDialer) ListenPacket(context.Context, M.Socksaddr) (net.PacketConn, error) {
	panic("unused")
}
func (d *testDialer) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	n := d.calls.Add(1)
	if n == 1 {
		close(d.entered)
		<-d.release
	}
	if d.fail != nil {
		return nil, d.fail
	}
	a, b := net.Pipe()
	go func() {
		defer b.Close()
		buf := make([]byte, 4096)
		_, _ = b.Read(buf)
		_, _ = b.Write([]byte("HTTP/1.1 204 No Content\r\nConnection: close\r\n\r\n"))
	}()
	return a, nil
}
func newTestDialer() *testDialer {
	return &testDialer{entered: make(chan struct{}), release: make(chan struct{})}
}
func waitForWaiters(t *testing.T, d *testDialer, link string, count int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		inFlight.Lock()
		call := inFlight.calls[testKey{d, link}]
		n := 0
		if call != nil {
			n = call.waiters
		}
		inFlight.Unlock()
		if n == count {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("callers did not overlap")
}
func TestOverlappingURLTestSharesWarmupMeasurementAndHistory(t *testing.T) {
	d := newTestDialer()
	h := NewHistoryStorage()
	results := make(chan *testResult, 8)
	go func() { results <- runURLTest(context.Background(), "", d) }()
	<-d.entered
	for i := 0; i < 7; i++ {
		go func() { results <- runURLTest(context.Background(), DefaultURL, d) }()
	}
	waitForWaiters(t, d, DefaultURL, 7)
	close(d.release)
	first := <-results
	for i := 0; i < 7; i++ {
		if r := <-results; r != first {
			t.Fatal("different final measurements")
		}
	}
	if first.err != nil || d.calls.Load() != 2 {
		t.Fatalf("calls=%d err=%v", d.calls.Load(), first.err)
	}
	h.StoreURLTestHistory("node", &first.history)
	copyHistory := first.history
	h.StoreURLTestHistory("node", &copyHistory)
	if h.LoadURLTestHistory("node") != &first.history {
		t.Fatal("duplicate history was republished")
	}
	if _, err := URLTestWithHistory(context.Background(), "", d, h, "node"); err != nil {
		t.Fatal(err)
	}
	if d.calls.Load() != 4 {
		t.Fatalf("sequential retest cached: %d", d.calls.Load())
	}
	if h.LoadURLTestHistory("node").Time.Equal(first.history.Time) {
		t.Fatal("retest timestamp not updated")
	}
}
func TestOverlappingURLTestSharesError(t *testing.T) {
	d := newTestDialer()
	d.fail = errors.New("dial failed")
	results := make(chan *testResult, 2)
	go func() { results <- runURLTest(context.Background(), "", d) }()
	<-d.entered
	go func() { results <- runURLTest(context.Background(), DefaultURL, d) }()
	waitForWaiters(t, d, DefaultURL, 1)
	close(d.release)
	a, b := <-results, <-results
	if a != b || a.err != d.fail || d.calls.Load() != 1 {
		t.Fatal("error not shared")
	}
}
func TestURLTestDoesNotSerializeNodesOrURLs(t *testing.T) {
	a, b := newTestDialer(), newTestDialer()
	done := make(chan struct{}, 3)
	go func() { URLTest(context.Background(), DefaultURL, a); done <- struct{}{} }()
	<-a.entered
	go func() { URLTest(context.Background(), DefaultURL, b); done <- struct{}{} }()
	select {
	case <-b.entered:
	case <-time.After(time.Second):
		t.Fatal("nodes serialized")
	}
	go func() { URLTest(context.Background(), "http://other.test/", a); done <- struct{}{} }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("URLs serialized")
	}
	close(a.release)
	close(b.release)
	<-done
	<-done
}
func TestDefaultURLIsHTTP(t *testing.T) {
	if DefaultURL != "http://www.gstatic.com/generate_204" {
		t.Fatal(DefaultURL)
	}
	var _ adapter.OutboundWithMultiplex = (*testDialer)(nil)
}
