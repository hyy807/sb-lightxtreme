package urltest

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"net/url"
	"reflect"
	"sync"
	"time"

	"github.com/sagernet/sing-anytls"
	"github.com/sagernet/sing-box/adapter"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-mux"
	"github.com/sagernet/sing-snell"
	"github.com/sagernet/sing/common"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/common/ntp"
	"github.com/sagernet/sing/common/observable"
)

type HistoryStorage struct {
	access       sync.RWMutex
	delayHistory map[string]*adapter.URLTestHistory
	updateHooks  []*observable.Subscriber[struct{}]
}

func NewHistoryStorage() *HistoryStorage {
	return &HistoryStorage{
		delayHistory: make(map[string]*adapter.URLTestHistory),
	}
}

func (s *HistoryStorage) AddUpdateHook(hook *observable.Subscriber[struct{}]) {
	s.access.Lock()
	defer s.access.Unlock()
	s.updateHooks = append(s.updateHooks, hook)
}

func (s *HistoryStorage) NotifyUpdated() {
	s.access.RLock()
	defer s.access.RUnlock()
	s.notifyUpdated()
}

func (s *HistoryStorage) LoadURLTestHistory(tag string) *adapter.URLTestHistory {
	if s == nil {
		return nil
	}
	s.access.RLock()
	defer s.access.RUnlock()
	return s.delayHistory[tag]
}

func (s *HistoryStorage) DeleteURLTestHistory(tag string) {
	s.access.Lock()
	if _, exists := s.delayHistory[tag]; !exists {
		s.access.Unlock()
		return
	}
	delete(s.delayHistory, tag)
	s.notifyUpdated()
	s.access.Unlock()
}

func (s *HistoryStorage) StoreURLTestHistory(tag string, history *adapter.URLTestHistory) {
	s.access.Lock()
	if previous := s.delayHistory[tag]; previous != nil && previous.Time.Equal(history.Time) && previous.Delay == history.Delay {
		s.access.Unlock()
		return
	}
	s.delayHistory[tag] = history
	s.notifyUpdated()
	s.access.Unlock()
}

func (s *HistoryStorage) notifyUpdated() {
	for _, updateHook := range s.updateHooks {
		updateHook.Emit(struct{}{})
	}
}

func (s *HistoryStorage) Close() error {
	s.access.Lock()
	defer s.access.Unlock()
	s.updateHooks = nil
	return nil
}

const DefaultURL = "http://www.gstatic.com/generate_204"

type testKey struct {
	dialer N.Dialer
	link   string
}
type testResult struct {
	history adapter.URLTestHistory
	err     error
	done    chan struct{}
	waiters int
}

var inFlight = struct {
	sync.Mutex
	calls map[testKey]*testResult
}{calls: make(map[testKey]*testResult)}

// runURLTest shares only overlapping measurements, never completed results.
// The first caller owns the context; waiters receive its final measurement/error.
func runURLTest(ctx context.Context, link string, detour N.Dialer) *testResult {
	if link == "" {
		link = DefaultURL
	}
	// Custom non-comparable dialers cannot be identity keys.
	if !reflect.TypeOf(detour).Comparable() {
		delay, err := measureURLTest(ctx, link, detour)
		return &testResult{history: adapter.URLTestHistory{Time: time.Now(), Delay: delay}, err: err}
	}
	key := testKey{detour, link}
	inFlight.Lock()
	if call := inFlight.calls[key]; call != nil {
		call.waiters++
		inFlight.Unlock()
		<-call.done
		return call
	}
	call := &testResult{done: make(chan struct{})}
	inFlight.calls[key] = call
	inFlight.Unlock()
	call.history.Delay, call.err = measureURLTest(ctx, link, detour)
	call.history.Time = time.Now()
	inFlight.Lock()
	delete(inFlight.calls, key)
	close(call.done)
	inFlight.Unlock()
	return call
}

func URLTest(ctx context.Context, link string, detour N.Dialer) (uint16, error) {
	result := runURLTest(ctx, link, detour)
	return result.history.Delay, result.err
}

// URLTestWithHistory publishes a shared measurement with its shared timestamp.
func URLTestWithHistory(ctx context.Context, link string, detour N.Dialer, history *HistoryStorage, tag string) (uint16, error) {
	result := runURLTest(ctx, link, detour)
	if result.err != nil {
		history.DeleteURLTestHistory(tag)
	} else {
		history.StoreURLTestHistory(tag, &result.history)
	}
	return result.history.Delay, result.err
}

func measureURLTest(ctx context.Context, link string, detour N.Dialer) (uint16, error) {
	multiplexOutbound, isMultiplexOutbound := common.Cast[adapter.OutboundWithMultiplex](detour)
	if isMultiplexOutbound && multiplexOutbound.MultiplexEnabled() {
		warmContext := adapter.ContextWithKeepSession(ctx)
		warmContext = mux.ContextWithKeepSession(warmContext)
		warmContext = anytls.ContextWithKeepSession(warmContext)
		warmContext = contextWithQUICKeepSession(warmContext)
		warmContext = snell.ContextWithKeepSession(warmContext)
		_, err := urlTest(warmContext, link, detour)
		if err != nil {
			return 0, err
		}
	}
	return urlTest(ctx, link, detour)
}

func urlTest(ctx context.Context, link string, detour N.Dialer) (t uint16, err error) {
	if link == "" {
		link = DefaultURL
	}
	linkURL, err := url.Parse(link)
	if err != nil {
		return
	}
	hostname := linkURL.Hostname()
	port := linkURL.Port()
	if port == "" {
		switch linkURL.Scheme {
		case "http":
			port = "80"
		case "https":
			port = "443"
		}
	}

	start := time.Now()
	instance, err := detour.DialContext(ctx, "tcp", M.ParseSocksaddrHostPortStr(hostname, port))
	if err != nil {
		return
	}
	defer instance.Close()
	if N.NeedHandshakeForWrite(instance) {
		start = time.Now()
	}
	req, err := http.NewRequest(http.MethodHead, link, nil)
	if err != nil {
		return
	}
	client := http.Client{
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
				return instance, nil
			},
			TLSClientConfig: &tls.Config{
				Time:    ntp.TimeFuncFromContext(ctx),
				RootCAs: adapter.RootPoolFromContext(ctx),
			},
		},
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
		Timeout: C.TCPTimeout,
	}
	defer client.CloseIdleConnections()
	resp, err := client.Do(req.WithContext(ctx))
	if err != nil {
		return
	}
	resp.Body.Close()
	t = uint16(time.Since(start) / time.Millisecond)
	return
}
