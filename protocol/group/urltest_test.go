package group

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/outbound"
	"github.com/sagernet/sing-box/common/urltest"
	"github.com/sagernet/sing-box/log"
	M "github.com/sagernet/sing/common/metadata"
)

type batchTestOutbound struct {
	adapter.Outbound
	tag   string
	calls atomic.Int32
}

func (o *batchTestOutbound) Network() []string { return []string{"tcp"} }
func (o *batchTestOutbound) Tag() string       { return o.tag }
func (o *batchTestOutbound) DialContext(context.Context, string, M.Socksaddr) (net.Conn, error) {
	o.calls.Add(1)
	a, b := net.Pipe()
	go func() {
		defer b.Close()
		buf := make([]byte, 4096)
		b.Read(buf)
		b.Write([]byte("HTTP/1.1 204 No Content\r\nConnection: close\r\n\r\n"))
	}()
	return a, nil
}

type batchTestGroup struct {
	adapter.OutboundGroup
	tag      string
	members  []string
	selected adapter.Outbound
}

func (g *batchTestGroup) Tag() string                      { return g.tag }
func (g *batchTestGroup) All() []string                    { return g.members }
func (g *batchTestGroup) Selected(string) adapter.Outbound { return g.selected }

type batchTestManager struct {
	adapter.OutboundManager
	members map[string]adapter.Outbound
}

func (m *batchTestManager) Outbound(tag string) (adapter.Outbound, bool) {
	o, ok := m.members[tag]
	return o, ok
}
func TestNestedGroupsMeasureRealNodeOnce(t *testing.T) {
	node := &batchTestOutbound{tag: "node"}
	manager := &batchTestManager{members: map[string]adapter.Outbound{"node": node}}
	selector := &batchTestGroup{tag: "selector", members: []string{"node"}, selected: node}
	manager.members["selector"] = selector
	auto := &URLTest{group: &URLTestGroup{outbounds: []adapter.Outbound{node}, link: urltest.DefaultURL}}
	// URLTest embeds Adapter; use the standard adapter constructor through its field.
	auto.Adapter = outbound.NewAdapter("urltest", "auto", []string{"tcp"}, nil)
	history := urltest.NewHistoryStorage()
	auto.group.history = history
	logger := log.NewNOPFactory().Logger()
	for i := 0; i < 2; i++ {
		result := URLTestOutbounds(context.Background(), manager, history, logger, []adapter.Outbound{selector, auto, node}, "", time.Minute, true)
		if node.calls.Load() != int32(i+1) {
			t.Fatalf("duplicate node tests: %d", node.calls.Load())
		}
		if _, ok := result["node"]; !ok {
			t.Fatal("missing real node result")
		}
	}
}
