package kamacache

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/youngyangyang04/KamaCache-Go/consistenthash"
	"github.com/youngyangyang04/KamaCache-Go/registry"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	defaultSvcName          = "kama-cache"
	oldRingSnapshotInterval = time.Minute
)

// PeerPicker defines peer selection behavior.
type PeerPicker interface {
	PickPeer(key string) (peer Peer, ok bool, self bool)
	Close() error
}

// Peer defines remote node operations.
type Peer interface {
	Get(group string, key string) ([]byte, error)
	GetLocal(group string, key string) ([]byte, error)
	Set(ctx context.Context, group string, key string, value []byte) error
	SetLocal(ctx context.Context, group string, key string, value []byte) error
	Delete(group string, key string) (bool, error)
	Close() error
}

type ClientPicker struct {
	selfAddr string
	svcName  string

	mu             sync.RWMutex
	consHash       *consistenthash.Map
	clients        map[string]*Client
	oldHash        *consistenthash.Map
	oldHashSavedAt time.Time

	etcdCli *clientv3.Client
	ctx     context.Context
	cancel  context.CancelFunc
}

type PeerNodeSnapshot struct {
	Addr   string `json:"addr"`
	Status string `json:"status"`
	Self   bool   `json:"self"`
}

type ClientPickerSnapshot struct {
	SelfAddr       string                   `json:"self_addr"`
	SvcName        string                   `json:"svc_name"`
	Peers          []PeerNodeSnapshot       `json:"peers"`
	Ring           consistenthash.Snapshot  `json:"ring"`
	OldRing        *consistenthash.Snapshot `json:"old_ring,omitempty"`
	OldRingSavedAt string                   `json:"old_ring_saved_at,omitempty"`
}

type PickerOption func(*ClientPicker)

func WithServiceName(name string) PickerOption {
	return func(p *ClientPicker) {
		p.svcName = name
	}
}

func (p *ClientPicker) PrintPeers() {
	p.mu.RLock()
	defer p.mu.RUnlock()

	log.Printf("current discovered peers:")
	for addr := range p.clients {
		log.Printf("- %s", addr)
	}
}

func NewClientPicker(addr string, opts ...PickerOption) (*ClientPicker, error) {
	ctx, cancel := context.WithCancel(context.Background())
	picker := &ClientPicker{
		selfAddr: addr,
		svcName:  defaultSvcName,
		clients:  make(map[string]*Client),
		consHash: consistenthash.New(),
		ctx:      ctx,
		cancel:   cancel,
	}

	for _, opt := range opts {
		opt(picker)
	}

	if err := picker.consHash.Add(addr); err != nil {
		cancel()
		return nil, err
	}
	picker.saveOldHashSnapshotLocked()

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   registry.DefaultConfig.Endpoints,
		DialTimeout: registry.DefaultConfig.DialTimeout,
	})
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create etcd client: %v", err)
	}
	picker.etcdCli = cli

	if err := picker.startServiceDiscovery(); err != nil {
		cancel()
		cli.Close()
		return nil, err
	}

	go picker.startOldHashSnapshotLoop()
	return picker, nil
}

func (p *ClientPicker) PickOldPeer(key string) (Peer, bool, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.oldHash == nil {
		return nil, false, false
	}

	addr := p.oldHash.Get(key)
	if addr == p.selfAddr {
		return nil, true, true
	}
	if client, ok := p.clients[addr]; ok {
		return client, true, false
	}

	return nil, false, false
}

func (p *ClientPicker) UpdateHash(newHash *consistenthash.Map) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.consHash = newHash
}

func (p *ClientPicker) Snapshot() ClientPickerSnapshot {
	p.mu.RLock()
	defer p.mu.RUnlock()

	peers := make([]PeerNodeSnapshot, 0, len(p.clients)+1)
	peers = append(peers, PeerNodeSnapshot{
		Addr:   p.selfAddr,
		Status: Active.String(),
		Self:   true,
	})
	for addr, client := range p.clients {
		peers = append(peers, PeerNodeSnapshot{
			Addr:   addr,
			Status: client.GetStatus().String(),
			Self:   false,
		})
	}

	var oldRing *consistenthash.Snapshot
	if p.oldHash != nil {
		snapshot := p.oldHash.Snapshot()
		oldRing = &snapshot
	}

	var oldRingSavedAt string
	if !p.oldHashSavedAt.IsZero() {
		oldRingSavedAt = p.oldHashSavedAt.Format(time.RFC3339)
	}

	return ClientPickerSnapshot{
		SelfAddr:       p.selfAddr,
		SvcName:        p.svcName,
		Peers:          peers,
		Ring:           p.consHash.Snapshot(),
		OldRing:        oldRing,
		OldRingSavedAt: oldRingSavedAt,
	}
}

func (p *ClientPicker) AddPeer(addr string) error {
	if addr == "" {
		return fmt.Errorf("addr is required")
	}
	if addr == p.selfAddr {
		return nil
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if _, exists := p.clients[addr]; exists {
		return nil
	}
	p.set(addr)
	return nil
}

func (p *ClientPicker) RemovePeer(addr string) error {
	if addr == "" {
		return fmt.Errorf("addr is required")
	}
	if addr == p.selfAddr {
		return fmt.Errorf("cannot remove self peer from picker")
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if _, exists := p.clients[addr]; !exists {
		return fmt.Errorf("peer %s not found", addr)
	}
	p.remove(addr)
	return nil
}

func (p *ClientPicker) startServiceDiscovery() error {
	if err := p.fetchAllServices(); err != nil {
		return err
	}

	go p.watchServiceChanges()
	return nil
}

func (p *ClientPicker) watchServiceChanges() {
	watcher := clientv3.NewWatcher(p.etcdCli)
	watchChan := watcher.Watch(p.ctx, "/services/"+p.svcName, clientv3.WithPrefix())

	for {
		select {
		case <-p.ctx.Done():
			watcher.Close()
			return
		case resp := <-watchChan:
			p.handleWatchEvents(resp.Events)
		}
	}
}

func (p *ClientPicker) handleWatchEvents(events []*clientv3.Event) {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, event := range events {
		addr := string(event.Kv.Value)
		if addr == p.selfAddr {
			continue
		}

		switch event.Type {
		case clientv3.EventTypePut:
			if _, exists := p.clients[addr]; !exists {
				p.set(addr)
				logrus.Infof("New service discovered at %s", addr)
			}
		case clientv3.EventTypeDelete:
			if client, exists := p.clients[addr]; exists {
				client.Close()
				p.remove(addr)
				logrus.Infof("Service removed at %s", addr)
			}
		}
	}
}

func (p *ClientPicker) fetchAllServices() error {
	ctx, cancel := context.WithTimeout(p.ctx, 3*time.Second)
	defer cancel()

	resp, err := p.etcdCli.Get(ctx, "/services/"+p.svcName, clientv3.WithPrefix())
	if err != nil {
		return fmt.Errorf("failed to get all services: %v", err)
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	for _, kv := range resp.Kvs {
		addr := string(kv.Value)
		if addr != "" && addr != p.selfAddr {
			p.set(addr)
			logrus.Infof("Discovered service at %s", addr)
		}
	}
	return nil
}

func (p *ClientPicker) set(addr string) {
	client, err := NewClient(addr, p.svcName, p.etcdCli)
	if err != nil {
		logrus.Errorf("Failed to create client for %s: %v", addr, err)
		return
	}

	if err := p.consHash.Add(addr); err != nil {
		client.Close()
		logrus.Errorf("Failed to add %s to hash ring: %v", addr, err)
		return
	}

	p.clients[addr] = client
	logrus.Infof("Successfully created client for %s", addr)
}

func (p *ClientPicker) remove(addr string) {
	if err := p.consHash.Remove(addr); err != nil {
		logrus.Warnf("Failed to remove %s from hash ring: %v", addr, err)
	}

	client := p.clients[addr]
	client.GracefulStop()
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-p.ctx.Done():
				return
			case <-ticker.C:
				if client.GetStatus() != Offline {
					continue
				}

				p.mu.Lock()
				delete(p.clients, addr)
				p.mu.Unlock()
				return
			}
		}
	}()
}

func (p *ClientPicker) PickPeer(key string) (Peer, bool, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	addr := p.consHash.Get(key)
	if addr == "" {
		return nil, false, false
	}
	if addr == p.selfAddr {
		return nil, true, true
	}

	client, ok := p.clients[addr]
	if !ok {
		return nil, false, false
	}
	return client, true, false
}

func (p *ClientPicker) Close() error {
	p.cancel()

	p.mu.Lock()
	defer p.mu.Unlock()

	var errs []error
	for addr, client := range p.clients {
		if err := client.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close client %s: %v", addr, err))
		}
	}

	if err := p.etcdCli.Close(); err != nil {
		errs = append(errs, fmt.Errorf("failed to close etcd client: %v", err))
	}

	if len(errs) > 0 {
		return fmt.Errorf("errors while closing: %v", errs)
	}
	return nil
}

func (p *ClientPicker) startOldHashSnapshotLoop() {
	ticker := time.NewTicker(oldRingSnapshotInterval)
	defer ticker.Stop()

	for {
		select {
		case <-p.ctx.Done():
			return
		case <-ticker.C:
			p.mu.Lock()
			p.saveOldHashSnapshotLocked()
			p.mu.Unlock()
		}
	}
}

func (p *ClientPicker) saveOldHashSnapshotLocked() {
	p.oldHash = p.consHash.Clone()
	p.oldHashSavedAt = time.Now()
}

func parseAddrFromKey(key, svcName string) string {
	prefix := fmt.Sprintf("/services/%s/", svcName)
	if strings.HasPrefix(key, prefix) {
		return strings.TrimPrefix(key, prefix)
	}
	return ""
}
