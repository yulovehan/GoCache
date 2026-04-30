package kamacache

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/youngyangyang04/KamaCache-Go/singleflight"
)

var (
	groupsMu sync.RWMutex
	groups   = make(map[string]*Group)
)

var ErrKeyRequired = errors.New("key is required")
var ErrValueRequired = errors.New("value is required")
var ErrGroupClosed = errors.New("cache group is closed")
var ErrCacheMiss = errors.New("cache miss")

const (
	peerSetTimeout = 800 * time.Millisecond
	peerReplicaTTL = 5 * time.Second
)

type Getter interface {
	Get(ctx context.Context, key string) ([]byte, error)
}

type GetterFunc func(ctx context.Context, key string) ([]byte, error)

func (f GetterFunc) Get(ctx context.Context, key string) ([]byte, error) {
	return f(ctx, key)
}

type Group struct {
	name       string
	getter     Getter
	mainCache  *Cache
	peers      PeerPicker
	loader     *singleflight.Group
	expiration time.Duration
	closed     int32
	stats      groupStats
}

type groupStats struct {
	loads        atomic.Int64
	localHits    atomic.Int64
	localMisses  atomic.Int64
	peerHits     atomic.Int64
	peerMisses   atomic.Int64
	loaderHits   atomic.Int64
	loaderErrors atomic.Int64
	loadDuration atomic.Int64
}

type loadResult struct {
	value          ByteView
	shouldSetOwner bool
}

type oldPeerPicker interface {
	PickOldPeer(string) (Peer, bool, bool)
}

type GroupOption func(*Group)

func WithExpiration(d time.Duration) GroupOption {
	return func(g *Group) {
		g.expiration = d
	}
}

func WithPeers(peers PeerPicker) GroupOption {
	return func(g *Group) {
		g.peers = peers
	}
}

func WithCacheOptions(opts CacheOptions) GroupOption {
	return func(g *Group) {
		g.mainCache = NewCache(opts)
	}
}

func NewGroup(name string, cacheBytes int64, getter Getter, opts ...GroupOption) *Group {
	if getter == nil {
		panic("nil Getter")
	}

	cacheOpts := DefaultCacheOptions()
	cacheOpts.MaxBytes = cacheBytes

	g := &Group{
		name:      name,
		getter:    getter,
		mainCache: NewCache(cacheOpts),
		loader:    &singleflight.Group{},
	}

	for _, opt := range opts {
		opt(g)
	}

	groupsMu.Lock()
	defer groupsMu.Unlock()

	if _, exists := groups[name]; exists {
		logrus.Warnf("Group with name %s already exists, will be replaced", name)
	}

	groups[name] = g
	logrus.Infof("Created cache group [%s] with cacheBytes=%d, expiration=%v", name, cacheBytes, g.expiration)
	return g
}

func GetGroup(name string) *Group {
	groupsMu.RLock()
	defer groupsMu.RUnlock()
	return groups[name]
}

func (g *Group) Get(ctx context.Context, key string) (ByteView, error) {
	if atomic.LoadInt32(&g.closed) == 1 {
		return ByteView{}, ErrGroupClosed
	}
	if key == "" {
		return ByteView{}, ErrKeyRequired
	}

	view, ok := g.mainCache.Get(ctx, key)
	if ok {
		g.stats.localHits.Add(1)
		return view, nil
	}

	g.stats.localMisses.Add(1)
	return g.load(ctx, key)
}

func (g *Group) GetLocal(ctx context.Context, key string) (ByteView, error) {
	if atomic.LoadInt32(&g.closed) == 1 {
		return ByteView{}, ErrGroupClosed
	}
	if key == "" {
		return ByteView{}, ErrKeyRequired
	}

	view, ok := g.mainCache.Get(ctx, key)
	if ok {
		g.stats.localHits.Add(1)
		return view, nil
	}

	g.stats.localMisses.Add(1)
	return ByteView{}, ErrCacheMiss
}

func (g *Group) Set(ctx context.Context, key string, value []byte) error {
	if atomic.LoadInt32(&g.closed) == 1 {
		return ErrGroupClosed
	}
	if key == "" {
		return ErrKeyRequired
	}
	if len(value) == 0 {
		return ErrValueRequired
	}

	return g.setToOwner(ctx, key, ByteView{b: cloneBytes(value)})
}

func (g *Group) SetLocal(ctx context.Context, key string, value []byte) error {
	if atomic.LoadInt32(&g.closed) == 1 {
		return ErrGroupClosed
	}
	if key == "" {
		return ErrKeyRequired
	}
	if len(value) == 0 {
		return ErrValueRequired
	}

	g.setLocal(key, ByteView{b: cloneBytes(value)})
	return nil
}

func (g *Group) setLocal(key string, view ByteView) {
	if g.expiration > 0 {
		g.mainCache.AddWithExpiration(key, view, time.Now().Add(g.expiration))
		return
	}
	g.mainCache.Add(key, view)
}

func (g *Group) setLocalWithTTL(key string, view ByteView, ttl time.Duration) {
	if ttl <= 0 {
		g.setLocal(key, view)
		return
	}
	g.mainCache.AddWithExpiration(key, view, time.Now().Add(ttl))
}

func (g *Group) setToOwner(ctx context.Context, key string, view ByteView) error {
	if g.peers != nil {
		peer, ok, isSelf := g.peers.PickPeer(key)
		if ok && !isSelf {
			setCtx, cancel := context.WithTimeout(ctx, peerSetTimeout)
			defer cancel()
			return peer.SetLocal(setCtx, g.name, key, view.ByteSLice())
		}
	}

	g.setLocal(key, view)
	return nil
}

func (g *Group) setLoadedValueToOwner(key string, view ByteView) {
	if g.peers != nil {
		peer, ok, isSelf := g.peers.PickPeer(key)
		if ok && !isSelf {
			ctx, cancel := context.WithTimeout(context.Background(), peerSetTimeout)
			defer cancel()
			if err := peer.SetLocal(ctx, g.name, key, view.ByteSLice()); err != nil {
				logrus.Warnf("[KamaCache] failed to set loaded key to owner: %v", err)
			}
			return
		}
	}

	g.setLocal(key, view)
}

func (g *Group) Delete(ctx context.Context, key string) error {
	if atomic.LoadInt32(&g.closed) == 1 {
		return ErrGroupClosed
	}
	if key == "" {
		return ErrKeyRequired
	}

	g.mainCache.Delete(key)
	isPeerRequest := ctx.Value("from_peer") != nil
	if !isPeerRequest && g.peers != nil {
		go g.syncToPeers(ctx, "delete", key, nil)
	}
	return nil
}

func (g *Group) syncToPeers(ctx context.Context, op string, key string, value []byte) {
	if g.peers == nil {
		return
	}

	peer, ok, isSelf := g.peers.PickPeer(key)
	if !ok || isSelf {
		return
	}

	syncCtx := context.WithValue(context.Background(), "from_peer", true)

	var err error
	switch op {
	case "set":
		err = peer.Set(syncCtx, g.name, key, value)
	case "delete":
		_, err = peer.Delete(g.name, key)
	}

	if err != nil {
		logrus.Errorf("[KamaCache] failed to sync %s to peer: %v", op, err)
	}
}

func (g *Group) Clear() {
	if atomic.LoadInt32(&g.closed) == 1 {
		return
	}

	g.mainCache.Clear()
	logrus.Infof("[KamaCache] cleared cache for group [%s]", g.name)
}

func (g *Group) Close() error {
	if !atomic.CompareAndSwapInt32(&g.closed, 0, 1) {
		return nil
	}

	if g.mainCache != nil {
		g.mainCache.Close()
	}

	groupsMu.Lock()
	delete(groups, g.name)
	groupsMu.Unlock()

	logrus.Infof("[KamaCache] closed cache group [%s]", g.name)
	return nil
}

func (g *Group) load(ctx context.Context, key string) (ByteView, error) {
	startTime := time.Now()
	viewi, err := g.loader.Do(key, func() (interface{}, error) {
		return g.loadData(ctx, key)
	})

	g.stats.loadDuration.Add(time.Since(startTime).Nanoseconds())
	g.stats.loads.Add(1)

	if err != nil {
		g.stats.loaderErrors.Add(1)
		return ByteView{}, err
	}

	result := viewi.(loadResult)
	if result.shouldSetOwner {
		go g.setLoadedValueToOwner(key, result.value)
	}
	return result.value, nil
}

func (g *Group) loadData(ctx context.Context, key string) (loadResult, error) {
	if g.peers != nil {
		peer, ok, isSelf := g.peers.PickPeer(key)
		if ok && !isSelf {
			value, err := g.getFromPeer(ctx, peer, key)
			if err == nil {
				g.stats.peerHits.Add(1)
				return loadResult{value: value, shouldSetOwner: false}, nil
			}

			g.stats.peerMisses.Add(1)
			logrus.Warnf("[KamaCache] failed to get from peer: %v", err)
		}
	}

	if picker, ok := g.peers.(oldPeerPicker); ok {
		if oldPeer, ok, isSelf := picker.PickOldPeer(key); ok && !isSelf {
			value, err := g.getFromPeer(ctx, oldPeer, key)
			if err == nil {
				logrus.Infof("[KamaCache] lazy migrate key=%s", key)
				return loadResult{value: value, shouldSetOwner: true}, nil
			}

			g.stats.peerMisses.Add(1)
		}
	}

	bytes, err := g.getter.Get(ctx, key)
	if err != nil {
		return loadResult{}, fmt.Errorf("failed to get data: %w", err)
	}

	g.stats.loaderHits.Add(1)
	return loadResult{
		value:          ByteView{b: cloneBytes(bytes)},
		shouldSetOwner: true,
	}, nil
}

func (g *Group) getFromPeer(_ context.Context, peer Peer, key string) (ByteView, error) {
	bytes, err := peer.GetLocal(g.name, key)
	if err != nil {
		return ByteView{}, fmt.Errorf("failed to get from peer: %w", err)
	}

	view := ByteView{b: cloneBytes(bytes)}
	g.setLocalWithTTL(key, view, peerReplicaTTL)
	return view, nil
}

func (g *Group) RegisterPeers(peers PeerPicker) {
	if g.peers != nil {
		panic("RegisterPeers called more than once")
	}
	g.peers = peers
	logrus.Infof("[KamaCache] registered peers for group [%s]", g.name)
}

func (g *Group) Stats() map[string]interface{} {
	stats := map[string]interface{}{
		"name":          g.name,
		"closed":        atomic.LoadInt32(&g.closed) == 1,
		"expiration":    g.expiration,
		"loads":         g.stats.loads.Load(),
		"local_hits":    g.stats.localHits.Load(),
		"local_misses":  g.stats.localMisses.Load(),
		"peer_hits":     g.stats.peerHits.Load(),
		"peer_misses":   g.stats.peerMisses.Load(),
		"loader_hits":   g.stats.loaderHits.Load(),
		"loader_errors": g.stats.loaderErrors.Load(),
	}

	totalGets := stats["local_hits"].(int64) + stats["local_misses"].(int64)
	if totalGets > 0 {
		stats["hit_rate"] = float64(stats["local_hits"].(int64)) / float64(totalGets)
	}

	totalLoads := stats["loads"].(int64)
	if totalLoads > 0 {
		stats["avg_load_time_ms"] = float64(g.stats.loadDuration.Load()) / float64(totalLoads) / float64(time.Millisecond)
	}

	if g.mainCache != nil {
		cacheStats := g.mainCache.Stats()
		for k, v := range cacheStats {
			stats["cache_"+k] = v
		}
	}

	return stats
}

func ListGroups() []string {
	groupsMu.RLock()
	defer groupsMu.RUnlock()

	names := make([]string, 0, len(groups))
	for name := range groups {
		names = append(names, name)
	}
	return names
}

func DestroyGroup(name string) bool {
	groupsMu.Lock()
	defer groupsMu.Unlock()

	if g, exists := groups[name]; exists {
		g.Close()
		delete(groups, name)
		logrus.Infof("[KamaCache] destroyed cache group [%s]", name)
		return true
	}

	return false
}

func DestroyAllGroups() {
	groupsMu.Lock()
	defer groupsMu.Unlock()

	for name, g := range groups {
		g.Close()
		delete(groups, name)
		logrus.Infof("[KamaCache] destroyed cache group [%s]", name)
	}
}
