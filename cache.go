package kamacache

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/youngyangyang04/KamaCache-Go/store"
)

// Cache wraps the underlying store implementation.
type Cache struct {
	mu          sync.RWMutex
	hits        atomic.Int64
	misses      atomic.Int64
	store       store.Store
	opts        CacheOptions
	initialized int32
	closed      int32
}

type CacheOptions struct {
	CacheType    store.CacheType
	MaxBytes     int64
	BucketCount  uint16
	CapPerBucket uint16
	Level2Cap    uint16
	CleanupTime  time.Duration
	OnEvicted    func(key string, value store.Value)
}

func DefaultCacheOptions() CacheOptions {
	return CacheOptions{
		CacheType:    store.LRU,
		MaxBytes:     8 * 1024 * 1024,
		BucketCount:  16,
		CapPerBucket: 512,
		Level2Cap:    256,
		CleanupTime:  time.Minute,
	}
}

func NewCache(opts CacheOptions) *Cache {
	return &Cache{opts: opts}
}

func (c *Cache) ensureInitialized() {
	if atomic.LoadInt32(&c.initialized) == 1 {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.initialized == 1 {
		return
	}

	storeOpts := store.Options{
		MaxBytes:        c.opts.MaxBytes,
		BucketCount:     c.opts.BucketCount,
		CapPerBucket:    c.opts.CapPerBucket,
		Level2Cap:       c.opts.Level2Cap,
		CleanupInterval: c.opts.CleanupTime,
		OnEvicted:       c.opts.OnEvicted,
	}

	c.store = store.NewStore(c.opts.CacheType, storeOpts)
	atomic.StoreInt32(&c.initialized, 1)
	logrus.Infof("Cache initialized with type %s, max bytes: %d", c.opts.CacheType, c.opts.MaxBytes)
}

func (c *Cache) Add(key string, value ByteView) {
	if atomic.LoadInt32(&c.closed) == 1 {
		logrus.Warnf("Attempted to add to a closed cache: %s", key)
		return
	}

	c.ensureInitialized()
	if err := c.store.Set(key, value); err != nil {
		logrus.Warnf("Failed to add key %s to cache: %v", key, err)
	}
}

func (c *Cache) Get(ctx context.Context, key string) (ByteView, bool) {
	if atomic.LoadInt32(&c.closed) == 1 {
		return ByteView{}, false
	}

	if atomic.LoadInt32(&c.initialized) == 0 {
		c.misses.Add(1)
		return ByteView{}, false
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	val, found := c.store.Get(key)
	if !found {
		c.misses.Add(1)
		return ByteView{}, false
	}

	view, ok := val.(ByteView)
	if !ok {
		logrus.Warnf("Type assertion failed for key %s, expected ByteView", key)
		c.misses.Add(1)
		return ByteView{}, false
	}

	c.hits.Add(1)
	return view, true
}

func (c *Cache) AddWithExpiration(key string, value ByteView, expirationTime time.Time) {
	if atomic.LoadInt32(&c.closed) == 1 {
		logrus.Warnf("Attempted to add to a closed cache: %s", key)
		return
	}

	c.ensureInitialized()

	expiration := time.Until(expirationTime)
	if expiration <= 0 {
		logrus.Debugf("Key %s already expired, not adding to cache", key)
		return
	}

	if err := c.store.SetWithExpiration(key, value, expiration); err != nil {
		logrus.Warnf("Failed to add key %s to cache with expiration: %v", key, err)
	}
}

func (c *Cache) Delete(key string) bool {
	if atomic.LoadInt32(&c.closed) == 1 || atomic.LoadInt32(&c.initialized) == 0 {
		return false
	}

	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.store.Delete(key)
}

func (c *Cache) Clear() {
	if atomic.LoadInt32(&c.closed) == 1 || atomic.LoadInt32(&c.initialized) == 0 {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.store.Clear()
	c.hits.Store(0)
	c.misses.Store(0)
}

func (c *Cache) Len() int {
	if atomic.LoadInt32(&c.closed) == 1 || atomic.LoadInt32(&c.initialized) == 0 {
		return 0
	}

	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.store.Len()
}

func (c *Cache) Close() {
	if !atomic.CompareAndSwapInt32(&c.closed, 0, 1) {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.store != nil {
		if closer, ok := c.store.(interface{ Close() }); ok {
			closer.Close()
		}
		c.store = nil
	}

	atomic.StoreInt32(&c.initialized, 0)
	logrus.Debugf("Cache closed, hits: %d, misses: %d", c.hits.Load(), c.misses.Load())
}

func (c *Cache) Stats() map[string]interface{} {
	stats := map[string]interface{}{
		"initialized": atomic.LoadInt32(&c.initialized) == 1,
		"closed":      atomic.LoadInt32(&c.closed) == 1,
		"hits":        c.hits.Load(),
		"misses":      c.misses.Load(),
	}

	if atomic.LoadInt32(&c.initialized) == 1 {
		stats["size"] = c.Len()
		totalRequests := stats["hits"].(int64) + stats["misses"].(int64)
		if totalRequests > 0 {
			stats["hit_rate"] = float64(stats["hits"].(int64)) / float64(totalRequests)
		} else {
			stats["hit_rate"] = 0.0
		}
	}

	return stats
}
