package consistenthash

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
)

// Map implements a stable consistent-hash ring.
type Map struct {
	mu sync.RWMutex

	totalRequests atomic.Int64
	config        *Config
	keys          []int
	hashMap       map[int]string
	nodeReplicas  map[string]int
	nodeCounts    map[string]int64
}

type VirtualNodeSnapshot struct {
	Hash int    `json:"hash"`
	Addr string `json:"addr"`
}

type Snapshot struct {
	VirtualNodes  []VirtualNodeSnapshot `json:"virtual_nodes"`
	NodeReplicas  map[string]int        `json:"node_replicas"`
	NodeCounts    map[string]int64      `json:"node_counts"`
	TotalRequests int64                 `json:"total_requests"`
}

func New(opts ...Option) *Map {
	m := &Map{
		config:       DefaultConfig,
		hashMap:      make(map[int]string),
		nodeReplicas: make(map[string]int),
		nodeCounts:   make(map[string]int64),
	}

	for _, opt := range opts {
		opt(m)
	}

	return m
}

type Option func(*Map)

func WithConfig(config *Config) Option {
	return func(m *Map) {
		m.config = config
	}
}

func (m *Map) Add(nodes ...string) error {
	if len(nodes) == 0 {
		return errors.New("no nodes provided")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	for _, node := range nodes {
		if node == "" {
			continue
		}
		m.addNode(node, m.config.DefaultReplicas)
	}

	sort.Ints(m.keys)
	return nil
}

func (m *Map) Remove(node string) error {
	if node == "" {
		return errors.New("invalid node")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	return m.removeNodeLocked(node)
}

func (m *Map) Clone() *Map {
	m.mu.RLock()
	defer m.mu.RUnlock()

	cloned := &Map{
		config:       m.config,
		keys:         append([]int(nil), m.keys...),
		hashMap:      make(map[int]string, len(m.hashMap)),
		nodeReplicas: make(map[string]int, len(m.nodeReplicas)),
		nodeCounts:   make(map[string]int64, len(m.nodeCounts)),
	}
	cloned.totalRequests.Store(m.totalRequests.Load())

	for hash, node := range m.hashMap {
		cloned.hashMap[hash] = node
	}
	for node, replicas := range m.nodeReplicas {
		cloned.nodeReplicas[node] = replicas
	}
	for node, count := range m.nodeCounts {
		cloned.nodeCounts[node] = count
	}

	return cloned
}

func (m *Map) Snapshot() Snapshot {
	m.mu.RLock()
	defer m.mu.RUnlock()

	virtualNodes := make([]VirtualNodeSnapshot, 0, len(m.keys))
	for _, hash := range m.keys {
		virtualNodes = append(virtualNodes, VirtualNodeSnapshot{
			Hash: hash,
			Addr: m.hashMap[hash],
		})
	}

	nodeReplicas := make(map[string]int, len(m.nodeReplicas))
	for node, replicas := range m.nodeReplicas {
		nodeReplicas[node] = replicas
	}

	nodeCounts := make(map[string]int64, len(m.nodeCounts))
	for node, count := range m.nodeCounts {
		nodeCounts[node] = count
	}

	return Snapshot{
		VirtualNodes:  virtualNodes,
		NodeReplicas:  nodeReplicas,
		NodeCounts:    nodeCounts,
		TotalRequests: m.totalRequests.Load(),
	}
}

func (m *Map) Get(key string) string {
	if key == "" {
		return ""
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if len(m.keys) == 0 {
		return ""
	}

	hash := int(m.config.HashFunc([]byte(key)))
	idx := sort.Search(len(m.keys), func(i int) bool {
		return m.keys[i] >= hash
	})
	if idx == len(m.keys) {
		idx = 0
	}

	node := m.hashMap[m.keys[idx]]
	m.nodeCounts[node]++
	m.totalRequests.Add(1)
	return node
}

func (m *Map) GetStats() map[string]float64 {
	m.mu.RLock()
	defer m.mu.RUnlock()

	stats := make(map[string]float64)
	total := m.totalRequests.Load()
	if total == 0 {
		return stats
	}

	for node, count := range m.nodeCounts {
		stats[node] = float64(count) / float64(total)
	}
	return stats
}

func (m *Map) addNode(node string, replicas int) {
	for i := 0; i < replicas; i++ {
		hash := int(m.config.HashFunc([]byte(fmt.Sprintf("%s-%d", node, i))))
		m.keys = append(m.keys, hash)
		m.hashMap[hash] = node
	}
	m.nodeReplicas[node] = replicas
}

func (m *Map) removeNodeLocked(node string) error {
	replicas := m.nodeReplicas[node]
	if replicas == 0 {
		return fmt.Errorf("node %s not found", node)
	}

	for i := 0; i < replicas; i++ {
		hash := int(m.config.HashFunc([]byte(fmt.Sprintf("%s-%d", node, i))))
		delete(m.hashMap, hash)
		for j := 0; j < len(m.keys); j++ {
			if m.keys[j] == hash {
				m.keys = append(m.keys[:j], m.keys[j+1:]...)
				break
			}
		}
	}

	delete(m.nodeReplicas, node)
	delete(m.nodeCounts, node)
	return nil
}
