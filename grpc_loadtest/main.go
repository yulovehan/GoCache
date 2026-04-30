package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	kamacache "github.com/youngyangyang04/KamaCache-Go"
)

type request struct {
	id  int
	key string
	op  string
}

func main() {
	var (
		addr        = flag.String("addr", "localhost:8001", "target KamaCache gRPC address")
		group       = flag.String("group", "test", "cache group name")
		total       = flag.Int("n", 10000, "total request count")
		concurrency = flag.Int("c", 200, "concurrent workers")
		keyspace    = flag.Int("keys", 1000, "number of distinct keys")
		hotKeys     = flag.Int("hot-keys", 100, "number of hot keys")
		hotRatio    = flag.Float64("hot", 0.8, "ratio of requests sent to hot keys, from 0 to 1")
		setRatio    = flag.Float64("set", 0, "ratio of Set requests, from 0 to 1")
		adminAddr   = flag.String("admin", "", "optional admin HTTP address for stats hint, e.g. localhost:9001")
	)
	flag.Parse()

	normalizeArgs(total, concurrency, keyspace, hotKeys, hotRatio, setRatio)

	fmt.Printf("Connecting to %s...\n", *addr)
	client, err := kamacache.NewClient(*addr, "kama-cache", nil)
	if err != nil {
		panic(err)
	}
	defer client.Close()
	fmt.Println("Connected.")

	jobs := make(chan request, *concurrency)
	latencies := make([]time.Duration, *total)

	var okCount int64
	var errCount int64
	var getCount int64
	var setCount int64
	errorStats := newErrorStats()

	var wg sync.WaitGroup
	start := time.Now()

	for worker := 0; worker < *concurrency; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for req := range jobs {
				begin := time.Now()
				err := runRequest(client, *group, req)
				latencies[req.id] = time.Since(begin)

				if err != nil {
					atomic.AddInt64(&errCount, 1)
					errorStats.add(err)
					continue
				}
				atomic.AddInt64(&okCount, 1)
				if req.op == "set" {
					atomic.AddInt64(&setCount, 1)
				} else {
					atomic.AddInt64(&getCount, 1)
				}
			}
		}()
	}

	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < *total; i++ {
		op := "get"
		if r.Float64() < *setRatio {
			op = "set"
		}
		jobs <- request{
			id:  i,
			key: nextKey(r, *keyspace, *hotKeys, *hotRatio),
			op:  op,
		}
	}
	close(jobs)
	wg.Wait()

	elapsed := time.Since(start)
	sort.Slice(latencies, func(i, j int) bool {
		return latencies[i] < latencies[j]
	})

	fmt.Println("=== KamaCache gRPC Load Test ===")
	fmt.Printf("target:        %s\n", *addr)
	fmt.Printf("group:         %s\n", *group)
	fmt.Printf("requests:      %d\n", *total)
	fmt.Printf("concurrency:   %d\n", *concurrency)
	fmt.Printf("keyspace:      %d\n", *keyspace)
	fmt.Printf("hot_keys:      %d\n", *hotKeys)
	fmt.Printf("hot_ratio:     %.2f\n", *hotRatio)
	fmt.Printf("set_ratio:     %.2f\n", *setRatio)
	fmt.Printf("elapsed:       %s\n", elapsed)
	fmt.Printf("qps:           %.2f\n", float64(*total)/elapsed.Seconds())
	fmt.Printf("ok/errors:     %d/%d\n", okCount, errCount)
	fmt.Printf("get/set:       %d/%d\n", getCount, setCount)
	fmt.Printf("p50/p95/p99:   %s / %s / %s\n", percentile(latencies, 50), percentile(latencies, 95), percentile(latencies, 99))
	fmt.Printf("")
	printErrorStats(errorStats.snapshot())

	if *adminAddr != "" {
		fmt.Println()
		printAdminStatus(*adminAddr, *group)
	}
}

type errorStats struct {
	mu     sync.Mutex
	counts map[string]int
}

func newErrorStats() *errorStats {
	return &errorStats{
		counts: make(map[string]int),
	}
}

func (s *errorStats) add(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.counts[classifyError(err)]++
}

func (s *errorStats) snapshot() map[string]int {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make(map[string]int, len(s.counts))
	for kind, count := range s.counts {
		result[kind] = count
	}
	return result
}

func normalizeArgs(total, concurrency, keyspace, hotKeys *int, hotRatio, setRatio *float64) {
	if *total <= 0 || *concurrency <= 0 || *keyspace <= 0 {
		panic("n, c, and keys must be greater than 0")
	}
	if *hotKeys < 0 {
		*hotKeys = 0
	}
	if *hotKeys > *keyspace {
		*hotKeys = *keyspace
	}
	if *hotRatio < 0 {
		*hotRatio = 0
	}
	if *hotRatio > 1 {
		*hotRatio = 1
	}
	if *setRatio < 0 {
		*setRatio = 0
	}
	if *setRatio > 1 {
		*setRatio = 1
	}
}

func runRequest(client *kamacache.Client, group string, req request) error {
	if req.op == "set" {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		return client.Set(ctx, group, req.key, []byte("value:"+req.key))
	}

	_, err := client.Get(group, req.key)
	return err
}

func classifyError(err error) string {
	msg := err.Error()
	lower := strings.ToLower(msg)

	switch {
	case strings.Contains(lower, "deadlineexceeded") || strings.Contains(lower, "deadline exceeded") || strings.Contains(lower, "context deadline"):
		return "deadline_exceeded"
	case strings.Contains(lower, "connection refused"):
		return "connection_refused"
	case strings.Contains(lower, "connection reset"):
		return "connection_reset"
	case strings.Contains(lower, "unavailable"):
		return "grpc_unavailable"
	case strings.Contains(lower, "not found"):
		return "not_found"
	case strings.Contains(lower, "status is not active"):
		return "node_not_active"
	default:
		return trimError(msg)
	}
}

func trimError(msg string) string {
	msg = strings.TrimSpace(msg)
	if len(msg) <= 100 {
		return msg
	}
	return msg[:100] + "..."
}

func printErrorStats(stats map[string]int) {
	fmt.Println()
	fmt.Println("=== Error Types ===")
	if len(stats) == 0 {
		fmt.Println("none")
		return
	}

	keys := make([]string, 0, len(stats))
	for kind := range stats {
		keys = append(keys, kind)
	}
	sort.Slice(keys, func(i, j int) bool {
		if stats[keys[i]] == stats[keys[j]] {
			return keys[i] < keys[j]
		}
		return stats[keys[i]] > stats[keys[j]]
	})

	for _, kind := range keys {
		fmt.Printf("%-24s %d\n", kind+":", stats[kind])
	}
}

func printAdminStatus(adminAddr string, group string) {
	stats, err := fetchJSON(adminAddr, "/api/stats")
	if err != nil {
		fmt.Printf("Admin stats error: %v\n", err)
		fmt.Printf("Dashboard:         http://%s\n", adminAddr)
		return
	}

	nodes, nodesErr := fetchJSON(adminAddr, "/api/nodes")
	ring, ringErr := fetchJSON(adminAddr, "/api/hash-ring")

	fmt.Println("=== Admin Status ===")
	fmt.Printf("Dashboard:          http://%s\n", adminAddr)
	fmt.Printf("Stats API:          http://%s/api/stats\n", adminAddr)
	printServerStatus(stats)
	printGroupStatus(stats, group)
	if nodesErr == nil {
		printNodeStatus(nodes)
	} else {
		fmt.Printf("nodes_error:        %v\n", nodesErr)
	}
	if ringErr == nil {
		printRingStatus(ring)
	} else {
		fmt.Printf("ring_error:         %v\n", ringErr)
	}
}

func fetchJSON(adminAddr string, path string) (map[string]interface{}, error) {
	client := http.Client{Timeout: 3 * time.Second}
	resp, err := client.Get("http://" + adminAddr + path)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("%s returned %s", path, resp.Status)
	}

	var data map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return nil, err
	}
	return data, nil
}

func printServerStatus(stats map[string]interface{}) {
	server, ok := asMap(stats["server"])
	if !ok {
		return
	}
	fmt.Printf("server_addr:        %v\n", server["addr"])
	fmt.Printf("server_status:      %v\n", server["status"])
	fmt.Printf("service_name:       %v\n", server["svc_name"])
}

func clusterHitRate(groupStats map[string]interface{}) float64 {
	localHits := toFloat64(groupStats["local_hits"])
	peerHits := toFloat64(groupStats["peer_hits"])
	localMisses := toFloat64(groupStats["local_misses"])

	total := localHits + localMisses
	if total == 0 {
		return 0
	}

	return (localHits + peerHits) / total
}

func toFloat64(value interface{}) float64 {
	switch v := value.(type) {
	case float64:
		return v
	case int:
		return float64(v)
	case int64:
		return float64(v)
	case json.Number:
		f, _ := v.Float64()
		return f
	default:
		return 0
	}
}

func printGroupStatus(stats map[string]interface{}, group string) {
	groups, ok := asMap(stats["groups"])
	if !ok {
		fmt.Println("groups:             none")
		return
	}

	groupStats, ok := asMap(groups[group])
	if !ok {
		fmt.Printf("group_%s:           not found\n", group)
		return
	}

	fmt.Printf("group:              %s\n", group)
	fmt.Printf("loads:              %s\n", valueString(groupStats["loads"]))
	fmt.Printf("local_hits:         %s\n", valueString(groupStats["local_hits"]))
	fmt.Printf("local_misses:       %s\n", valueString(groupStats["local_misses"]))
	fmt.Printf("peer_hits:          %s\n", valueString(groupStats["peer_hits"]))
	fmt.Printf("peer_misses:        %s\n", valueString(groupStats["peer_misses"]))
	fmt.Printf("loader_hits:        %s\n", valueString(groupStats["loader_hits"]))
	fmt.Printf("loader_errors:      %s\n", valueString(groupStats["loader_errors"]))
	fmt.Printf("hit_rate:           %s\n", percentString(groupStats["hit_rate"]))
	fmt.Printf("cache_size:         %s\n", valueString(groupStats["cache_size"]))
	fmt.Printf("cache_hit_rate:     %s\n", percentString(groupStats["cache_hit_rate"]))
	fmt.Printf("cluster_hit_rate:   %s\n", percentString(clusterHitRate(groupStats)))
	fmt.Printf("avg_load_time_ms:   %s\n", valueString(groupStats["avg_load_time_ms"]))
}

func printNodeStatus(nodes map[string]interface{}) {
	picker, ok := asMap(nodes["picker"])
	if !ok {
		return
	}
	peers, ok := picker["peers"].([]interface{})
	if !ok {
		return
	}

	active := 0
	draining := 0
	offline := 0
	for _, item := range peers {
		peer, ok := asMap(item)
		if !ok {
			continue
		}
		switch fmt.Sprint(peer["status"]) {
		case "Active":
			active++
		case "Draining":
			draining++
		case "Offline":
			offline++
		}
	}

	fmt.Printf("nodes_total:        %d\n", len(peers))
	fmt.Printf("nodes_active:       %d\n", active)
	fmt.Printf("nodes_draining:     %d\n", draining)
	fmt.Printf("nodes_offline:      %d\n", offline)
}

func printRingStatus(ring map[string]interface{}) {
	ringData, ok := asMap(ring["ring"])
	if !ok {
		return
	}
	virtualNodes, _ := ringData["virtual_nodes"].([]interface{})
	fmt.Printf("ring_virtual_nodes: %d\n", len(virtualNodes))
	fmt.Printf("ring_requests:      %s\n", valueString(ringData["total_requests"]))
}

func asMap(value interface{}) (map[string]interface{}, bool) {
	result, ok := value.(map[string]interface{})
	return result, ok
}

func valueString(value interface{}) string {
	if value == nil {
		return "0"
	}
	return fmt.Sprint(value)
}

func percentString(value interface{}) string {
	number, ok := value.(float64)
	if !ok {
		return "0%"
	}
	return fmt.Sprintf("%.2f%%", number*100)
}

func nextKey(r *rand.Rand, keyspace, hotKeys int, hotRatio float64) string {
	if hotKeys > 0 && r.Float64() < hotRatio {
		return fmt.Sprintf("key_%d", r.Intn(hotKeys))
	}
	return fmt.Sprintf("key_%d", r.Intn(keyspace))
}

func percentile(values []time.Duration, p int) time.Duration {
	if len(values) == 0 {
		return 0
	}
	idx := len(values) * p / 100
	if idx >= len(values) {
		idx = len(values) - 1
	}
	return values[idx]
}
