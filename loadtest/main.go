package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	kamacache "github.com/youngyangyang04/KamaCache-Go"
)

func main() {
	var (
		total       = flag.Int("n", 10000, "total request count")
		concurrency = flag.Int("c", 200, "concurrent workers")
		keyspace    = flag.Int("keys", 1000, "number of distinct keys")
		preload     = flag.Int("preload", 700, "number of keys to preload before the test")
		hotRatio    = flag.Float64("hot", 0.8, "ratio of requests sent to preloaded hot keys, from 0 to 1")
		expiration  = flag.Duration("ttl", 0, "cache entry ttl, 0 means never expire")
	)
	flag.Parse()

	if *total <= 0 || *concurrency <= 0 || *keyspace <= 0 {
		panic("n, c, and keys must be greater than 0")
	}
	if *preload < 0 {
		*preload = 0
	}
	if *preload > *keyspace {
		*preload = *keyspace
	}
	if *hotRatio < 0 {
		*hotRatio = 0
	}
	if *hotRatio > 1 {
		*hotRatio = 1
	}

	ctx := context.Background()
	var sourceLoads int64

	group := kamacache.NewGroup("loadtest", 64<<20, kamacache.GetterFunc(
		func(ctx context.Context, key string) ([]byte, error) {
			atomic.AddInt64(&sourceLoads, 1)
			return []byte("value:" + key), nil
		}),
		kamacache.WithExpiration(*expiration),
	)
	defer group.Close()

	for i := 0; i < *preload; i++ {
		key := fmt.Sprintf("key:%d", i)
		if err := group.Set(ctx, key, []byte("value:"+key)); err != nil {
			panic(err)
		}
	}

	jobs := make(chan request, *concurrency)
	latencies := make([]time.Duration, *total)
	var okCount int64
	var errCount int64
	var wg sync.WaitGroup

	start := time.Now()
	for worker := 0; worker < *concurrency; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for req := range jobs {
				begin := time.Now()
				_, err := group.Get(ctx, req.key)
				latencies[req.id] = time.Since(begin)
				if err != nil {
					atomic.AddInt64(&errCount, 1)
					continue
				}
				atomic.AddInt64(&okCount, 1)
			}
		}()
	}

	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < *total; i++ {
		key := nextKey(r, *keyspace, *preload, *hotRatio)
		jobs <- request{id: i, key: key}
	}
	close(jobs)
	wg.Wait()

	elapsed := time.Since(start)
	sort.Slice(latencies, func(i, j int) bool {
		return latencies[i] < latencies[j]
	})

	stats := group.Stats()
	fmt.Println("=== KamaCache Load Test ===")
	fmt.Printf("requests:      %d\n", *total)
	fmt.Printf("concurrency:   %d\n", *concurrency)
	fmt.Printf("keyspace:      %d\n", *keyspace)
	fmt.Printf("preload:       %d\n", *preload)
	fmt.Printf("hot_ratio:     %.2f\n", *hotRatio)
	fmt.Printf("elapsed:       %s\n", elapsed)
	fmt.Printf("qps:           %.2f\n", float64(*total)/elapsed.Seconds())
	fmt.Printf("ok/errors:     %d/%d\n", okCount, errCount)
	fmt.Printf("source_loads:  %d\n", sourceLoads)
	fmt.Printf("p50/p95/p99:   %s / %s / %s\n", percentile(latencies, 50), percentile(latencies, 95), percentile(latencies, 99))
	fmt.Println()
	fmt.Println("=== Group Stats ===")
	printStat(stats, "loads")
	printStat(stats, "local_hits")
	printStat(stats, "local_misses")
	printStat(stats, "hit_rate")
	printStat(stats, "loader_hits")
	printStat(stats, "loader_errors")
	printStat(stats, "avg_load_time_ms")
	printStat(stats, "cache_size")
	printStat(stats, "cache_hits")
	printStat(stats, "cache_misses")
	printStat(stats, "cache_hit_rate")
}

type request struct {
	id  int
	key string
}

func nextKey(r *rand.Rand, keyspace, preload int, hotRatio float64) string {
	if preload > 0 && r.Float64() < hotRatio {
		return fmt.Sprintf("key:%d", r.Intn(preload))
	}
	return fmt.Sprintf("key:%d", r.Intn(keyspace))
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

func printStat(stats map[string]interface{}, key string) {
	if value, ok := stats[key]; ok {
		fmt.Printf("%-17s %v\n", key+":", value)
	}
}
