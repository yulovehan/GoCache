package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	lcache "github.com/youngyangyang04/KamaCache-Go"
	"github.com/youngyangyang04/KamaCache-Go/admin/controller"
	"github.com/youngyangyang04/KamaCache-Go/admin/router"
)

func main() {
	port := flag.Int("port", 8001, "cache grpc port")
	adminPort := flag.Int("admin-port", 9001, "admin http port")
	nodeID := flag.String("node", "A", "node id")
	flag.Parse()

	addr := fmt.Sprintf("localhost:%d", *port)
	log.Printf("[node %s] starting grpc server at %s", *nodeID, addr)

	node, err := lcache.NewServer(addr, "kama-cache",
		lcache.WithEtcdEndpoints([]string{"localhost:2379"}),
		lcache.WithDialTimeout(5*time.Second),
	)
	if err != nil {
		log.Fatal("create node failed:", err)
	}

	picker, err := lcache.NewClientPicker(addr)
	if err != nil {
		log.Fatal("create peer picker failed:", err)
	}

	group := lcache.NewGroup("test", 32<<20, lcache.GetterFunc(
		func(ctx context.Context, key string) ([]byte, error) {
			log.Printf("[node %s] load from source key=%s", *nodeID, key)
			return []byte(fmt.Sprintf("source-value-from-%s:%s", *nodeID, key)), nil
		}),
	)
	group.RegisterPeers(picker)

	admin := controller.NewAdminController(node, picker)
	adminRouter := router.NewRouter(admin, dashboardPath())
	go func() {
		adminAddr := fmt.Sprintf("localhost:%d", *adminPort)
		log.Printf("[node %s] admin http server at http://%s", *nodeID, adminAddr)
		if err := adminRouter.Run(adminAddr); err != nil {
			log.Fatal("admin http server failed:", err)
		}
	}()

	go func() {
		log.Printf("[node %s] grpc server serving...", *nodeID)
		if err := node.Start(); err != nil {
			log.Fatal("grpc server failed:", err)
		}
	}()

	log.Printf("[node %s] waiting for service registration...", *nodeID)
	time.Sleep(5 * time.Second)

	ctx := context.Background()
	localKey := fmt.Sprintf("key_%s", *nodeID)
	localValue := []byte(fmt.Sprintf("value-from-node-%s", *nodeID))

	if err := group.Set(ctx, localKey, localValue); err != nil {
		log.Fatal("set local key failed:", err)
	}
	log.Printf("[node %s] set %s ok", *nodeID, localKey)

	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("key_%d", i)
		value := []byte(fmt.Sprintf("value_%d_from_%s", i, *nodeID))
		if err := group.Set(ctx, key, value); err != nil {
			log.Fatal(err)
		}
	}

	log.Printf("[node %s] ready; try http://localhost:%d", *nodeID, *adminPort)
	select {}
}

func dashboardPath() string {
	path := "dashboard.html"
	if _, err := os.Stat(path); err == nil {
		return path
	}
	return filepath.Join("..", "dashboard.html")
}
