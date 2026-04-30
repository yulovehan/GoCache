package main

import (
	"fmt"
	"log"
	"time"

	lcache "github.com/youngyangyang04/KamaCache-Go"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func main() {
	// 创建 etcd 客户端
	etcdCli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"localhost:2379"},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		log.Fatal(err)
	}

	// 模拟访问不同节点
	addrs := []string{
		"localhost:8001",
		"localhost:8002",
		"localhost:8003",
	}

	// 测试多个 key
	keys := []string{
		"key_A", "key_B", "key_C",
		"key_0", "key_1", "key_2",
	}

	for _, addr := range addrs {
		fmt.Printf("\n===== 连接节点 %s =====\n", addr)

		client, err := lcache.NewClient(addr, "kama-cache", etcdCli)
		if err != nil {
			log.Println("连接失败:", err)
			continue
		}

		for _, key := range keys {
			val, err := client.Get("test", key)
			if err != nil {
				fmt.Printf("获取 %s 失败: %v\n", key, err)
				continue
			}
			fmt.Printf("获取 %s 成功: %s\n", key, string(val))
		}

		client.Close()
	}
	group := lcache.GetGroup("test")
	fmt.Printf("group %s stats :%+v", "test", group.Stats())
}
