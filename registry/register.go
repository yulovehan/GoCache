package registry

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/sirupsen/logrus"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// Config 定义etcd客户端配置
type Config struct {
	Endpoints   []string      // 集群地址
	DialTimeout time.Duration // 连接超时时间
}

// DefaultConfig 提供默认配置
var DefaultConfig = &Config{
	Endpoints:   []string{"localhost:2379"},
	DialTimeout: 5 * time.Second,
}

// Register 注册服务到etcd
func Register(etcdCli *clientv3.Client, svcName, addr string, stopCh <-chan error) error {
	// cli, err := clientv3.New(clientv3.Config{
	// 	Endpoints:   DefaultConfig.Endpoints,
	// 	DialTimeout: DefaultConfig.DialTimeout,
	// })
	// if err != nil {
	// 	return fmt.Errorf("failed to create etcd client: %v", err)
	// }
	cli := etcdCli
	// 短变量声明 := 规则,:= 至少要声明一个新变量,Go 实际做的是：localIP:新声明 ,err:重新赋值
	localIP, err := getLocalIP()
	if err != nil {
		cli.Close()
		return fmt.Errorf("failed to get local IP: %v", err)
	}
	if addr[0] == ':' {
		addr = fmt.Sprintf("%s%s", localIP, addr)
	}

	// 创建租约, 在 etcd 中创建一个带 TTL 的租约（lease），有效期 10 秒。
	lease, err := cli.Grant(context.Background(), 10) // 增加租约时间到10秒
	if err != nil {
		cli.Close()
		return fmt.Errorf("failed to create lease: %v", err)
	}

	// 注册服务，使用完整的key路径,如/services/kamacache/10.0.0.5:8080
	key := fmt.Sprintf("/services/%s/%s", svcName, addr)
	// 写入 etcd，key = /services/kamacache/10.0.0.5:8080   value = 10.0.0.5:8080
	_, err = cli.Put(context.Background(), key, addr, clientv3.WithLease(lease.ID))
	if err != nil {
		cli.Close()
		return fmt.Errorf("failed to put key-value to etcd: %v", err)
	}

	// 保持租约
	// 如果服务正常：TTL 会不断刷新：10 → 10 → 10
	// 如果服务崩溃：KeepAlive 停止，租约到期后lease 过期，etcd 自动删除到期的leaseID对应的key：/services/kamacache/10.0.0.6:8080
	keepAliveCh, err := cli.KeepAlive(context.Background(), lease.ID)
	if err != nil {
		cli.Close()
		return fmt.Errorf("failed to keep lease alive: %v", err)
	}

	// 处理租约续期和服务注销
	go func() {
		defer cli.Close()
		for {
			select {
			case <-stopCh:
				// 服务注销，撤销租约
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				// 主动删除 lease，因为 key 绑定 lease，撤
				// 如果 cli.Revoke 3秒内没完成，ctx.Done() 关闭，etcd client 会检测到 context 取消，从而：请求终止
				// 撤销（Revoke）一个 etcd 的租约（Lease），从而让 绑定在这个租约上的所有 key 立即删除。
				cli.Revoke(ctx, lease.ID)
				cancel()
				return
			case resp, ok := <-keepAliveCh:
				if !ok {
					logrus.Warn("keep alive channel closed")
					return
				}
				logrus.Debugf("successfully renewed lease: %d", resp.ID)
			}
		}
	}()

	logrus.Infof("Service registered: %s at %s", svcName, addr)
	return nil
}

// 从本机所有网络接口中找到一个可用的 IPv4 地址（非 127.0.0.1）并返回
func getLocalIP() (string, error) {
	// net.InterfaceAddrs()当前机器所有网络接口的地址
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "", err
	}

	for _, addr := range addrs {
		// addr可能是 *net.IPNet,*net.IPAddr， 所以做断言判断
		// ipNet.IP.IsLoopback()排除回环地址 127.0.0.1
		if ipNet, ok := addr.(*net.IPNet); ok && !ipNet.IP.IsLoopback() {
			// To4() 的行为:地址.To4()
			// IPv4	返回 IPv4
			// IPv6	返回 nil
			if ipNet.IP.To4() != nil {
				return ipNet.IP.String(), nil
			}
		}
	}

	return "", fmt.Errorf("no valid local IP found")
}
