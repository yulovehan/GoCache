package kamacache

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"
	pb "github.com/youngyangyang04/KamaCache-Go/pb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Client struct {
	addr    string // 远程节点地址，表示 当前 Client 要连接的缓存节点地址。如 192.168.71.130:8001
	svcName string // 服务在 etcd 中注册的名字。
	// etcdCli *clientv3.Client   // 连接 etcd 的客户端对象。
	conn    *grpc.ClientConn   // gRPC 网络连接
	grpcCli pb.KamaCacheClient // gRPC 生成的客户端接口。
	status  atomic.Int32
}

// 编译期检查 Client 是否实现 Peer 接口
// *Client 实现了接口，把 nil 转换成 *Client 类型，(*Client)是指针类型转换，所以加()，interface类型转换用断言
var _ Peer = (*Client)(nil)

func NewClient(addr string, svcName string, etcdCli *clientv3.Client) (*Client, error) {
	var err error
	if etcdCli == nil {
		etcdCli, err = clientv3.New(clientv3.Config{
			Endpoints:   []string{"localhost:2379"},
			DialTimeout: 5 * time.Second,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to create etcd client: %v", err)
		}
	}
	// grpc.Dial(...): 建立到 gRPC 服务器的客户端连接
	conn, err := grpc.Dial(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()), //设置传输安全方式,我们这里是不使用 TLS，也就是明文传输
		grpc.WithBlock(),                                     // Dial 会阻塞直到连接成功
		grpc.WithTimeout(10*time.Second),                     // 连接超时，如果 10 秒内没有连上服务器就报错
		grpc.WithDefaultCallOptions(grpc.WaitForReady(true)), // 设置 RPC 调用默认选项，这里是如果服务器暂时不可用，RPC 会等待连接恢复，而不是直接返回错误
	)
	if err != nil {
		return nil, fmt.Errorf("failed to dial server: %v", err)
	}

	grpcClient := pb.NewKamaCacheClient(conn)

	client := &Client{
		addr:    addr,
		svcName: svcName,
		// etcdCli: etcdCli,
		conn:    conn,
		grpcCli: grpcClient,
	}

	return client, nil
}

func (c *Client) GracefulStop() {
	c.status.Store(int32(Draining))

	go func() {
		if err := c.Close(); err != nil {
			logrus.Warnf("failed to close client connection for %s: %v", c.addr, err)
		}

		c.status.Store(int32(Offline))
	}()
}

func (c *Client) GetStatus() NodeState {
	return NodeState(c.status.Load())
}

// 由group.go下的get_from_peer()调用
func (c *Client) Get(group, key string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// 通过 gRPC 调用远程缓存服务的 Get 方法，Get 方法通过网络（TCP/IP）调用 远程 gRPC 服务端 的 Get 方法
	// &pb.Request{}构造 gRPC 消息
	resp, err := c.grpcCli.Get(ctx, &pb.Request{
		Group: group,
		Key:   key,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get value from kamacache: %v", err)
	}
	// resp.GetValue() → 从 gRPC 响应中获取缓存值
	// resp 可能为 nil（网络错误或服务返回异常）
	// 如果直接写 resp.Value → nil 指针访问会 panic
	// resp.GetValue() 会帮你安全处理 nil，返回 nil 而不是 panic
	return resp.GetValue(), nil
}

func (c *Client) GetLocal(group, key string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 800*time.Millisecond)
	defer cancel()

	resp, err := c.grpcCli.GetLocal(ctx, &pb.Request{
		Group: group,
		Key:   key,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get local value from kamacache: %v", err)
	}
	return resp.GetValue(), nil
}

func (c *Client) Delete(group, key string) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	resp, err := c.grpcCli.Delete(ctx, &pb.Request{
		Group: group,
		Key:   key,
	})
	if err != nil {
		return false, fmt.Errorf("failed to delete value from kamacache: %v", err)
	}
	// resp.GetValue() 是 gRPC / Protobuf 生成代码里的一个 getter 方法，作用是：
	// 从 gRPC 返回的响应 resp 中获取 Value 字段的值。
	return resp.GetValue(), nil
}

func (c *Client) Set(ctx context.Context, group, key string, value []byte) error {
	// resp 服务端返回的响应数据
	resp, err := c.grpcCli.Set(ctx, &pb.Request{
		Group: group,
		Key:   key,
		Value: value,
	})
	if err != nil {
		return fmt.Errorf("failed to set value to kamacache: %v", err)
	}
	// %+v 输出结构体字段名 + 值
	logrus.Infof("grpc set request resp: %+v", resp)

	return nil
}

func (c *Client) SetLocal(ctx context.Context, group, key string, value []byte) error {
	resp, err := c.grpcCli.SetLocal(ctx, &pb.Request{
		Group: group,
		Key:   key,
		Value: value,
	})
	if err != nil {
		return fmt.Errorf("failed to set local value to kamacache: %v", err)
	}
	logrus.Debugf("grpc set local request resp: %+v", resp)

	return nil
}

func (c *Client) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}
