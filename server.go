package kamacache

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"crypto/tls"

	"github.com/sirupsen/logrus"
	pb "github.com/youngyangyang04/KamaCache-Go/pb"
	"github.com/youngyangyang04/KamaCache-Go/registry"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

type NodeState int

const (
	Active NodeState = iota
	Draining
	Offline
)

func (s NodeState) String() string {
	switch s {
	case Active:
		return "Active"
	case Draining:
		return "Draining"
	case Offline:
		return "Offline"
	default:
		return "Unknown"
	}
}

// Server 定义缓存服务器
type Server struct {
	pb.UnimplementedKamaCacheServer
	addr       string           // 服务地址
	svcName    string           // 服务名称
	groups     *sync.Map        // 缓存组
	grpcServer *grpc.Server     // gRPC服务器
	etcdCli    *clientv3.Client // etcd客户端
	stopCh     chan error       // 停止信号
	opts       *ServerOptions   // 服务器选项
}

// ServerOptions 服务器配置选项
type ServerSnapshot struct {
	Addr    string   `json:"addr"`
	SvcName string   `json:"svc_name"`
	Status  string   `json:"status"`
	Groups  []string `json:"groups"`
}

type ServerOptions struct {
	EtcdEndpoints     []string      // etcd端点
	DialTimeout       time.Duration // 连接超时
	ServerDialTimeout time.Duration //过期时间
	lastAccessTime    atomic.Int64  //最后访问时间
	MaxMsgSize        int           // 最大消息大小
	TLS               bool          // 是否启用TLS
	CertFile          string        // 证书文件
	KeyFile           string        // 密钥文件
	Status            NodeState
}

// DefaultServerOptions 默认配置
var DefaultServerOptions = &ServerOptions{
	EtcdEndpoints:     []string{"localhost:2379"},
	DialTimeout:       5 * time.Second,
	ServerDialTimeout: 5 * time.Minute,
	MaxMsgSize:        4 << 20, // 4MB
	Status:            Active,
}

// ServerOption 定义选项函数类型
type ServerOption func(*ServerOptions)

// WithEtcdEndpoints 设置etcd端点
func WithEtcdEndpoints(endpoints []string) ServerOption {
	return func(o *ServerOptions) {
		o.EtcdEndpoints = endpoints
	}
}

// WithDialTimeout 设置连接超时
func WithDialTimeout(timeout time.Duration) ServerOption {
	return func(o *ServerOptions) {
		o.DialTimeout = timeout
	}
}

// WithTLS 设置TLS配置
func WithTLS(certFile, keyFile string) ServerOption {
	return func(o *ServerOptions) {
		o.TLS = true
		o.CertFile = certFile
		o.KeyFile = keyFile
	}
}

// NewServer 创建新的服务器实例
func NewServer(addr, svcName string, opts ...ServerOption) (*Server, error) {
	options := DefaultServerOptions
	for _, opt := range opts {
		opt(options)
	}

	// 创建etcd客户端
	etcdCli, err := clientv3.New(clientv3.Config{
		Endpoints:   options.EtcdEndpoints,
		DialTimeout: options.DialTimeout,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create etcd client: %v", err)
	}

	// 创建gRPC服务器
	var serverOpts []grpc.ServerOption
	serverOpts = append(serverOpts, grpc.MaxRecvMsgSize(options.MaxMsgSize))

	if options.TLS {
		creds, err := loadTLSCredentials(options.CertFile, options.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("failed to load TLS credentials: %v", err)
		}
		serverOpts = append(serverOpts, grpc.Creds(creds))
	}

	srv := &Server{
		addr:       addr,
		svcName:    svcName,
		groups:     &sync.Map{},
		grpcServer: grpc.NewServer(serverOpts...),
		etcdCli:    etcdCli,
		stopCh:     make(chan error),
		opts:       options,
	}

	// 注册服务,注册 KamaCache gRPC 服务
	// 把 KamaCache 的 RPC 方法注册到 gRPC Server
	// Client ----RPC----> gRPC Server
	//                      │
	//                      ▼
	//                  srv.Get()
	pb.RegisterKamaCacheServer(srv.grpcServer, srv)

	// 注册grpc健康检查服务,创建一个健康状态管理器。
	// 内部维护 map[serviceName]status，如 kama-cache → SERVING
	healthServer := health.NewServer()
	healthpb.RegisterHealthServer(srv.grpcServer, healthServer)
	// 设置当前服务状态status为 SERVING
	healthServer.SetServingStatus(svcName, healthpb.HealthCheckResponse_SERVING)

	return srv, nil
}

// Start 启动服务器
func (s *Server) Start() error {
	// 启动gRPC服务器,在指定地址创建 TCP 监听器
	lis, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("failed to listen: %v", err)
	}

	// 注册到etcd
	// stopCh := make(chan error)
	go func() {
		// 所有节点共享一个 etcd 集群，watch 同一前缀，可以看到其他节点的注册变化。
		if err := registry.Register(s.etcdCli, s.svcName, s.addr, s.stopCh); err != nil {
			logrus.Errorf("failed to register service: %v", err)
			close(s.stopCh)
			return
		}
	}()

	logrus.Infof("Server starting at %s", s.addr)
	// 启动 gRPC 服务器并开始接收客户端rpc请求
	// gRPC 会进入 无限循环：
	//for {
	//     conn := lis.Accept()
	//     go handleConnection(conn)
	// }
	// Serve() 是阻塞的,只有三种情况会关闭：1 server.Stop() 2 listener 关闭 3 程序崩溃
	return s.grpcServer.Serve(lis)
}

// Stop 停止服务器
func (s *Server) Stop() {
	s.opts.Status = Offline

	s.grpcServer.GracefulStop()
	if s.etcdCli != nil {
		s.etcdCli.Close()
	}
}

func (s *Server) ObjectStop() {
	close(s.stopCh)
}

func (s *Server) GracefulStop() {
	s.opts.Status = Draining
	go s.waitForIdleThenShutdown()
}

func (s *Server) waitForIdleThenShutdown() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {

		select {

		case <-ticker.C:

			last := time.Unix(0,
				s.opts.lastAccessTime.Load())

			if time.Since(last) > s.opts.ServerDialTimeout {

				logrus.Infof(
					"No access for %v, shutting down...",
					s.opts.ServerDialTimeout)

				s.Stop()

				return
			}
		}
	}
}

func (s *Server) GetStatus() NodeState {
	return s.opts.Status
}

func (s *Server) Snapshot() ServerSnapshot {
	return ServerSnapshot{
		Addr:    s.addr,
		SvcName: s.svcName,
		Status:  s.GetStatus().String(),
		Groups:  ListGroups(),
	}
}

// Get 实现Cache服务的Get方法
func (s *Server) Get(ctx context.Context, req *pb.Request) (*pb.ResponseForGet, error) {
	group := GetGroup(req.Group)
	if group == nil {
		return nil, fmt.Errorf("group %s not found", req.Group)
	}

	view, err := group.Get(ctx, req.Key)
	if err != nil {
		return nil, err
	}

	return &pb.ResponseForGet{Value: view.ByteSLice()}, nil
}

func (s *Server) GetLocal(ctx context.Context, req *pb.Request) (*pb.ResponseForGet, error) {
	group := GetGroup(req.Group)
	if group == nil {
		return nil, fmt.Errorf("group %s not found", req.Group)
	}

	view, err := group.GetLocal(ctx, req.Key)
	if err != nil {
		return nil, err
	}

	return &pb.ResponseForGet{Value: view.ByteSLice()}, nil
}

// Set 实现Cache服务的Set方法
func (s *Server) Set(ctx context.Context, req *pb.Request) (*pb.ResponseForGet, error) {
	if s.opts.Status == Active {
		group := GetGroup(req.Group)
		if group == nil {
			return nil, fmt.Errorf("group %s not found", req.Group)
		}

		// 从 context 中获取标记，如果没有则创建新的 context
		// 如果没有 from_peer,说明是客户端请求,给它打一个 peer 标记
		// Server 是 peer 之间调用的入口。
		// NodeA → RPC → NodeB
		// NodeB 的 Server.Set() 被调用。
		// 如果 NodeB 再往其他节点同步，就可能出现循环。
		// 所以在 RPC入口就打标记。
		// fromPeer := ctx.Value("from_peer")
		// if fromPeer == nil {
		// 	ctx = context.WithValue(ctx, "from_peer", true)
		// }

		if err := group.Set(ctx, req.Key, req.Value); err != nil {
			return nil, err
		}

		return &pb.ResponseForGet{Value: req.Value}, nil
	}
	return nil, fmt.Errorf("status is not active")
}

func (s *Server) SetLocal(ctx context.Context, req *pb.Request) (*pb.ResponseForGet, error) {
	if s.opts.Status != Active {
		return nil, fmt.Errorf("status is not active")
	}

	group := GetGroup(req.Group)
	if group == nil {
		return nil, fmt.Errorf("group %s not found", req.Group)
	}

	if err := group.SetLocal(ctx, req.Key, req.Value); err != nil {
		return nil, err
	}

	return &pb.ResponseForGet{Value: req.Value}, nil
}

// Delete 实现Cache服务的Delete方法
func (s *Server) Delete(ctx context.Context, req *pb.Request) (*pb.ResponseForDelete, error) {
	group := GetGroup(req.Group)
	if group == nil {
		return nil, fmt.Errorf("group %s not found", req.Group)
	}

	err := group.Delete(ctx, req.Key)
	return &pb.ResponseForDelete{Value: err == nil}, err
}

// loadTLSCredentials 加载TLS证书
func loadTLSCredentials(certFile, keyFile string) (credentials.TransportCredentials, error) {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, err
	}
	return credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{cert},
	}), nil
}
