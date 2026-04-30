package consistenthash

import "github.com/cespare/xxhash/v2"

// Config defines the stable parts of the consistent hash ring.
type Config struct {
	DefaultReplicas int
	HashFunc        func(data []byte) uint32
}

// DefaultConfig keeps a fixed number of virtual nodes per real node.
var DefaultConfig = &Config{
	DefaultReplicas: 50,
	HashFunc: func(data []byte) uint32 {
		return uint32(xxhash.Sum64(data))
	},
}
