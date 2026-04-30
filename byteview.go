package kamacache

// ByteView 只读的字节视图，用于缓存数据，防止缓存数据被外部修改
type ByteView struct {
	// 这个字段不会直接暴露，防止直接被外界修改
	b []byte
}

func (b ByteView) Len() int {
	return len(b.b)
}

func (b ByteView) ByteSLice() []byte {
	return cloneBytes(b.b)
}

func (b ByteView) String() string {
	return string(b.b)
}

// 深拷贝
func cloneBytes(b []byte) []byte {
	c := make([]byte, len(b))
	copy(c, b)
	return c
}
