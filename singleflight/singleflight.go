package singleflight

import (
	"sync"
)

// 代表正在进行或已结束的请求
type call struct {
	// 用于等待 fn 执行完成
	wg sync.WaitGroup
	// 保存fn返回结果
	val interface{}
	// 保存fn返回错误
	err error
}

// Group manages all kinds of calls
type Group struct {
	m sync.Map // 使用sync.Map来优化并发性能
}

// Do 针对相同的key，保证多次调用Do()，都只会调用一次fn
func (g *Group) Do(key string, fn func() (interface{}, error)) (interface{}, error) {
	// Check if there is already an ongoing call for this key
	if existing, ok := g.m.Load(key); ok {
		c := existing.(*call)
		c.wg.Wait()         // Wait for the existing request to finish
		return c.val, c.err // Return the result from the ongoing call
	}

	// If no ongoing request, create a new one
	c := &call{}
	// 给 WaitGroup 增加 1 个任务。
	c.wg.Add(1)
	g.m.Store(key, c) // Store the call in the map

	// Execute the function and set the result
	c.val, c.err = fn()
	// 任务完成，WaitGroup counter -1，所有等待的 goroutine：c.wg.Wait()都会被唤醒
	c.wg.Done() // Mark the request as done

	// After the request is done, clean up the map
	g.m.Delete(key)

	return c.val, c.err
}
