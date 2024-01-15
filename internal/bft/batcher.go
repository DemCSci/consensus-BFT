// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package bft

import (
	"sync"
	"time"
)

// BatchBuilder implements Batcher
type BatchBuilder struct {
	pool          RequestPool
	submittedChan chan struct{}
	maxMsgCount   int
	maxSizeBytes  uint64
	batchTimeout  time.Duration
	closeChan     chan struct{} // 用于接收 关闭通知
	closeLock     sync.Mutex    // Reset and Close may be called by different threads
}

// NewBatchBuilder creates a new BatchBuilder
//
//	@Description:
//	@param pool 请求池
//	@param submittedChan
//	@param maxMsgCount 一个batch中最大消息的数量
//	@param maxSizeBytes 一个batch中总消息的最大大小
//	@param batchTimeout 超时时间
//	@return *BatchBuilder
func NewBatchBuilder(pool RequestPool, submittedChan chan struct{}, maxMsgCount uint64, maxSizeBytes uint64, batchTimeout time.Duration) *BatchBuilder {
	b := &BatchBuilder{
		pool:          pool,
		submittedChan: submittedChan,
		maxMsgCount:   int(maxMsgCount),
		maxSizeBytes:  maxSizeBytes,
		batchTimeout:  batchTimeout,
		closeChan:     make(chan struct{}),
	}
	return b
}

// NextBatch returns the next batch of requests to be proposed.
// The method returns as soon as the batch is full, in terms of request count or total size, or after a timeout.
// The method may block.
// NextBatch返回要提出的下一批请求。该方法在批已满时 (根据请求计数或总大小) 或在超时后立即返回
// 该方法会阻塞
func (b *BatchBuilder) NextBatch() [][]byte {
	currBatch, full := b.pool.NextRequests(b.maxMsgCount, b.maxSizeBytes, true)
	// 如果满足设定条件，直接返回打包
	if full {
		return currBatch
	}
	// 不满足，等待超时，或者等待客户端有新的提交更新batch
	timeout := time.After(b.batchTimeout) // TODO use task-scheduler based on logical time

	for {
		select {
		case <-b.closeChan:
			return nil
		case <-timeout:
			currBatch, _ = b.pool.NextRequests(b.maxMsgCount, b.maxSizeBytes, false)
			return currBatch
		case <-b.submittedChan:
			// there is a possibility to extend the current batch
			currBatch, full = b.pool.NextRequests(b.maxMsgCount, b.maxSizeBytes, true)
			if full {
				return currBatch
			}
		}
	}
}

// Close closes the close channel to stop NextBatch
// 关闭通道以停止NextBatch
func (b *BatchBuilder) Close() {
	b.closeLock.Lock()
	defer b.closeLock.Unlock()
	select {
	case <-b.closeChan:
		return
	default:
	}
	close(b.closeChan)
}

// Closed returns true if the batcher is closed
func (b *BatchBuilder) Closed() bool {
	select {
	case <-b.closeChan:
		return true
	default:
		return false
	}
}

// Reset reopens the close channel to allow calling NextBatch
// 重新打开关闭通道以允许调用NextBatch
func (b *BatchBuilder) Reset() {
	b.closeLock.Lock()
	defer b.closeLock.Unlock()
	b.closeChan = make(chan struct{})
}
