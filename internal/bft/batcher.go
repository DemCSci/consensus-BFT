// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package bft

import (
	"sync"
	"time"
)

type BatchBuilder struct {
	pool         RequestPool
	maxMsgCount  int
	maxSizeBytes uint64
	batchTimeout time.Duration
	closeChan    chan struct{}
	remainder    [][]byte
	closeLock    sync.Mutex // Reset and Close may be called by different threads
}

func NewBatchBuilder(pool RequestPool, maxMsgCount int, maxSizeBytes uint64, batchTimeout time.Duration) *BatchBuilder {
	b := &BatchBuilder{
		pool:         pool,
		maxMsgCount:  maxMsgCount,
		maxSizeBytes: maxSizeBytes,
		batchTimeout: batchTimeout,
		closeChan:    make(chan struct{}),
	}
	return b
}

// NextBatch returns the next batch of requests to be proposed
func (b *BatchBuilder) NextBatch() [][]byte {
	currBatch := make([][]byte, 0)
	remainderCount := len(b.remainder)
	if remainderCount > 0 {
		currBatch = b.remainder
	}
	b.remainder = make([][]byte, 0)
	timeout := time.After(b.batchTimeout)
	for {
		select {
		case <-b.closeChan:
			return nil
		case <-timeout:
			return b.buildBatch(remainderCount, currBatch)
		default:
			if b.pool.Size() >= b.maxMsgCount-remainderCount {
				return b.buildBatch(remainderCount, currBatch)
			}
			time.Sleep(b.batchTimeout / 100)
		}
	}
}

// takes the current batch and appends to it requests from the pool
func (b *BatchBuilder) buildBatch(remainderCount int, currBatch [][]byte) [][]byte {
	reqs, _ := b.pool.NextRequests(b.maxMsgCount-remainderCount, 100*1024*1024)
	for i := 0; i < len(reqs); i++ {
		currBatch = append(currBatch, reqs[i])
	}
	return currBatch
}

// BatchRemainder sets the remainder of requests to be included in the next batch
func (b *BatchBuilder) BatchRemainder(remainder [][]byte) {
	if len(b.remainder) != 0 {
		panic("batch remainder should always be empty when setting remainder")
	}
	b.remainder = remainder
}

// Close closes the close channel to stop NextBatch
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

func (b *BatchBuilder) Closed() bool {
	select {
	case <-b.closeChan:
		return true
	default:
		return false
	}
}

// Reset resets the remainder and reopens the close channel to allow calling NextBatch
func (b *BatchBuilder) Reset() {
	b.closeLock.Lock()
	defer b.closeLock.Unlock()
	b.remainder = nil
	b.closeChan = make(chan struct{})
}

// PopRemainder returns the remainder and resets it
func (b *BatchBuilder) PopRemainder() [][]byte {
	defer func() {
		b.remainder = nil
	}()
	return b.remainder
}
