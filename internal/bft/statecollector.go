// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package bft

import (
	"sync"
	"time"

	"github.com/SmartBFT-Go/consensus/pkg/api"
	"github.com/SmartBFT-Go/consensus/pkg/types"
	protos "github.com/SmartBFT-Go/consensus/smartbftprotos"
)

// StateCollector collects the current state from other nodes
// 从其他节点收集当前状态
type StateCollector struct {
	SelfID uint64
	N      uint64
	f      int
	quorum int

	Logger api.Logger

	incMsgs chan *incMsg

	CollectTimeout time.Duration

	responses *voteSet

	stopOnce sync.Once
	stopChan chan struct{}
}

// Start starts the state collector
func (s *StateCollector) Start() {
	s.incMsgs = make(chan *incMsg, s.N)
	s.quorum, s.f = computeQuorum(s.N)
	s.stopChan = make(chan struct{})
	s.stopOnce = sync.Once{}
	// 接收投票的规则：是StateTransferResponse消息就可以
	acceptResponse := func(_ uint64, message *protos.Message) bool {
		return message.GetStateTransferResponse() != nil
	}
	s.responses = &voteSet{
		validVote: acceptResponse,
	}
	s.responses.clear(s.N)
}

// HandleMessage handle messages addressed to the state collector
// 分发 发往状态收集器的消息
func (s *StateCollector) HandleMessage(sender uint64, m *protos.Message) {
	if m.GetStateTransferResponse() == nil {
		s.Logger.Panicf("Node %d handling a message which is not a response", s.SelfID)
	}
	msg := &incMsg{sender: sender, Message: m}
	s.Logger.Debugf("Node %d handling state response: %v", s.SelfID, msg)
	select {
	case <-s.stopChan:
		return
	case s.incMsgs <- msg:
	default: // if incMsgs is full do nothing
		s.Logger.Debugf("Node %d reached default in handling state response: %v", s.SelfID, msg)
	}
}

// ClearCollected clears the responses collected by the state collector
func (s *StateCollector) ClearCollected() {
	// drain message channel
	for len(s.incMsgs) > 0 {
		<-s.incMsgs
	}
}

// CollectStateResponses return a valid response or nil if reached timeout
// 返回一个有效的响应 如果达到超时 返回nil
func (s *StateCollector) CollectStateResponses() *types.ViewAndSeq {
	s.responses.clear(s.N)
	// 设置超时定时器
	timer := time.NewTimer(s.CollectTimeout)
	defer timer.Stop()
	// 开始收集 state responses
	s.Logger.Debugf("Node %d started collecting state responses", s.SelfID)

	for {
		select {
		case <-s.stopChan:
			return nil
		case <-timer.C:
			//collector 超时了，直接返回nil
			s.Logger.Infof("Node %d reached the state collector timeout", s.SelfID)
			return nil
		case msg := <-s.incMsgs:
			s.Logger.Debugf("Node %d collected a response: %v", s.SelfID, msg)
			s.responses.registerVote(msg.sender, msg.Message)
			if viewAndSeq := s.collectedEnoughEqualVotes(); viewAndSeq != nil {
				s.Logger.Infof("Node %d collected a valid state: view - %d and seq - %d", s.SelfID, viewAndSeq.View, viewAndSeq.Seq)
				return viewAndSeq
			}
		}
	}
}

// 收集到f+1个相等的viewNumber和seq，返回viewNumber和seq，如果没有收集到 返回 false
func (s *StateCollector) collectedEnoughEqualVotes() *types.ViewAndSeq {
	// 收集到 少于=f个 直接返回nil
	if len(s.responses.voted) <= s.f {
		return nil
	}
	votesMap := make(map[types.ViewAndSeq]uint64)
	num := len(s.responses.votes)
	for i := 0; i < num; i++ {
		vote := <-s.responses.votes
		response := vote.GetStateTransferResponse()
		if response == nil {
			s.Logger.Panicf("Node %d collected a message which is not a response", s.SelfID)
			return nil
		}
		viewAndSeq := types.ViewAndSeq{
			View: response.ViewNum,
			Seq:  response.Sequence,
		}
		s.Logger.Debugf("Node %d collected a responses with view - %d and seq - %d", s.SelfID, viewAndSeq.View, viewAndSeq.Seq)
		s.responses.votes <- vote
		votesMap[viewAndSeq]++
	}
	for viewAndSeq, count := range votesMap {
		// 收集到了 f+1个相同的 响应
		if count > uint64(s.f) {
			return &viewAndSeq
		}
	}
	return nil
}

func (s *StateCollector) close() {
	s.stopOnce.Do(
		func() {
			select {
			case <-s.stopChan:
				return
			default:
				close(s.stopChan)
			}
		},
	)
}

// Stop the state collector
func (s *StateCollector) Stop() {
	s.close()
}
