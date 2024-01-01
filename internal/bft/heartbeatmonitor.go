// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package bft

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/SmartBFT-Go/consensus/pkg/api"
	"github.com/SmartBFT-Go/consensus/smartbftprotos"
)

// 心跳运行机制
// leader 给 follower 发送heartbeat消息
// follower 如果检测到 leader发来的心跳消息view 小于当前监视器的view，会回复response消息，携带监视器的view
// leader 如果收到f+1的response消息，那么会触发同步。

// A node could either be a leader or a follower
// 节点的角色 要么是Leader 要么是Follower
const (
	Leader   Role = false
	Follower Role = true
)

//go:generate mockery -dir . -name HeartbeatEventHandler -case underscore -output ./mocks/

// HeartbeatEventHandler defines who to call when a heartbeat timeout expires or a Sync needs to be triggered.
// This is implemented by the Controller.
// HeartbeatEventHandler 定义当心跳超时到期或需要触发同步时调用谁，
// 废话一堆，就是超时/同步处理器
type HeartbeatEventHandler interface {
	// OnHeartbeatTimeout is called when a heartbeat timeout expires.
	// 当心跳超时时被调用
	OnHeartbeatTimeout(view uint64, leaderID uint64)
	// Sync is called when enough heartbeat responses report that the current leader's view is outdated.
	// 当有足够的心跳响应报告当前领导者的视图已过时时，将调用Sync
	Sync()
}

// Role indicates if this node is a follower or a leader
type Role bool

// 这是一条命令
type roleChange struct {
	view                            uint64
	leaderID                        uint64
	follower                        Role
	onlyStopSendHeartbearFromLeader bool // 表示leader仅停止发送心跳消息，不改变view
}

// heartbeatResponseCollector is a map from node ID to view number, and hold the last response from each node.
// key ：nodeID
// value: view number 该nodeID 最新发送过来心跳响应的view
type heartbeatResponseCollector map[uint64]uint64

// HeartbeatMonitor implements LeaderMonitor
type HeartbeatMonitor struct {
	scheduler                     <-chan time.Time // 每隔一段时间 向 scheduler 发送时间
	inc                           chan incMsg
	stopChan                      chan struct{}
	commandChan                   chan roleChange // 命令管道
	logger                        api.Logger
	hbTimeout                     time.Duration // 间隔 这么长时间 leader 发送心跳
	hbCount                       uint64        // 时间单位
	comm                          Comm
	numberOfNodes                 uint64
	handler                       HeartbeatEventHandler
	view                          uint64
	leaderID                      uint64
	follower                      Role
	stopSendHeartbearFromLeader   bool
	lastHeartbeat                 time.Time // 上一次发送心跳的时间
	lastTick                      time.Time
	hbRespCollector               heartbeatResponseCollector // map[nodeId]view
	running                       sync.WaitGroup             // 运行状态
	runOnce                       sync.Once
	timedOut                      bool // leader 发送来的消息是否超时
	syncReq                       bool
	viewSequences                 *atomic.Value // view 中的 seq
	sentHeartbeat                 chan struct{}
	artificialHeartbeat           chan incMsg
	behindSeq                     uint64
	behindCounter                 uint64
	numOfTicksBehindBeforeSyncing uint64
	followerBehind                bool // 如果当前身份是follower ，表示当前的proposal seq 是否滞后
}

// NewHeartbeatMonitor creates a new HeartbeatMonitor
// 创建心跳监视器
func NewHeartbeatMonitor(scheduler <-chan time.Time, logger api.Logger, heartbeatTimeout time.Duration, heartbeatCount uint64, comm Comm, numberOfNodes uint64, handler HeartbeatEventHandler, viewSequences *atomic.Value, numOfTicksBehindBeforeSyncing uint64) *HeartbeatMonitor {
	hm := &HeartbeatMonitor{
		stopChan:                      make(chan struct{}),
		inc:                           make(chan incMsg),
		commandChan:                   make(chan roleChange),
		scheduler:                     scheduler,
		logger:                        logger,
		hbTimeout:                     heartbeatTimeout,
		hbCount:                       heartbeatCount,
		comm:                          comm,
		numberOfNodes:                 numberOfNodes,
		handler:                       handler,
		hbRespCollector:               make(heartbeatResponseCollector),
		viewSequences:                 viewSequences,
		sentHeartbeat:                 make(chan struct{}, 1),
		artificialHeartbeat:           make(chan incMsg, 1),
		numOfTicksBehindBeforeSyncing: numOfTicksBehindBeforeSyncing,
	}
	return hm
}

func (hm *HeartbeatMonitor) start() {
	hm.running.Add(1)
	go hm.run()
}

// Close stops following or sending heartbeats.
func (hm *HeartbeatMonitor) Close() {
	if hm.closed() {
		return
	}
	defer func() {
		hm.lastHeartbeat = time.Time{}
		hm.lastTick = time.Time{}
	}()
	defer hm.running.Wait()
	close(hm.stopChan)
}

func (hm *HeartbeatMonitor) run() {
	defer hm.running.Done()
	for {
		select {
		case <-hm.stopChan:
			return
		case now := <-hm.scheduler:
			// leader 给follower 发送心跳消息
			hm.tick(now)
		case msg := <-hm.inc:
			hm.handleMsg(msg.sender, msg.Message)
		case cmd := <-hm.commandChan:
			hm.handleCommand(cmd)
		case <-hm.sentHeartbeat:
			hm.lastHeartbeat = hm.lastTick
		case msg := <-hm.artificialHeartbeat:
			hm.handleArtificialHeartBeat(msg.sender, msg.GetHeartBeat())
		}
	}
}

// ProcessMsg handles an incoming heartbeat or heartbeat-response.
// If the sender and msg.View equal what we expect, and the timeout had not expired yet, the timeout is extended.
// 处理传入的心跳或心跳响应 封装消息，发送到了 hm.inc 管道
// 如果sender和msg.View等于我们所期望的，并且超时尚未到期，则超时被延长。
func (hm *HeartbeatMonitor) ProcessMsg(sender uint64, msg *smartbftprotos.Message) {
	select {
	case hm.inc <- incMsg{
		sender:  sender,
		Message: msg,
	}:
	case <-hm.stopChan:
	}
}

// InjectArtificialHeartbeat injects an artificial heartbeat to the monitor
// 注入人造心跳消息
func (hm *HeartbeatMonitor) InjectArtificialHeartbeat(sender uint64, msg *smartbftprotos.Message) {
	select {
	case hm.artificialHeartbeat <- incMsg{
		sender:  sender,
		Message: msg,
	}:
	default:
	}
}

// 向hm.commandChan 发送 了一条消息
// 在不更改当前视图和当前领导者的情况下将角色更改为follower
func (hm *HeartbeatMonitor) StopLeaderSendMsg() {
	// 在不更改当前视图和当前领导者的情况下将角色更改为follower
	hm.logger.Infof("Changing role to folower without change current view and current leader")
	select {
	case hm.commandChan <- roleChange{
		onlyStopSendHeartbearFromLeader: true,
	}:
	case <-hm.stopChan:
		return
	}
}

// ChangeRole will change the role of this HeartbeatMonitor
// 更改此HeartbeatMonitor的角色
//
//	@Description:
//	@receiver hm
//	@param follower 更改为
//	@param view  更改为
//	@param leaderID 更改为
func (hm *HeartbeatMonitor) ChangeRole(follower Role, view uint64, leaderID uint64) {
	hm.runOnce.Do(func() {
		hm.follower = follower
		hm.start()
	})

	role := "leader"
	if follower {
		role = "follower"
	}

	hm.logger.Infof("Changing to %s role, current view: %d, current leader: %d", role, view, leaderID)
	select {
	case hm.commandChan <- roleChange{
		leaderID: leaderID,
		view:     view,
		follower: follower,
	}:
	case <-hm.stopChan:
		return
	}
}

// 处理发送者发来的消息，分类器，不同的消息，分发到不同的方法
func (hm *HeartbeatMonitor) handleMsg(sender uint64, msg *smartbftprotos.Message) {
	switch msg.GetContent().(type) {
	case *smartbftprotos.Message_HeartBeat:
		hm.handleRealHeartBeat(sender, msg.GetHeartBeat())
	case *smartbftprotos.Message_HeartBeatResponse:
		hm.handleHeartBeatResponse(sender, msg.GetHeartBeatResponse())
	default:
		hm.logger.Warnf("Unexpected message type, ignoring")
	}
}

// 处理真正的心跳消息
func (hm *HeartbeatMonitor) handleRealHeartBeat(sender uint64, hb *smartbftprotos.HeartBeat) {
	hm.handleHeartBeat(sender, hb, false)
}

// 处理人造心跳
func (hm *HeartbeatMonitor) handleArtificialHeartBeat(sender uint64, hb *smartbftprotos.HeartBeat) {
	hm.handleHeartBeat(sender, hb, true)
}

// handleHeartBeat
//
//	@Description:
//	@receiver hm
//	@param sender 发送者
//	@param hb 心跳消息
//	@param artificial true 表示是人造的心跳，false 表示真实的消息
func (hm *HeartbeatMonitor) handleHeartBeat(sender uint64, hb *smartbftprotos.HeartBeat, artificial bool) {
	// leader 发来的心跳 小于当前HeartbeatMonitor view 需要给leader 发送response消息
	// 只有这个地方 给 发送者 回复消息，下面的处理 都没有发
	if hb.View < hm.view {
		hm.logger.Debugf("Heartbeat view is lower than expected, sending response; expected-view=%d, received-view: %d", hm.view, hb.View)
		hm.sendHeartBeatResponse(sender)
		return
	}
	// 判断心跳的发送者是不是leader ，如果不是leader 直接忽略
	if !hm.stopSendHeartbearFromLeader && sender != hm.leaderID {
		hm.logger.Debugf("Heartbeat sender is not leader, ignoring; leader: %d, sender: %d", hm.leaderID, sender)
		return
	}
	// 收到消息的view 大于 监视器的view，会触发本节点同步
	if hb.View > hm.view {
		hm.logger.Debugf("Heartbeat view is bigger than expected, syncing and ignoring; expected-view=%d, received-view: %d", hm.view, hb.View)
		hm.handler.Sync()
		return
	}

	active, ourSeq := hm.viewActive(hb)
	// 激活状态 并且 不是人造的
	if active && !artificial {
		if ourSeq+1 < hb.Seq {
			// 心跳序列大于预期，领导者的序列为 % d，我们的序列为 % d，本节点调用同步   并不给发送者回复消息
			hm.logger.Debugf("Heartbeat sequence is bigger than expected, leader's sequence is %d and ours is %d, syncing and ignoring", hb.Seq, ourSeq)
			hm.handler.Sync()
			return
		}
		if ourSeq+1 == hb.Seq {
			hm.followerBehind = true
			// 我们的序列在心跳序列后面，领导者的序列是 % d，我们的是 % d
			hm.logger.Debugf("Our sequence is behind the heartbeat sequence, leader's sequence is %d and ours is %d", hb.Seq, ourSeq)
			if ourSeq > hm.behindSeq {
				hm.behindSeq = ourSeq
				hm.behindCounter = 0
			}
		} else {
			hm.followerBehind = false
		}
	} else {
		hm.followerBehind = false
	}

	hm.logger.Debugf("Received heartbeat from %d, last heartbeat was %v ago", sender, hm.lastTick.Sub(hm.lastHeartbeat))
	hm.lastHeartbeat = hm.lastTick
}

// handleHeartBeatResponse keeps track of responses, and if we get f+1 identical, force a sync
// 处理心跳响应消息  跟踪响应，如果我们得到相同的f+1条，强制同步
func (hm *HeartbeatMonitor) handleHeartBeatResponse(sender uint64, hbr *smartbftprotos.HeartBeatResponse) {
	// 如果是follower 角色，不处理响应消息
	if hm.follower {
		hm.logger.Debugf("Monitor is not a leader, ignoring HeartBeatResponse; sender: %d, msg: %v", sender, hbr)
		return
	}
	// 能来到这里，那么说明一定是leader角色了

	// 表示正在处理同步状态了，也不用处理消息
	if hm.syncReq {
		hm.logger.Debugf("Monitor already called Sync, ignoring HeartBeatResponse; sender: %d, msg: %v", sender, hbr)
		return
	}
	// 心跳响应的view 比 我的还小，直接忽略
	if hm.view >= hbr.View {
		hm.logger.Debugf("Monitor view: %d >= HeartBeatResponse, ignoring; sender: %d, msg: %v", hm.view, sender, hbr)
		return
	}

	hm.logger.Debugf("Received HeartBeatResponse, msg: %v; from %d", hbr, sender)
	// 记录到map中 发送者 发送者的vew
	hm.hbRespCollector[sender] = hbr.View

	// check if we have f+1 votes
	// 收到 F + 1 个心跳响应 触发同步？？为啥 leader收到心跳响应了 还要同步
	_, f := computeQuorum(hm.numberOfNodes)
	if len(hm.hbRespCollector) >= f+1 {
		hm.logger.Infof("Received HeartBeatResponse triggered a call to HeartBeatEventHandler Sync, view: %d", hbr.View)
		hm.handler.Sync()
		hm.syncReq = true
	}
}

// sendHeartBeatResponse
//
//	@Description: 响应体中 是 HeartbeatMonitor 中持有的view 编号
//	@receiver hm
//	@param target 接收人
func (hm *HeartbeatMonitor) sendHeartBeatResponse(target uint64) {
	heartbeatResponse := &smartbftprotos.Message{
		Content: &smartbftprotos.Message_HeartBeatResponse{
			HeartBeatResponse: &smartbftprotos.HeartBeatResponse{
				View: hm.view,
			},
		},
	}
	hm.comm.SendConsensus(target, heartbeatResponse)
	hm.logger.Debugf("Sent HeartBeatResponse view: %d; to %d", hm.view, target)
}

// 判断 hm的视图是否激活，如果激活 返回 true, proposal seq
func (hm *HeartbeatMonitor) viewActive(hbMsg *smartbftprotos.HeartBeat) (bool, uint64) {
	vs := hm.viewSequences.Load()
	// View isn't initialized 视图未初始化
	if vs == nil {
		return false, 0
	}

	viewSeq := vs.(ViewSequence)
	if !viewSeq.ViewActive {
		return false, 0
	}

	return true, viewSeq.ProposalSeq
}

func (hm *HeartbeatMonitor) tick(now time.Time) {
	hm.lastTick = now // 更新上次tick的时间
	if hm.lastHeartbeat.IsZero() {
		hm.lastHeartbeat = now
	}
	// 当前节点如果是follower 或者
	if bool(hm.follower) || hm.stopSendHeartbearFromLeader {
		hm.followerTick(now)
	} else {
		hm.leaderTick(now)
	}
}

func (hm *HeartbeatMonitor) closed() bool {
	select {
	case <-hm.stopChan:
		return true
	default:
		return false
	}
}

// 处理角色更改命令等
func (hm *HeartbeatMonitor) handleCommand(cmd roleChange) {
	// leader 仅停止发送心跳消息
	if cmd.onlyStopSendHeartbearFromLeader {
		hm.stopSendHeartbearFromLeader = true
		return
	}

	hm.stopSendHeartbearFromLeader = false
	hm.view = cmd.view
	hm.leaderID = cmd.leaderID
	hm.follower = cmd.follower
	hm.timedOut = false
	hm.lastHeartbeat = hm.lastTick
	// 重新清空了 响应收集
	hm.hbRespCollector = make(heartbeatResponseCollector)
	hm.syncReq = false
}

// 这个方法里面 leader 给follower 发送 心跳消息
func (hm *HeartbeatMonitor) leaderTick(now time.Time) {
	// 判断上次发送心跳的时间 与 本次的时间 是否 满足事件
	if now.Sub(hm.lastHeartbeat)*time.Duration(hm.hbCount) < hm.hbTimeout {
		return
	}
	// 取出 当前 view 的 seq
	var sequence uint64
	vs := hm.viewSequences.Load()
	if vs != nil && vs.(ViewSequence).ViewActive {
		sequence = vs.(ViewSequence).ProposalSeq
	} else {
		hm.logger.Infof("ViewSequence uninitialized or view inactive")
		return
	}
	hm.logger.Debugf("Sending heartbeat with view %d, sequence %d", hm.view, sequence)
	heartbeat := &smartbftprotos.Message{
		Content: &smartbftprotos.Message_HeartBeat{
			HeartBeat: &smartbftprotos.HeartBeat{
				View: hm.view,
				Seq:  sequence,
			},
		},
	}
	hm.comm.BroadcastConsensus(heartbeat)
	hm.lastHeartbeat = now
}

// follower的心跳控制
func (hm *HeartbeatMonitor) followerTick(now time.Time) {
	if hm.timedOut || hm.lastHeartbeat.IsZero() {
		hm.lastHeartbeat = now
		return
	}

	delta := now.Sub(hm.lastHeartbeat)
	// leader上次发过来的心跳和本次心跳的时间间隔大于超时时间了
	if delta >= hm.hbTimeout {
		hm.logger.Warnf("Heartbeat timeout (%v) from %d expired; last heartbeat was observed %s ago",
			hm.hbTimeout, hm.leaderID, delta)
		// 调用超时处理方法
		hm.handler.OnHeartbeatTimeout(hm.view, hm.leaderID)
		hm.timedOut = true
		return
	}

	hm.logger.Debugf("Last heartbeat from %d was %v ago", hm.leaderID, delta)

	if !hm.followerBehind {
		return
	}
	// 能来到这里，说明一定 followerBehind = true
	hm.behindCounter++
	// 滞后的数量 到达阈值，调用同步方法
	if hm.behindCounter >= hm.numOfTicksBehindBeforeSyncing {
		// 同步，因为具有seq % d的follower 在最后 % d个滴答中落后于leader
		hm.logger.Warnf("Syncing since the follower with seq %d is behind the leader for the last %d ticks", hm.behindSeq, hm.numOfTicksBehindBeforeSyncing)
		hm.handler.Sync()
		hm.behindCounter = 0
		return
	}
}

// HeartbeatWasSent tells the monitor to skip sending a heartbeat
// 告诉监视器跳过发送心跳
func (hm *HeartbeatMonitor) HeartbeatWasSent() {
	select {
	case hm.sentHeartbeat <- struct{}{}:
	default:
	}
}
