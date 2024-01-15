// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package bft

import (
	"bytes"
	"crypto/sha256"
	"encoding/asn1"
	"encoding/base64"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/SmartBFT-Go/consensus/pkg/api"
	"github.com/SmartBFT-Go/consensus/pkg/types"
	protos "github.com/SmartBFT-Go/consensus/smartbftprotos"
	"github.com/golang/protobuf/proto"
)

type proposalInfo struct {
	digest string
	view   uint64
	seq    uint64
}

// 提取出 viewNumber
func viewNumber(m *protos.Message) uint64 {
	if pp := m.GetPrePrepare(); pp != nil {
		return pp.GetView()
	}

	if prp := m.GetPrepare(); prp != nil {
		return prp.GetView()
	}

	if cmt := m.GetCommit(); cmt != nil {
		return cmt.GetView()
	}

	return math.MaxUint64
}

// 提取出 view中的 seq
func proposalSequence(m *protos.Message) uint64 {
	if pp := m.GetPrePrepare(); pp != nil {
		return pp.Seq
	}

	if prp := m.GetPrepare(); prp != nil {
		return prp.Seq
	}

	if cmt := m.GetCommit(); cmt != nil {
		return cmt.Seq
	}

	return math.MaxUint64
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// MarshalOrPanic marshals or panics when an error occurs
func MarshalOrPanic(msg proto.Message) []byte {
	b, err := proto.Marshal(msg)
	if err != nil {
		panic(err)
	}
	return b
}

func getLeaderID(
	view uint64, // 视图编号
	n uint64, // 节点总数量
	nodes []uint64, // 节点id
	leaderRotation bool, // leader 是否轮换
	decisionsInView uint64, // 当前视图中 已经决定的决策数量
	decisionsPerLeader uint64, // 每个leader 应该决策多少
	blacklist []uint64,
) uint64 {
	blackListed := make(map[uint64]struct{})
	for _, i := range blacklist {
		blackListed[i] = struct{}{}
	}
	// 没有开启leader 轮换
	if !leaderRotation {
		return nodes[view%n]
	}
	for i := 0; i < len(nodes); i++ {
		index := (view + (decisionsInView / decisionsPerLeader)) + uint64(i)
		node := nodes[int(index)%len(nodes)]
		_, exists := blackListed[node]
		if !exists {
			return node
		}
	}

	panic(fmt.Sprintf("all %d nodes are blacklisted", len(nodes)))
}

type vote struct {
	*protos.Message
	sender uint64
}

type voteSet struct {
	// 验证投票是否有效
	validVote func(voter uint64, message *protos.Message) bool
	voted     map[uint64]struct{} // 用来标识 某个节点是否已经投票
	votes     chan *vote
}

// 清空投票集合，n表示重新开的投票集合大小
func (vs *voteSet) clear(n uint64) {
	// Drain the votes channel
	for len(vs.votes) > 0 {
		<-vs.votes
	}

	vs.voted = make(map[uint64]struct{}, n)
	vs.votes = make(chan *vote, n)
}

// 验证 投票是否有效，并保存到投票集合中 向 channel 发送了消息
func (vs *voteSet) registerVote(voter uint64, message *protos.Message) {
	if !vs.validVote(voter, message) {
		return
	}

	_, hasVoted := vs.voted[voter]
	if hasVoted {
		// Received double vote
		return
	}

	vs.voted[voter] = struct{}{}
	vs.votes <- &vote{Message: message, sender: voter}
}

type nextViews struct {
	n map[uint64]uint64
}

func (nv *nextViews) clear() {
	nv.n = make(map[uint64]uint64)
}

func (nv *nextViews) registerNext(next uint64, sender uint64) {
	if next <= nv.n[sender] {
		return
	}

	nv.n[sender] = next
}

// 判断sender 发送的 view编号为next的 viewChange 消息是否已经收到
func (nv *nextViews) sendRecv(next uint64, sender uint64) bool {
	return next == nv.n[sender]
}

type incMsg struct {
	*protos.Message
	sender uint64
}

// computeQuorum calculates the quorums size Q, given a cluster size N.
//
// The calculation satisfies the following:
// Given a cluster size of N nodes, which tolerates f failures according to:
//
//	f = argmax ( N >= 3f+1 )
//
// Q is the size of the quorum such that:
//
//	any two subsets q1, q2 of size Q, intersect in at least f+1 nodes.
//
// Note that this is different from N-f (the number of correct nodes), when N=3f+3. That is, we have two extra nodes
// above the minimum required to tolerate f failures.
func computeQuorum(n uint64) (q int, f int) {
	f = (int(n) - 1) / 3
	q = int(math.Ceil((float64(n) + float64(f) + 1) / 2.0))
	return
}

// InFlightData records proposals that are in-flight,
// as well as their corresponding prepares.
// 记录正在进行中的提案及其相应的准备工作。
type InFlightData struct {
	lock sync.RWMutex
	v    *inFlightProposalData
}

type inFlightProposalData struct {
	proposal *types.Proposal
	prepared bool
}

// InFlightProposal returns an in-flight proposal or nil if there is no such.
// 返回一个正在进行的提案，如果没有，则返回 nil。
func (ifp *InFlightData) InFlightProposal() *types.Proposal {
	ifp.lock.RLock()
	defer ifp.lock.RUnlock()

	if ifp.v == nil {
		return nil
	}

	return ifp.v.proposal
}

// IsInFlightPrepared returns true if the in-flight proposal is prepared.
// 如果正在进行的提案是否是prepared状态，则返回 true
func (ifp *InFlightData) IsInFlightPrepared() bool {
	ifp.lock.RLock()
	defer ifp.lock.RUnlock()

	if ifp.v == nil {
		return false
	}

	return ifp.v.prepared
}

// StoreProposal stores an in-flight proposal.
func (ifp *InFlightData) StoreProposal(prop types.Proposal) {
	p := prop

	ifp.lock.Lock()
	defer ifp.lock.Unlock()

	ifp.v = &inFlightProposalData{proposal: &p}
}

// StorePrepares stores alongside the already stored in-flight proposal that it is prepared.
func (ifp *InFlightData) StorePrepares(view, seq uint64) {
	prop := ifp.InFlightProposal()
	if prop == nil {
		panic("stored prepares but proposal is not initialized")
	}
	p := prop

	ifp.lock.Lock()
	defer ifp.lock.Unlock()

	ifp.v = &inFlightProposalData{proposal: p, prepared: true}
}

func (ifp *InFlightData) clear() {
	ifp.lock.Lock()
	defer ifp.lock.Unlock()

	ifp.v = nil
}

// ProposalMaker implements ProposerBuilder
// ProposerBuilder 实现者
type ProposalMaker struct {
	DecisionsPerLeader uint64
	N                  uint64
	SelfID             uint64
	Decider            Decider
	FailureDetector    FailureDetector
	Sync               Synchronizer
	Logger             api.Logger
	MetricsBlacklist   *api.MetricsBlacklist
	MetricsView        *api.MetricsView
	Comm               Comm
	Verifier           api.Verifier
	Signer             api.Signer
	MembershipNotifier api.MembershipNotifier
	State              State
	InMsqQSize         int
	ViewSequences      *atomic.Value
	restoreOnceFromWAL sync.Once
	Checkpoint         *types.Checkpoint
}

// NewProposer returns a new view
// 返回一个新视图 Proposer 是view 的接口
//
//	@Description:
//	@receiver pm
//	@param leader 新视图的 leader
//	@param proposalSequence 新视图的 seq
//	@param viewNum 新视图的 viewNum
//	@param decisionsInView
//	@param quorumSize
//	@return proposer
//	@return phase
func (pm *ProposalMaker) NewProposer(leader, proposalSequence, viewNum, decisionsInView uint64, quorumSize int) (proposer Proposer, phase Phase) {
	// 基本项赋值
	view := &View{
		RetrieveCheckpoint: pm.Checkpoint.Get,
		DecisionsPerLeader: pm.DecisionsPerLeader,
		N:                  pm.N,
		LeaderID:           leader,
		SelfID:             pm.SelfID,
		Quorum:             quorumSize,
		Number:             viewNum,
		Decider:            pm.Decider,
		FailureDetector:    pm.FailureDetector,
		Sync:               pm.Sync,
		Logger:             pm.Logger,
		Comm:               pm.Comm,
		Verifier:           pm.Verifier,
		Signer:             pm.Signer,
		MembershipNotifier: pm.MembershipNotifier,
		ProposalSequence:   proposalSequence,
		DecisionsInView:    decisionsInView,
		State:              pm.State,
		InMsgQSize:         pm.InMsqQSize,
		ViewSequences:      pm.ViewSequences, // 和controller共享同一块内存
		MetricsBlacklist:   pm.MetricsBlacklist,
		MetricsView:        pm.MetricsView,
	}
	// 存进去了 新的 seq ，controller层也能获取到
	view.ViewSequences.Store(ViewSequence{
		ViewActive:  true,
		ProposalSeq: proposalSequence,
	})
	// 只会执行一次 为了从程序崩溃中 恢复过来
	pm.restoreOnceFromWAL.Do(func() {
		err := pm.State.Restore(view)
		if err != nil {
			pm.Logger.Panicf("Failed restoring view from WAL: %v", err)
		}
	})
	// 判断从WAL 恢复出来的 view 和seq 与当前 view seq的关系
	// 取最新的
	if proposalSequence > view.ProposalSequence {
		view.ProposalSequence = proposalSequence
		view.DecisionsInView = decisionsInView
	}

	if viewNum > view.Number {
		view.Number = viewNum
		view.DecisionsInView = decisionsInView
	}

	view.MetricsView.ViewNumber.Set(float64(view.Number))
	view.MetricsView.LeaderID.Set(float64(view.LeaderID))
	view.MetricsView.ProposalSequence.Set(float64(view.ProposalSequence))
	view.MetricsView.DecisionsInView.Set(float64(view.DecisionsInView))
	view.MetricsView.Phase.Set(float64(view.Phase))
	// 如果没有从WAL恢复出来，那么Phase 是COMMIT状态
	return view, view.Phase
}

// ViewSequence indicates if a view is currently active and its current proposal sequence
// 如果视图当前处于活动状态且其当前proposal sequence
type ViewSequence struct {
	ViewActive  bool
	ProposalSeq uint64
}

// MsgToString converts a given message to a printable string
func MsgToString(m *protos.Message) string {
	if m == nil {
		return "empty message"
	}
	switch m.GetContent().(type) {
	case *protos.Message_PrePrepare:
		return prePrepareToString(m.GetPrePrepare())
	case *protos.Message_NewView:
		return newViewToString(m.GetNewView())
	case *protos.Message_ViewData:
		return signedViewDataToString(m.GetViewData())
	case *protos.Message_HeartBeat:
		return heartBeatToString(m.GetHeartBeat())
	case *protos.Message_HeartBeatResponse:
		return heartBeatResponseToString(m.GetHeartBeatResponse())
	default:
		return m.String()
	}
}

func prePrepareToString(prp *protos.PrePrepare) string {
	if prp == nil {
		return "<empty PrePrepare>"
	}
	if prp.Proposal == nil {
		return fmt.Sprintf("<PrePrepare with view: %d, seq: %d, empty proposal>", prp.View, prp.Seq)
	}
	return fmt.Sprintf("<PrePrepare with view: %d, seq: %d, payload of %d bytes, header: %s>",
		prp.View, prp.Seq, len(prp.Proposal.Payload), base64.StdEncoding.EncodeToString(prp.Proposal.Header))
}

func newViewToString(nv *protos.NewView) string {
	if nv == nil || nv.SignedViewData == nil {
		return "<empty NewView>"
	}
	buff := bytes.Buffer{}
	buff.WriteString("< NewView with ")
	for i, svd := range nv.SignedViewData {
		buff.WriteString(signedViewDataToString(svd))
		if i == len(nv.SignedViewData)-1 {
			break
		}
		buff.WriteString(", ")
	}
	buff.WriteString(">")
	return buff.String()
}

func signedViewDataToString(svd *protos.SignedViewData) string {
	if svd == nil {
		return "empty ViewData"
	}
	vd := &protos.ViewData{}
	if err := proto.Unmarshal(svd.RawViewData, vd); err != nil {
		return fmt.Sprintf("<malformed viewdata from %d>", svd.Signer)
	}

	return fmt.Sprintf("<ViewData signed by %d with NextView: %d>",
		svd.Signer, vd.NextView)
}

func heartBeatToString(hb *protos.HeartBeat) string {
	if hb == nil {
		return "empty HeartBeat"
	}

	return fmt.Sprintf("<HeartBeat with view: %d, seq: %d", hb.View, hb.Seq)
}

func heartBeatResponseToString(hbr *protos.HeartBeatResponse) string {
	if hbr == nil {
		return "empty HeartBeatResponse"
	}

	return fmt.Sprintf("<HeartBeatResponse with view: %d", hbr.View)
}

type blacklist struct {
	currentLeader      uint64
	leaderRotation     bool
	prevMD             *protos.ViewMetadata
	n                  uint64
	nodes              []uint64
	currView           uint64
	preparesFrom       map[uint64]*protos.PreparesFrom
	logger             api.Logger
	metricsBlacklist   *api.MetricsBlacklist
	f                  int
	decisionsPerLeader uint64 // 如果 > 0 那么leaderRotation 为true
}

func (bl blacklist) computeUpdate() []uint64 {
	newBlacklist := bl.prevMD.BlackList
	// 上一个proposal 的视图编号
	viewBeforeViewChanges := bl.prevMD.ViewId

	bl.logger.Debugf("view before: %d, current view: %d", viewBeforeViewChanges, bl.currView)

	// In case the previous view is different from this view, then we had a view change.
	// Thus, we need to add some nodes to the blacklist.
	// 如果以前的视图与这个视图不同，那么我们有一个视图更改。因此，我们需要将一些节点添加到黑名单中。
	if viewBeforeViewChanges != bl.currView {
		// If we are in the first proposal, then the leader ID of the previous view is not computed with an offset.
		// However, if we are in any subsequent proposal, then the previous leader's ID was computed by adding 1 to the
		// latest decisions in view that was committed.
		// 如果我们在第一个提案中，则前一个视图的leader ID 不会使用偏移量进行计算。
		// 但是，如果我们在任何后续提案中， 则前一个leader的 ID 是通过在已提交的视图中的最新决策中加 1 来计算的。
		offset := uint64(1)
		if bl.prevMD.LatestSequence == 0 {
			offset = 0
		}

		// Locate every leader of all views previous to this views.
		// 找到此视图之前的每个视图的leader
		for viewPreviousToThisView := viewBeforeViewChanges; viewPreviousToThisView < bl.currView; viewPreviousToThisView++ {
			bl.logger.Debugf("viewPreviousToThisView: %d, N: %d, Nodes: %v, rotation: %v, decisions in view: %d, decisions per leader: %d, blacklist: %v",
				viewPreviousToThisView, bl.n, bl.nodes, bl.leaderRotation, bl.prevMD.DecisionsInView, bl.decisionsPerLeader, bl.prevMD.BlackList)
			// offset 用在了这里， 这里放入的黑名单，也是前一个view的黑名单
			leaderID := getLeaderID(viewPreviousToThisView, bl.n, bl.nodes, bl.leaderRotation, bl.prevMD.DecisionsInView+offset, bl.decisionsPerLeader, bl.prevMD.BlackList)
			// leader 没有更换
			if leaderID == bl.currentLeader {
				bl.logger.Debugf("Skipping blacklisting current node (%d)", leaderID)
				continue
			}
			// Add that leader to the blacklist, because it did not drive any proposal, hence we skipped it because of view changes.
			// 将该领导者添加到黑名单中，因为它没有推动任何提案，因此我们由于视图更改而跳过了它
			newBlacklist = append(newBlacklist, leaderID)
			bl.logger.Infof("Blacklisting %d", leaderID)
		}
	} else {
		// We are in the same view, hence we can remove some nodes from the blacklist, if applicable,
		// because they helped us drive from the previous sequence to this sequence.
		// Compute the new blacklist according to your collected attestations on prepares sent
		// in previous round.
		// 我们处于相同的视图中，因此如果适用的话，我们可以从黑名单中删除一些节点，
		// 因为它们帮助我们从前一个序列驱动到这个序列。根据您收集的上一轮发送的准备证明计算新的黑名单。
		newBlacklist = pruneBlacklist(newBlacklist, bl.preparesFrom, bl.f, bl.nodes, bl.logger)
	}

	// If blacklist is too big, remove items from its beginning
	// 黑名单太大了，那么就删除一个
	for len(newBlacklist) > bl.f {
		bl.logger.Infof("Removing %d from %d sized blacklist due to size constraint", newBlacklist[0], len(newBlacklist))
		newBlacklist = newBlacklist[1:]
	}

	if len(bl.prevMD.BlackList) != len(newBlacklist) {
		bl.logger.Infof("Blacklist changed: %v --> %v", bl.prevMD.BlackList, newBlacklist)
	}

	// metric 使用 不用管
	newBlacklistMap := make(map[uint64]bool, len(newBlacklist))
	for _, node := range newBlacklist {
		newBlacklistMap[node] = true
	}
	for _, node := range bl.nodes {
		inBlacklist := newBlacklistMap[node]
		bl.metricsBlacklist.NodesInBlackList.With(
			bl.metricsBlacklist.LabelsForWith("blackid", strconv.FormatUint(node, 10))...,
		).Set(btoi(inBlacklist))
	}
	bl.metricsBlacklist.CountBlackList.Set(float64(len(newBlacklist)))

	return newBlacklist
}

func btoi(b bool) float64 {
	if b {
		return 1
	}
	return 0
}

// pruneBlacklist receives the previous blacklist, prepare acknowledgements from nodes, and returns
// the new blacklist such that a node that was observed by more than f observers is removed from the blacklist,
// and all nodes that no longer exist are also removed from the blacklist.
// pruneBlacklist 接收先前的黑名单，prepare acknowledgements from nodes，并返回新的黑名单，
// 以便将超过 f 个观察者观察到的节点从黑名单中删除，并且所有不再存在的节点也从黑名单中删除。
// 对 prevBlacklist 做了减少，没有任何的增加
func pruneBlacklist(prevBlacklist []uint64, preparesFrom map[uint64]*protos.PreparesFrom, f int, nodes []uint64, logger api.Logger) []uint64 {
	if len(prevBlacklist) == 0 {
		logger.Debugf("Blacklist empty, nothing to prune")
		return prevBlacklist
	}
	logger.Debugf("Pruning blacklist %v with %d acknowledgements, f=%d, n=%d", prevBlacklist, len(preparesFrom), f, len(nodes))
	// Build a set of all nodes
	currentNodeIDs := make(map[uint64]struct{})
	for _, n := range nodes {
		currentNodeIDs[n] = struct{}{}
	}

	// For each sender of a prepare, count the number of commit signatures which acknowledge receiving a prepare from it.
	// 对于prepare的每个发送者，计算commit signatures
	nodeID2Acks := make(map[uint64]int)
	for from, gotPrepareFrom := range preparesFrom {
		logger.Debugf("%d observed prepares from %v", from, gotPrepareFrom)
		for _, prepareSender := range gotPrepareFrom.Ids {
			nodeID2Acks[prepareSender]++
		}
	}
	// 构建新的黑名单
	var newBlackList []uint64
	// 遍历旧的黑名单每一个节点
	for _, blackListedNode := range prevBlacklist {
		// Purge nodes that were removed by a reconfiguration
		if _, exists := currentNodeIDs[blackListedNode]; !exists {
			logger.Infof("Node %d no longer exists, removing it from the blacklist", blackListedNode)
			continue
		}

		// Purge nodes that have enough attestations of being alive
		// 清除有足够活着证明的节点
		observers := nodeID2Acks[blackListedNode]
		if observers > f {
			logger.Infof("Node %d was observed sending a prepare by %d nodes, removing it from blacklist", blackListedNode, observers)
			continue
		}
		newBlackList = append(newBlackList, blackListedNode)
	}

	return newBlackList
}

func equalIntLists(a, b []uint64) bool {
	if len(a) != len(b) {
		return false
	}

	for i := 0; i < len(a); i++ {
		if a[i] != b[i] {
			return false
		}
	}

	return true
}

func CommitSignaturesDigest(sigs []*protos.Signature) []byte {
	if len(sigs) == 0 {
		return nil
	}
	idb := IntDoubleBytes{}
	for _, sig := range sigs {
		s := IntDoubleByte{
			A: int64(sig.Signer),
			B: sig.Value,
			C: sig.Msg,
		}
		idb.A = append(idb.A, s)
	}

	serializedSignatures, err := asn1.Marshal(idb)
	if err != nil {
		panic(fmt.Sprintf("failed serializing signatures: %v", err))
	}

	h := sha256.New()
	h.Write(serializedSignatures)
	return h.Sum(nil)
}

type IntDoubleByte struct {
	A    int64
	B, C []byte
}

type IntDoubleBytes struct {
	A []IntDoubleByte
}

func PrettyStruct(i interface{}) string {
	params := Flatten(i)
	var buffer bytes.Buffer
	for i := range params {
		buffer.WriteString("\n\t")
		buffer.WriteString(params[i])
	}
	return buffer.String()
}

// Flatten performs a depth-first serialization of a struct to a slice of
// strings. Each string will be formatted at 'path.to.leaf = value'.
func Flatten(i interface{}) []string {
	var res []string
	flatten("", &res, reflect.ValueOf(i))
	return res
}

// flatten recursively retrieves every leaf node in a struct in depth-first fashion
// and aggregate the results into given string slice with format: "path.to.leaf = value"
// in the order of definition. Root name is ignored in the path. This helper function is
// useful to pretty-print a struct, such as configs.
// for example, given data structure:
//
//	A{
//	  B{
//	    C: "foo",
//	    D: 42,
//	  },
//	  E: nil,
//	}
//
// it should yield a slice of string containing following items:
// [
//
//	"B.C = \"foo\"",
//	"B.D = 42",
//	"E =",
//
// ]
func flatten(k string, m *[]string, v reflect.Value) {
	delimiter := "."
	if k == "" {
		delimiter = ""
	}

	switch v.Kind() {
	case reflect.Ptr:
		if v.IsNil() {
			*m = append(*m, fmt.Sprintf("%s =", k))
			return
		}
		flatten(k, m, v.Elem())
	case reflect.Struct:
		if x, ok := v.Interface().(fmt.Stringer); ok {
			*m = append(*m, fmt.Sprintf("%s = %v", k, x))
			return
		}

		for i := 0; i < v.NumField(); i++ {
			flatten(k+delimiter+v.Type().Field(i).Name, m, v.Field(i))
		}
	case reflect.String:
		// It is useful to quote string values
		*m = append(*m, fmt.Sprintf("%s = \"%s\"", k, v))
	default:
		*m = append(*m, fmt.Sprintf("%s = %v", k, v))
	}
}
