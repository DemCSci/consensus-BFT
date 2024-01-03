// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package naive

import (
	"context"
	"crypto/sha256"
	"encoding/asn1"
	"encoding/hex"
	"fmt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/peer"
	"log"
	"net"
	"path/filepath"
	"sync"
	"time"

	smart "github.com/SmartBFT-Go/consensus/pkg/api"
	smartbft "github.com/SmartBFT-Go/consensus/pkg/consensus"
	bft "github.com/SmartBFT-Go/consensus/pkg/types"
	"github.com/SmartBFT-Go/consensus/pkg/wal"
	"github.com/SmartBFT-Go/consensus/smartbftprotos"
	"github.com/golang/protobuf/proto"
)

type Block struct {
	Sequence     uint64
	PrevHash     string
	Transactions []Transaction
}

type BlockHeader struct {
	Sequence int64
	PrevHash string
	DataHash string
}

func (header BlockHeader) ToBytes() []byte {
	rawHeader, err := asn1.Marshal(header)
	if err != nil {
		panic(err)
	}
	return rawHeader
}

func BlockHeaderFromBytes(rawHeader []byte) *BlockHeader {
	var header BlockHeader
	asn1.Unmarshal(rawHeader, &header)
	return &header
}

type Transaction struct {
	ClientID string
	ID       string
}

func (txn Transaction) ToBytes() []byte {
	rawTxn, err := asn1.Marshal(txn)
	if err != nil {
		panic(err)
	}
	return rawTxn
}

func TransactionFromBytes(rawTxn []byte) *Transaction {
	var txn Transaction
	asn1.Unmarshal(rawTxn, &txn)
	return &txn
}

type BlockData struct {
	Transactions [][]byte
}

func (b BlockData) ToBytes() []byte {
	rawBlock, err := asn1.Marshal(b)
	if err != nil {
		panic(err)
	}
	return rawBlock
}

func BlockDataFromBytes(rawBlock []byte) *BlockData {
	var block BlockData
	asn1.Unmarshal(rawBlock, &block)
	return &block
}

type NetworkOptions struct {
	NumNodes     int
	BatchSize    uint64
	BatchTimeout time.Duration
	LocalAddress string
	Id2Address   map[uint64]string
}

type Node struct {
	clock       *time.Ticker
	secondClock *time.Ticker
	stopChan    chan struct{}
	doneWG      sync.WaitGroup
	prevHash    string
	id          uint64
	deliverChan chan<- *Block
	Consensus   *smartbft.Consensus
	comm        *CommService
}

func (*Node) Sync() bft.SyncResponse {
	panic("implement me")
}

func (*Node) AuxiliaryData(_ []byte) []byte {
	return nil
}

func (*Node) RequestID(req []byte) bft.RequestInfo {
	txn := TransactionFromBytes(req)
	return bft.RequestInfo{
		ClientID: txn.ClientID,
		ID:       txn.ID,
	}
}

func (*Node) VerifyProposal(proposal bft.Proposal) ([]bft.RequestInfo, error) {
	blockData := BlockDataFromBytes(proposal.Payload)
	requests := make([]bft.RequestInfo, 0)
	for _, t := range blockData.Transactions {
		tx := TransactionFromBytes(t)
		reqInfo := bft.RequestInfo{ID: tx.ID, ClientID: tx.ClientID}
		requests = append(requests, reqInfo)
	}
	return requests, nil
}

func (*Node) RequestsFromProposal(proposal bft.Proposal) []bft.RequestInfo {
	blockData := BlockDataFromBytes(proposal.Payload)
	requests := make([]bft.RequestInfo, 0)
	for _, t := range blockData.Transactions {
		tx := TransactionFromBytes(t)
		reqInfo := bft.RequestInfo{ID: tx.ID, ClientID: tx.ClientID}
		requests = append(requests, reqInfo)
	}
	return requests
}

func (*Node) VerifyRequest(val []byte) (bft.RequestInfo, error) {
	return bft.RequestInfo{}, nil
}

func (*Node) VerifyConsenterSig(_ bft.Signature, prop bft.Proposal) ([]byte, error) {
	return nil, nil
}

func (*Node) VerifySignature(signature bft.Signature) error {
	return nil
}

func (*Node) VerificationSequence() uint64 {
	return 0
}

func (*Node) Sign(msg []byte) []byte {
	return nil
}

func (n *Node) SignProposal(bft.Proposal, []byte) *bft.Signature {
	return &bft.Signature{
		ID: n.id,
	}
}

func (n *Node) AssembleProposal(metadata []byte, requests [][]byte) bft.Proposal {
	blockData := BlockData{Transactions: requests}.ToBytes()
	md := &smartbftprotos.ViewMetadata{}
	if err := proto.Unmarshal(metadata, md); err != nil {
		panic(fmt.Sprintf("Unable to unmarshal metadata, error: %v", err))
	}
	return bft.Proposal{
		Header: BlockHeader{
			PrevHash: n.prevHash,
			DataHash: computeDigest(blockData),
			Sequence: int64(md.LatestSequence),
		}.ToBytes(),
		Payload:  BlockData{Transactions: requests}.ToBytes(),
		Metadata: metadata,
	}
}

func (n *Node) SendConsensus(targetID uint64, message *smartbftprotos.Message) {
	//n.out[int(targetID)] <- message
	n.comm.sendConsensus(targetID, message)
}

func (n *Node) SendTransaction(targetID uint64, request []byte) {
	msg := &smartbftprotos.FwdMessage{
		Payload: request,
		Sender:  n.id,
	}
	//n.out[int(targetID)] <- msg
	n.comm.sendTransaction(targetID, msg)
}

func (n *Node) MembershipChange() bool {
	return false
}

func (n *Node) Deliver(proposal bft.Proposal, signature []bft.Signature) bft.Reconfig {
	blockData := BlockDataFromBytes(proposal.Payload)
	txns := make([]Transaction, 0, len(blockData.Transactions))
	for _, rawTxn := range blockData.Transactions {
		txn := TransactionFromBytes(rawTxn)
		txns = append(txns, Transaction{
			ClientID: txn.ClientID,
			ID:       txn.ID,
		})
	}
	header := BlockHeaderFromBytes(proposal.Header)

	select {
	case <-n.stopChan:
		return bft.Reconfig{InLatestDecision: false}
	case n.deliverChan <- &Block{
		Sequence:     uint64(header.Sequence),
		PrevHash:     header.PrevHash,
		Transactions: txns,
	}:
	}

	return bft.Reconfig{InLatestDecision: false}
}

func NewNode(id uint64, deliverChan chan<- *Block, logger smart.Logger, walmet *wal.Metrics, bftmet *smart.Metrics, opts NetworkOptions, testDir string) *Node {
	nodeDir := filepath.Join(testDir, fmt.Sprintf("node%d", id))

	writeAheadLog, err := wal.Create(logger, nodeDir, &wal.Options{Metrics: walmet.With("label1", "val1")})
	if err != nil {
		logger.Panicf("Cannot create WAL at %s", nodeDir)
	}

	comm := &CommService{
		UnimplementedCommServiceServer: smartbftprotos.UnimplementedCommServiceServer{},
		in:                             make(map[uint64]chan proto.Message),
		out:                            nil,
		networkoptions:                 &opts,
	}
	for k, _ := range opts.Id2Address {
		comm.in[k] = make(chan proto.Message)
	}
	node := &Node{
		clock:       time.NewTicker(time.Second),
		secondClock: time.NewTicker(time.Second),
		id:          id,
		comm:        comm,
		deliverChan: deliverChan,
		stopChan:    make(chan struct{}),
	}
	comm.node = node
	config := bft.DefaultConfig
	config.SelfID = id
	config.RequestBatchMaxInterval = opts.BatchTimeout
	config.RequestBatchMaxCount = opts.BatchSize

	node.Consensus = &smartbft.Consensus{
		Config:             config,
		ViewChangerTicker:  node.secondClock.C,
		Scheduler:          node.clock.C,
		Logger:             logger,
		Metrics:            bftmet,
		Comm:               node,
		Signer:             node,
		MembershipNotifier: node,
		Verifier:           node,
		Application:        node,
		Assembler:          node,
		RequestInspector:   node,
		Synchronizer:       node,
		WAL:                writeAheadLog,
		Metadata: &smartbftprotos.ViewMetadata{
			LatestSequence: 0,
			ViewId:         0,
		},
	}
	if err = node.Consensus.Start(); err != nil {
		panic("error on consensus start")
	}
	node.Start()
	// 启动服务
	go func() {
		node.comm.server(opts.LocalAddress)
	}()
	return node
}

func (n *Node) Start() {
	for id, in := range n.comm.in {
		if uint64(id) == n.id {
			continue
		}
		n.doneWG.Add(1)

		go func(id uint64, in <-chan proto.Message) {
			defer n.doneWG.Done()

			for {
				select {
				case <-n.stopChan:
					return
				case msg := <-in:
					switch m := msg.(type) {
					case *smartbftprotos.Message:
						n.Consensus.HandleMessage(id, m)
					case *smartbftprotos.FwdMessage:
						n.Consensus.SubmitRequest(m.Payload)
					}
				}
			}
		}(uint64(id), in)
	}
}

func (n *Node) Stop() {
	select {
	case <-n.stopChan:
		break
	default:
		close(n.stopChan)
	}
	n.clock.Stop()
	n.doneWG.Wait()
	n.Consensus.Stop()
}

func (n *Node) Nodes() []uint64 {
	nodes := make([]uint64, 0, len(n.comm.in))
	for id := range n.comm.in {
		nodes = append(nodes, id)
	}

	return nodes
}

func computeDigest(rawBytes []byte) string {
	h := sha256.New()
	h.Write(rawBytes)
	digest := h.Sum(nil)
	return hex.EncodeToString(digest)
}

// grpc 通讯系统
type CommService struct {
	smartbftprotos.UnimplementedCommServiceServer
	networkoptions *NetworkOptions
	in             map[uint64]chan proto.Message
	out            chan proto.Message
	node           *Node
}

// grpc 服务端收到了消息
func (c *CommService) Communication(ctx context.Context, in *smartbftprotos.Request) (*smartbftprotos.Response, error) {
	_, ok := peer.FromContext(ctx)
	if !ok {
		fmt.Println("提取发送者信息错误")
		return nil, nil
	}
	message := in.GetMessage()
	if message != nil {
		c.in[in.GetId()] <- message
	} else {
		message2 := in.GetFwdMessage()
		if message2 != nil {
			c.in[in.GetId()] <- message2
		}
	}
	return &smartbftprotos.Response{
		Code: 200,
		Data: "",
	}, nil
}

func (c *CommService) server(localAddress string) {

	listener, err := net.Listen("tcp", localAddress)
	if err != nil {
		log.Fatalf("net.Listen err: %v", err)
	}
	log.Println(localAddress + " net.Listing...")
	// 新建gRPC服务器实例
	grpcServer := grpc.NewServer()
	smartbftprotos.RegisterCommServiceServer(grpcServer, c)
	//用服务器 Serve() 方法以及我们的端口信息区实现阻塞等待，直到进程被杀死或者 Stop() 被调用
	err = grpcServer.Serve(listener)
	if err != nil {
		log.Fatalf("grpcServer.Serve err: %v", err)
	}
	fmt.Println("不应该到这里")
}

func (c *CommService) sendConsensus(id uint64, message *smartbftprotos.Message) {
	targetAddress := c.networkoptions.Id2Address[id]
	// 连接服务器
	conn, err := grpc.Dial(targetAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("net.Connect err: %v", err)
	}
	defer conn.Close()
	// 建立gRPC连接
	client := smartbftprotos.NewCommServiceClient(conn)
	// 初始化上下文，设置请求超时时间为1秒
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	request := &smartbftprotos.Request{
		Id:   c.node.id,
		Body: &smartbftprotos.Request_Message{Message: message},
	}

	_, err = client.Communication(ctx, request)
	if err != nil {
		fmt.Printf("与%d 节点通信错误， err= %v \n", id, err)
	}
}

func (c *CommService) sendTransaction(id uint64, message *smartbftprotos.FwdMessage) {
	targetAddress := c.networkoptions.Id2Address[id]
	// 连接服务器
	conn, err := grpc.Dial(targetAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("net.Connect err: %v", err)
	}
	defer conn.Close()
	// 建立gRPC连接
	client := smartbftprotos.NewCommServiceClient(conn)
	// 初始化上下文，设置请求超时时间为1秒
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	request := &smartbftprotos.Request{
		Id:   c.node.id,
		Body: &smartbftprotos.Request_FwdMessage{FwdMessage: message},
	}
	_, err = client.Communication(ctx, request)
	if err != nil {
		fmt.Printf("与%d 节点通信错误， err= %\n", err)
	}
}
