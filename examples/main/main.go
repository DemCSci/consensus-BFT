package main

import (
	"fmt"
	naive "github.com/SmartBFT-Go/consensus/examples/naive_chain"
	smart "github.com/SmartBFT-Go/consensus/pkg/api"
	"github.com/SmartBFT-Go/consensus/pkg/metrics/disabled"
	"github.com/SmartBFT-Go/consensus/pkg/wal"
	"go.uber.org/zap"
	"net/http"
	"os"
	"strconv"
	"time"
)

type Application struct {
	deliverChan <-chan *naive.Block
	node        *naive.Node
}

func (app *Application) Listen() naive.Block {
	block := <-app.deliverChan
	return *block
}

func (app *Application) Order(txn naive.Transaction) error {
	return app.node.Consensus.SubmitRequest(txn.ToBytes())
}

func NewApplication(id uint64, logger smart.Logger, walmet *wal.Metrics, bftmet *smart.Metrics, opts naive.NetworkOptions, testDir string) *Application {
	deliverChan := make(chan *naive.Block)
	node := naive.NewNode(id, deliverChan, logger, walmet, bftmet, opts, testDir)
	return &Application{
		node:        node,
		deliverChan: deliverChan,
	}
}

var application *Application

func main() {
	id2Address := make(map[uint64]string)

	id2Address[1] = "127.0.0.1:8081"
	id2Address[2] = "127.0.0.1:8082"
	id2Address[3] = "127.0.0.1:8083"
	id2Address[4] = "127.0.0.1:8084"

	basicLog, err := zap.NewDevelopment(zap.IncreaseLevel(zap.InfoLevel))
	defer basicLog.Sync()

	basicLog.WithOptions()
	if err != nil {
		fmt.Println(err)
	}
	idInt, err := strconv.Atoi(os.Args[1])
	if err != nil {
		return
	}
	localId := uint64(idInt)
	LocalAddress := id2Address[localId]

	testDir, err := os.MkdirTemp("", "naive_chain"+strconv.FormatUint(localId, 10))
	numNodes := 4
	defer os.RemoveAll(testDir)
	logger := basicLog.Sugar()

	met := &disabled.Provider{}
	walMet := wal.NewMetrics(met, "label1")
	bftMet := smart.NewMetrics(met, "label1")

	networkoptions := naive.NetworkOptions{
		NumNodes: numNodes, BatchSize: 1, BatchTimeout: 10 * time.Second,
		LocalAddress: LocalAddress,
		Id2Address:   id2Address,
	}
	application = NewApplication(localId, logger, walMet, bftMet, networkoptions, testDir)
	if localId == 1 {
		go func() {
			http.HandleFunc("/hello", hello)
			http.ListenAndServe(":8090", nil)
		}()
	}
	for {
		block := application.Listen()
		logger.Infof("节点 %d 监听到区块 %d\n", localId, block.Sequence)
	}

}
func hello(w http.ResponseWriter, req *http.Request) {
	for i := 0; i < 10; i++ {
		application.Order(naive.Transaction{
			ClientID: "alice",
			ID:       fmt.Sprintf("tx %d\n", i),
		})
	}
	fmt.Fprintf(w, "hello\n")
}
