package test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"
	"github.com/stretchr/testify/assert"
	chk "github.com/vmware/vmware-go-kcl/clientlibrary/checkpoint"
	cfg "github.com/vmware/vmware-go-kcl/clientlibrary/config"
	wk "github.com/vmware/vmware-go-kcl/clientlibrary/worker"
)

type LeaseStealingTest struct {
	t       *testing.T
	config  *TestClusterConfig
	cluster *TestCluster
	kc      kinesisiface.KinesisAPI
	dc      dynamodbiface.DynamoDBAPI

	backOffSeconds int
	maxRetries     int
}

func NewLeaseStealingTest(t *testing.T, config *TestClusterConfig, workerFactory TestWorkerFactory) *LeaseStealingTest {
	cluster := NewTestCluster(t, config, workerFactory)
	clientConfig := cluster.workerFactory.CreateKCLConfig("test-client", config)
	return &LeaseStealingTest{
		t:              t,
		config:         config,
		cluster:        cluster,
		kc:             NewKinesisClient(t, config.regionName, clientConfig.KinesisEndpoint, clientConfig.KinesisCredentials),
		dc:             NewDynamoDBClient(t, config.regionName, clientConfig.DynamoDBEndpoint, clientConfig.KinesisCredentials),
		backOffSeconds: 5,
		maxRetries:     60,
	}
}

func (lst *LeaseStealingTest) WithBackoffSeconds(backoff int) *LeaseStealingTest {
	lst.backOffSeconds = backoff
	return lst
}

func (lst *LeaseStealingTest) WithMaxRetries(retries int) *LeaseStealingTest {
	lst.maxRetries = retries
	return lst
}

func (lst *LeaseStealingTest) publishSomeData() (stop func()) {
	done := make(chan int)
	wg := &sync.WaitGroup{}

	wg.Add(1)
	go func() {
		ticker := time.NewTicker(500 * time.Millisecond)
		defer wg.Done()
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				lst.t.Log("Coninuously publishing records")
				publishSomeData(lst.t, lst.kc)
			}
		}
	}()

	return func() {
		close(done)
		wg.Wait()
	}
}

func (lst *LeaseStealingTest) getShardCountByWorker() map[string]int {
	input := &dynamodb.ScanInput{
		TableName: aws.String(lst.config.appName),
	}

	shardsByWorker := map[string]map[string]bool{}
	err := lst.dc.ScanPages(input, func(out *dynamodb.ScanOutput, lastPage bool) bool {
		for _, result := range out.Items {
			if shardID, ok := result[chk.LeaseKeyKey]; !ok {
				continue
			} else if assignedTo, ok := result[chk.LeaseOwnerKey]; !ok {
				continue
			} else {
				if _, ok := shardsByWorker[*assignedTo.S]; !ok {
					shardsByWorker[*assignedTo.S] = map[string]bool{}
				}
				shardsByWorker[*assignedTo.S][*shardID.S] = true
			}
		}
		return !lastPage
	})
	assert.Nil(lst.t, err)

	shardCountByWorker := map[string]int{}
	for worker, shards := range shardsByWorker {
		shardCountByWorker[worker] = len(shards)
	}
	return shardCountByWorker
}

type LeaseStealingAssertions struct {
	expectedLeasesForIntialWorker int
	expectedLeasesPerWorker       int
}

func (lst *LeaseStealingTest) Run(assertions LeaseStealingAssertions) {
	// Publish records onto stream thoughtout the entire duration of the test
	stop := lst.publishSomeData()
	defer stop()

	// Start worker 1
	worker1, _ := lst.cluster.SpawnWorker()

	// Wait until the above worker has all leases
	var worker1ShardCount int
	for i := 0; i < lst.maxRetries; i++ {
		time.Sleep(time.Duration(lst.backOffSeconds) * time.Second)

		shardCountByWorker := lst.getShardCountByWorker()
		if shardCount, ok := shardCountByWorker[worker1]; ok && shardCount == assertions.expectedLeasesForIntialWorker {
			worker1ShardCount = shardCount
			break
		}
	}

	// Assert correct number of leases
	assert.Equal(lst.t, assertions.expectedLeasesForIntialWorker, worker1ShardCount)

	// Spawn Remaining Wokers
	for i := 0; i < lst.config.numWorkers-1; i++ {
		lst.cluster.SpawnWorker()
	}

	// Wait For Rebalance
	var shardCountByWorker map[string]int
	for i := 0; i < lst.maxRetries; i++ {
		time.Sleep(time.Duration(lst.backOffSeconds) * time.Second)

		shardCountByWorker = lst.getShardCountByWorker()

		correctCount := true
		for _, count := range shardCountByWorker {
			if count != assertions.expectedLeasesPerWorker {
				correctCount = false
			}
		}

		if correctCount {
			break
		}
	}

	// Assert Rebalanced
	assert.Greater(lst.t, len(shardCountByWorker), 0)
	for _, count := range shardCountByWorker {
		assert.Equal(lst.t, assertions.expectedLeasesPerWorker, count)
	}

	// Shutdown Workers
	time.Sleep(10 * time.Second)
	lst.cluster.Shutdown()
}

type TestWorkerFactory interface {
	CreateWorker(workerID string, kclConfig *cfg.KinesisClientLibConfiguration) *wk.Worker
	CreateKCLConfig(workerID string, config *TestClusterConfig) *cfg.KinesisClientLibConfiguration
}

type TestClusterConfig struct {
	numShards  int
	numWorkers int

	appName          string
	streamName       string
	regionName       string
	workerIDTemplate string
}

type TestCluster struct {
	t             *testing.T
	config        *TestClusterConfig
	workerFactory TestWorkerFactory
	workerIDs     []string
	workers       map[string]*wk.Worker
}

func NewTestCluster(t *testing.T, config *TestClusterConfig, workerFactory TestWorkerFactory) *TestCluster {
	return &TestCluster{
		t:             t,
		config:        config,
		workerFactory: workerFactory,
		workerIDs:     make([]string, 0),
		workers:       make(map[string]*wk.Worker),
	}
}

func (tc *TestCluster) addWorker(workerID string, config *cfg.KinesisClientLibConfiguration) *wk.Worker {
	worker := tc.workerFactory.CreateWorker(workerID, config)
	tc.workerIDs = append(tc.workerIDs, workerID)
	tc.workers[workerID] = worker
	return worker
}

func (tc *TestCluster) SpawnWorker() (string, *wk.Worker) {
	id := len(tc.workers)
	workerID := fmt.Sprintf(tc.config.workerIDTemplate, id)

	config := tc.workerFactory.CreateKCLConfig(workerID, tc.config)
	worker := tc.addWorker(workerID, config)

	err := worker.Start()
	assert.Nil(tc.t, err)
	return workerID, worker
}

func (tc *TestCluster) Shutdown() {
	for workerID, worker := range tc.workers {
		tc.t.Logf("Shutting down worker: %v", workerID)
		worker.Shutdown()
	}
}
