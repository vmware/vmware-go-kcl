package test

import (
	"testing"

	chk "github.com/vmware/vmware-go-kcl/clientlibrary/checkpoint"
	cfg "github.com/vmware/vmware-go-kcl/clientlibrary/config"
	wk "github.com/vmware/vmware-go-kcl/clientlibrary/worker"
	"github.com/vmware/vmware-go-kcl/logger"
)

func TestLeaseStealing(t *testing.T) {
	config := &TestClusterConfig{
		numShards:        4,
		numWorkers:       2,
		appName:          appName,
		streamName:       streamName,
		regionName:       regionName,
		workerIDTemplate: workerID + "-%v",
	}
	test := NewLeaseStealingTest(t, config, newLeaseStealingWorkerFactory(t))
	test.Run(LeaseStealingAssertions{
		expectedLeasesForIntialWorker: config.numShards,
		expectedLeasesPerWorker:       config.numShards / config.numWorkers,
	})
}

type leaseStealingWorkerFactory struct {
	t *testing.T
}

func newLeaseStealingWorkerFactory(t *testing.T) *leaseStealingWorkerFactory {
	return &leaseStealingWorkerFactory{t}
}

func (wf *leaseStealingWorkerFactory) CreateKCLConfig(workerID string, config *TestClusterConfig) *cfg.KinesisClientLibConfiguration {
	log := logger.NewLogrusLoggerWithConfig(logger.Configuration{
		EnableConsole:     true,
		ConsoleLevel:      logger.Error,
		ConsoleJSONFormat: false,
		EnableFile:        true,
		FileLevel:         logger.Info,
		FileJSONFormat:    true,
		Filename:          "log.log",
	})

	log.WithFields(logger.Fields{"worker": workerID})

	return cfg.NewKinesisClientLibConfig(config.appName, config.streamName, config.regionName, workerID).
		WithInitialPositionInStream(cfg.LATEST).
		WithMaxRecords(10).
		WithShardSyncIntervalMillis(5000).
		WithFailoverTimeMillis(10000).
		WithLeaseStealing(true).
		WithLogger(log)
}

func (wf *leaseStealingWorkerFactory) CreateWorker(workerID string, kclConfig *cfg.KinesisClientLibConfiguration) *wk.Worker {
	worker := wk.NewWorker(recordProcessorFactory(wf.t), kclConfig)
	return worker
}

func TestLeaseStealingInjectCheckpointer(t *testing.T) {
	config := &TestClusterConfig{
		numShards:        4,
		numWorkers:       2,
		appName:          appName,
		streamName:       streamName,
		regionName:       regionName,
		workerIDTemplate: workerID + "-%v",
	}
	test := NewLeaseStealingTest(t, config, newleaseStealingWorkerFactoryCustomChk(t))
	test.Run(LeaseStealingAssertions{
		expectedLeasesForIntialWorker: config.numShards,
		expectedLeasesPerWorker:       config.numShards / config.numWorkers,
	})
}

type leaseStealingWorkerFactoryCustom struct {
	*leaseStealingWorkerFactory
}

func newleaseStealingWorkerFactoryCustomChk(t *testing.T) *leaseStealingWorkerFactoryCustom {
	return &leaseStealingWorkerFactoryCustom{
		newLeaseStealingWorkerFactory(t),
	}
}

func (wfc *leaseStealingWorkerFactoryCustom) CreateWorker(workerID string, kclConfig *cfg.KinesisClientLibConfiguration) *wk.Worker {
	worker := wfc.leaseStealingWorkerFactory.CreateWorker(workerID, kclConfig)
	checkpointer := chk.NewDynamoCheckpoint(kclConfig)
	return worker.WithCheckpointer(checkpointer)
}

func TestLeaseStealingWithMaxLeasesForWorker(t *testing.T) {
	config := &TestClusterConfig{
		numShards:        4,
		numWorkers:       2,
		appName:          appName,
		streamName:       streamName,
		regionName:       regionName,
		workerIDTemplate: workerID + "-%v",
	}
	test := NewLeaseStealingTest(t, config, newleaseStealingWorkerFactoryMaxLeases(t, config.numShards-1))
	test.Run(LeaseStealingAssertions{
		expectedLeasesForIntialWorker: config.numShards - 1,
		expectedLeasesPerWorker:       2,
	})
}

type leaseStealingWorkerFactoryMaxLeases struct {
	maxLeases int
	*leaseStealingWorkerFactory
}

func newleaseStealingWorkerFactoryMaxLeases(t *testing.T, maxLeases int) *leaseStealingWorkerFactoryMaxLeases {
	return &leaseStealingWorkerFactoryMaxLeases{
		maxLeases,
		newLeaseStealingWorkerFactory(t),
	}
}

func (wfm *leaseStealingWorkerFactoryMaxLeases) CreateKCLConfig(workerID string, config *TestClusterConfig) *cfg.KinesisClientLibConfiguration {
	kclConfig := wfm.leaseStealingWorkerFactory.CreateKCLConfig(workerID, config)
	kclConfig.WithMaxLeasesForWorker(wfm.maxLeases)
	return kclConfig
}
