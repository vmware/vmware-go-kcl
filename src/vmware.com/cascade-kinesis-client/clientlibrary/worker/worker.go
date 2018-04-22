package worker

import (
	"errors"
	log "github.com/sirupsen/logrus"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"

	"vmware.com/cascade-kinesis-client/clientlibrary/config"
	kcl "vmware.com/cascade-kinesis-client/clientlibrary/interfaces"
	"vmware.com/cascade-kinesis-client/clientlibrary/metrics"
)

type shardStatus struct {
	ID            string
	ParentShardId string
	Checkpoint    string
	AssignedTo    string
	mux           *sync.Mutex
	LeaseTimeout  time.Time
	// Shard Range
	StartingSequenceNumber string
	// child shard doesn't have end sequence number
	EndingSequenceNumber string
}

func (ss *shardStatus) getLeaseOwner() string {
	ss.mux.Lock()
	defer ss.mux.Unlock()
	return ss.AssignedTo
}

func (ss *shardStatus) setLeaseOwner(owner string) {
	ss.mux.Lock()
	defer ss.mux.Unlock()
	ss.AssignedTo = owner
}

/**
 * Worker is the high level class that Kinesis applications use to start processing data. It initializes and oversees
 * different components (e.g. syncing shard and lease information, tracking shard assignments, and processing data from
 * the shards).
 */
type Worker struct {
	streamName string
	regionName string
	workerID   string

	processorFactory kcl.IRecordProcessorFactory
	kclConfig        *config.KinesisClientLibConfiguration
	kc               kinesisiface.KinesisAPI
	dynamo           dynamodbiface.DynamoDBAPI
	checkpointer     Checkpointer

	stop      *chan struct{}
	waitGroup *sync.WaitGroup
	sigs      *chan os.Signal

	shardStatus map[string]*shardStatus

	metricsConfig *metrics.MonitoringConfiguration
	mService      metrics.MonitoringService
}

// NewWorker constructs a Worker instance for processing Kinesis stream data.
func NewWorker(factory kcl.IRecordProcessorFactory, kclConfig *config.KinesisClientLibConfiguration, metricsConfig *metrics.MonitoringConfiguration) *Worker {
	w := &Worker{
		streamName:       kclConfig.StreamName,
		regionName:       kclConfig.RegionName,
		workerID:         kclConfig.WorkerID,
		processorFactory: factory,
		kclConfig:        kclConfig,
		metricsConfig:    metricsConfig,
	}

	// create session for Kinesis
	log.Info("Creating Kinesis session")
	s := session.New(&aws.Config{Region: aws.String(w.regionName)})
	w.kc = kinesis.New(s)

	log.Info("Creating DynamoDB session")
	s = session.New(&aws.Config{Region: aws.String(w.regionName)})
	w.dynamo = dynamodb.New(s)
	w.checkpointer = NewDynamoCheckpoint(w.dynamo, kclConfig)

	if w.metricsConfig == nil {
		w.metricsConfig = &metrics.MonitoringConfiguration{MonitoringService: ""}
	}
	return w
}

// Run starts consuming data from the stream, and pass it to the application record processors.
func (w *Worker) Start() error {
	if err := w.initialize(); err != nil {
		log.Errorf("Failed to start Worker: %+v", err)
		return err
	}

	// Start monitoring service
	log.Info("Starting monitoring service.")
	w.mService.Start()

	log.Info("Starting worker event loop.")
	// entering event loop
	go w.eventLoop()
	return nil
}

// Shutdown signals worker to shutdown. Worker will try initiating shutdown of all record processors.
func (w *Worker) Shutdown() {
	log.Info("Worker shutdown in requested.")

	close(*w.stop)
	w.waitGroup.Wait()

	w.mService.Shutdown()
	log.Info("Worker loop is complete. Exiting from worker.")
}

// Publish to write some data into stream. This function is mainly used for testing purpose.
func (w *Worker) Publish(streamName, partitionKey string, data []byte) error {
	_, err := w.kc.PutRecord(&kinesis.PutRecordInput{
		Data:         data,
		StreamName:   aws.String(streamName),
		PartitionKey: aws.String(partitionKey),
	})
	if err != nil {
		log.Errorf("Error in publishing data to %s/%s. Error: %+v", streamName, partitionKey, err)
	}
	return err
}

// initialize
func (w *Worker) initialize() error {
	log.Info("Worker initialization in progress...")

	err := w.metricsConfig.Init(w.kclConfig.ApplicationName, w.streamName, w.workerID)
	if err != nil {
		log.Errorf("Failed to start monitoring service: %s", err)
	}
	w.mService = w.metricsConfig.GetMonitoringService()

	log.Info("Initializing Checkpointer")
	if err := w.checkpointer.Init(); err != nil {
		log.Errorf("Failed to start Checkpointer: %+v", err)
		return err
	}

	w.shardStatus = make(map[string]*shardStatus)

	sigs := make(chan os.Signal, 1)
	w.sigs = &sigs
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	stopChan := make(chan struct{})
	w.stop = &stopChan

	wg := sync.WaitGroup{}
	w.waitGroup = &wg

	log.Info("Initialization complete.")

	return nil
}

// newShardConsumer to create a shard consumer instance
func (w *Worker) newShardConsumer(shard *shardStatus) *ShardConsumer {
	return &ShardConsumer{
		streamName:      w.streamName,
		shard:           shard,
		kc:              w.kc,
		checkpointer:    w.checkpointer,
		recordProcessor: w.processorFactory.CreateProcessor(),
		kclConfig:       w.kclConfig,
		consumerID:      w.workerID,
		stop:            w.stop,
		waitGroup:       w.waitGroup,
		mService:        w.mService,
		state:           WAITING_ON_PARENT_SHARDS,
	}
}

// eventLoop
func (w *Worker) eventLoop() {
	for {
		err := w.syncShard()
		if err != nil {
			log.Errorf("Error getting Kinesis shards: %v", err)
			// Back-off?
			time.Sleep(500 * time.Millisecond)
		}

		log.Infof("Found %d shards", len(w.shardStatus))

		// Count the number of leases hold by this worker
		counter := 0
		for _, shard := range w.shardStatus {
			if shard.getLeaseOwner() == w.workerID {
				counter++
			}
		}

		// max number of lease has not been reached
		if counter < w.kclConfig.MaxLeasesForWorker {
			for _, shard := range w.shardStatus {
				// We already own this shard so carry on
				if shard.getLeaseOwner() == w.workerID {
					continue
				}

				err := w.checkpointer.FetchCheckpoint(shard)
				if err != nil {
					// checkpoint may not existed yet if not an error condition.
					if err != ErrSequenceIDNotFound {
						log.Error(err)
						// move on to next shard
						continue
					}
				}

				// The shard is closed and we have processed all records
				if shard.Checkpoint == SHARD_END {
					continue
				}

				err = w.checkpointer.GetLease(shard, w.workerID)
				if err != nil {
					// cannot get lease on the shard
					if err.Error() != ErrLeaseNotAquired {
						log.Error(err)
					}
					continue
				}

				// log metrics on got lease
				w.mService.LeaseGained(shard.ID)

				log.Infof("Start Shard Consumer for shard: %v", shard.ID)
				sc := w.newShardConsumer(shard)
				go sc.getRecords(shard)
				w.waitGroup.Add(1)
			}
		}

		select {
		case sig := <-*w.sigs:
			log.Infof("Received signal %s. Exiting", sig)
			w.Shutdown()
			return
		case <-*w.stop:
			log.Info("Shutting down")
			return
		case <-time.After(time.Duration(w.kclConfig.ShardSyncIntervalMillis) * time.Millisecond):
		}
	}
}

// List all ACTIVE shard and store them into shardStatus table
// If shard has been removed, need to exclude it from cached shard status.
func (w *Worker) getShardIDs(startShardID string, shardInfo map[string]bool) error {
	// The default pagination limit is 100.
	args := &kinesis.DescribeStreamInput{
		StreamName: aws.String(w.streamName),
	}
	if startShardID != "" {
		args.ExclusiveStartShardId = aws.String(startShardID)
	}
	streamDesc, err := w.kc.DescribeStream(args)
	if err != nil {
		return err
	}

	if *streamDesc.StreamDescription.StreamStatus != "ACTIVE" {
		return errors.New("Stream not active")
	}

	var lastShardID string
	for _, s := range streamDesc.StreamDescription.Shards {
		// record avail shardId from fresh reading from Kinesis
		shardInfo[*s.ShardId] = true
		// found new shard
		if _, ok := w.shardStatus[*s.ShardId]; !ok {
			log.Debugf("Found shard with id %s", *s.ShardId)
			w.shardStatus[*s.ShardId] = &shardStatus{
				ID:            *s.ShardId,
				ParentShardId: aws.StringValue(s.ParentShardId),
				mux:           &sync.Mutex{},
				StartingSequenceNumber: aws.StringValue(s.SequenceNumberRange.StartingSequenceNumber),
				EndingSequenceNumber:   aws.StringValue(s.SequenceNumberRange.EndingSequenceNumber),
			}
		}
		lastShardID = *s.ShardId
	}

	if *streamDesc.StreamDescription.HasMoreShards {
		err := w.getShardIDs(lastShardID, shardInfo)
		if err != nil {
			return err
		}
	}

	return nil
}

// syncShard to sync the cached shard info with actual shard info from Kinesis
func (w *Worker) syncShard() error {
	shardInfo := make(map[string]bool)
	err := w.getShardIDs("", shardInfo)

	for _, shard := range w.shardStatus {
		// The cached shard no longer existed, remove it.
		if _, ok := shardInfo[shard.ID]; !ok {
			delete(w.shardStatus, shard.ID)
		}
	}

	return err
}
