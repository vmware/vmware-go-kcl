/*
 * Copyright (c) 2018 VMware, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and
 * associated documentation files (the "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is furnished to do
 * so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies or substantial
 * portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT
 * NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
 * WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */
// The implementation is derived from https://github.com/patrobinson/gokini
//
// Copyright 2018 Patrick robinson
//
// Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
package worker

import (
	"math/rand"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"

	chk "github.com/vmware/vmware-go-kcl/clientlibrary/checkpoint"
	"github.com/vmware/vmware-go-kcl/clientlibrary/config"
	kcl "github.com/vmware/vmware-go-kcl/clientlibrary/interfaces"
	"github.com/vmware/vmware-go-kcl/clientlibrary/metrics"
	par "github.com/vmware/vmware-go-kcl/clientlibrary/partition"
)

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
	checkpointer     chk.Checkpointer
	mService         metrics.MonitoringService

	stop      *chan struct{}
	waitGroup *sync.WaitGroup
	done      bool

	rng *rand.Rand

	shardStatus map[string]*par.ShardStatus
}

// NewWorker constructs a Worker instance for processing Kinesis stream data.
func NewWorker(factory kcl.IRecordProcessorFactory, kclConfig *config.KinesisClientLibConfiguration) *Worker {
	mService := kclConfig.MonitoringService
	if mService == nil {
		// Replaces nil with noop monitor service (not emitting any metrics).
		mService = metrics.NoopMonitoringService{}
	}

	// Create a pseudo-random number generator and seed it.
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	return &Worker{
		streamName:       kclConfig.StreamName,
		regionName:       kclConfig.RegionName,
		workerID:         kclConfig.WorkerID,
		processorFactory: factory,
		kclConfig:        kclConfig,
		mService:         mService,
		done:             false,
		rng:              rng,
	}
}

// WithKinesis is used to provide Kinesis service for either custom implementation or unit testing.
func (w *Worker) WithKinesis(svc kinesisiface.KinesisAPI) *Worker {
	w.kc = svc
	return w
}

// WithCheckpointer is used to provide a custom checkpointer service for non-dynamodb implementation
// or unit testing.
func (w *Worker) WithCheckpointer(checker chk.Checkpointer) *Worker {
	w.checkpointer = checker
	return w
}

// Run starts consuming data from the stream, and pass it to the application record processors.
func (w *Worker) Start() error {
	log := w.kclConfig.Logger
	if err := w.initialize(); err != nil {
		log.Errorf("Failed to initialize Worker: %+v", err)
		return err
	}

	// Start monitoring service
	log.Infof("Starting monitoring service.")
	if err := w.mService.Start(); err != nil {
		log.Errorf("Failed to start monitoring service: %+v", err)
		return err
	}

	log.Infof("Starting worker event loop.")
	w.waitGroup.Add(1)
	go func() {
		defer w.waitGroup.Done()
		// entering event loop
		w.eventLoop()
	}()
	return nil
}

// Shutdown signals worker to shutdown. Worker will try initiating shutdown of all record processors.
func (w *Worker) Shutdown() {
	log := w.kclConfig.Logger
	log.Infof("Worker shutdown in requested.")

	if w.done || w.stop == nil {
		return
	}

	close(*w.stop)
	w.done = true
	w.waitGroup.Wait()

	w.mService.Shutdown()
	log.Infof("Worker loop is complete. Exiting from worker.")
}

// Publish to write some data into stream. This function is mainly used for testing purpose.
func (w *Worker) Publish(streamName, partitionKey string, data []byte) error {
	log := w.kclConfig.Logger
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
	log := w.kclConfig.Logger
	log.Infof("Worker initialization in progress...")

	// Create default Kinesis session
	if w.kc == nil {
		// create session for Kinesis
		log.Infof("Creating Kinesis session")

		s, err := session.NewSession(&aws.Config{
			Region:      aws.String(w.regionName),
			Endpoint:    &w.kclConfig.KinesisEndpoint,
			Credentials: w.kclConfig.KinesisCredentials,
		})

		if err != nil {
			// no need to move forward
			log.Fatalf("Failed in getting Kinesis session for creating Worker: %+v", err)
		}
		w.kc = kinesis.New(s)
	} else {
		log.Infof("Use custom Kinesis service.")
	}

	// Create default dynamodb based checkpointer implementation
	if w.checkpointer == nil {
		log.Infof("Creating DynamoDB based checkpointer")
		w.checkpointer = chk.NewDynamoCheckpoint(w.kclConfig)
	} else {
		log.Infof("Use custom checkpointer implementation.")
	}

	err := w.mService.Init(w.kclConfig.ApplicationName, w.streamName, w.workerID)
	if err != nil {
		log.Errorf("Failed to start monitoring service: %+v", err)
	}

	log.Infof("Initializing Checkpointer")
	if err := w.checkpointer.Init(); err != nil {
		log.Errorf("Failed to start Checkpointer: %+v", err)
		return err
	}

	w.shardStatus = make(map[string]*par.ShardStatus)

	stopChan := make(chan struct{})
	w.stop = &stopChan

	w.waitGroup = &sync.WaitGroup{}

	log.Infof("Initialization complete.")

	return nil
}

// newShardConsumer to create a shard consumer instance
func (w *Worker) newShardConsumer(shard *par.ShardStatus) *ShardConsumer {
	return &ShardConsumer{
		streamName:      w.streamName,
		shard:           shard,
		kc:              w.kc,
		checkpointer:    w.checkpointer,
		recordProcessor: w.processorFactory.CreateProcessor(),
		kclConfig:       w.kclConfig,
		consumerID:      w.workerID,
		stop:            w.stop,
		mService:        w.mService,
		state:           WAITING_ON_PARENT_SHARDS,
	}
}

// eventLoop
func (w *Worker) eventLoop() {
	log := w.kclConfig.Logger

	var foundShards int
	for {
		// Add [-50%, +50%] random jitter to ShardSyncIntervalMillis. When multiple workers
		// starts at the same time, this decreases the probability of them calling
		// kinesis.DescribeStream at the same time, and hit the hard-limit on aws API calls.
		// On average the period remains the same so that doesn't affect behavior.
		shardSyncSleep := w.kclConfig.ShardSyncIntervalMillis/2 + w.rng.Intn(int(w.kclConfig.ShardSyncIntervalMillis))

		err := w.syncShard()
		if err != nil {
			log.Errorf("Error syncing shards: %+v, Retrying in %d ms...", err, shardSyncSleep)
			time.Sleep(time.Duration(shardSyncSleep) * time.Millisecond)
			continue
		}

		if foundShards == 0 || foundShards != len(w.shardStatus) {
			foundShards = len(w.shardStatus)
			log.Infof("Found %d shards", foundShards)
		}

		// Count the number of leases hold by this worker excluding the processed shard
		counter := 0
		for _, shard := range w.shardStatus {
			if shard.GetLeaseOwner() == w.workerID && shard.Checkpoint != chk.SHARD_END {
				counter++
			}
		}

		// max number of lease has not been reached yet
		if counter < w.kclConfig.MaxLeasesForWorker {
			for _, shard := range w.shardStatus {
				// already owner of the shard
				if shard.GetLeaseOwner() == w.workerID {
					continue
				}

				err := w.checkpointer.FetchCheckpoint(shard)
				if err != nil {
					// checkpoint may not existed yet is not an error condition.
					if err != chk.ErrSequenceIDNotFound {
						log.Errorf(" Error: %+v", err)
						// move on to next shard
						continue
					}
				}

				// The shard is closed and we have processed all records
				if shard.Checkpoint == chk.SHARD_END {
					continue
				}

				err = w.checkpointer.GetLease(shard, w.workerID)
				if err != nil {
					// cannot get lease on the shard
					if err.Error() != chk.ErrLeaseNotAquired {
						log.Errorf("Cannot get lease: %+v", err)
					}
					continue
				}

				// log metrics on got lease
				w.mService.LeaseGained(shard.ID)

				log.Infof("Start Shard Consumer for shard: %v", shard.ID)
				sc := w.newShardConsumer(shard)
				w.waitGroup.Add(1)
				go func() {
					defer w.waitGroup.Done()
					if err := sc.getRecords(shard); err != nil {
						log.Errorf("Error in getRecords: %+v", err)
					}
				}()
				// exit from for loop and not to grab more shard for now.
				break
			}
		}

		select {
		case <-*w.stop:
			log.Infof("Shutting down...")
			return
		case <-time.After(time.Duration(shardSyncSleep) * time.Millisecond):
			log.Debugf("Waited %d ms to sync shards...", shardSyncSleep)
		}
	}
}

// List all shards and store them into shardStatus table
// If shard has been removed, need to exclude it from cached shard status.
func (w *Worker) getShardIDs(nextToken string, shardInfo map[string]bool) error {
	log := w.kclConfig.Logger

	args := &kinesis.ListShardsInput{}

	// When you have a nextToken, you can't set the streamName
	if nextToken != "" {
		args.NextToken = aws.String(nextToken)
	} else {
		args.StreamName = aws.String(w.streamName)
	}

	listShards, err := w.kc.ListShards(args)
	if err != nil {
		log.Errorf("Error in ListShards: %s Error: %+v Request: %s", w.streamName, err, args)
		return err
	}

	for _, s := range listShards.Shards {
		// record avail shardId from fresh reading from Kinesis
		shardInfo[*s.ShardId] = true

		// found new shard
		if _, ok := w.shardStatus[*s.ShardId]; !ok {
			log.Infof("Found new shard with id %s", *s.ShardId)
			w.shardStatus[*s.ShardId] = &par.ShardStatus{
				ID:                     *s.ShardId,
				ParentShardId:          aws.StringValue(s.ParentShardId),
				Mux:                    &sync.Mutex{},
				StartingSequenceNumber: aws.StringValue(s.SequenceNumberRange.StartingSequenceNumber),
				EndingSequenceNumber:   aws.StringValue(s.SequenceNumberRange.EndingSequenceNumber),
			}
		}
	}

	if listShards.NextToken != nil {
		err := w.getShardIDs(*listShards.NextToken, shardInfo)
		if err != nil {
			log.Errorf("Error in ListShards: %s Error: %+v Request: %s", w.streamName, err, args)
			return err
		}
	}

	return nil
}

// syncShard to sync the cached shard info with actual shard info from Kinesis
func (w *Worker) syncShard() error {
	log := w.kclConfig.Logger
	shardInfo := make(map[string]bool)
	err := w.getShardIDs("", shardInfo)

	if err != nil {
		return err
	}

	for _, shard := range w.shardStatus {
		// The cached shard no longer existed, remove it.
		if _, ok := shardInfo[shard.ID]; !ok {
			// remove the shard from local status cache
			delete(w.shardStatus, shard.ID)
			// remove the shard entry in dynamoDB as well
			// Note: syncShard runs periodically. we don't need to do anything in case of error here.
			if err := w.checkpointer.RemoveLeaseInfo(shard.ID); err != nil {
				log.Errorf("Failed to remove shard lease info: %s Error: %+v", shard.ID, err)
			}
		}
	}

	return nil
}
