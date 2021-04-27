/*
 * Copyright (c) 2021 VMware, Inc.
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
package worker

import (
	"errors"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/kinesis"

	chk "github.com/vmware/vmware-go-kcl/clientlibrary/checkpoint"
	kcl "github.com/vmware/vmware-go-kcl/clientlibrary/interfaces"
)

// FanOutShardConsumer is  responsible for consuming data records of a (specified) shard.
// Note: FanOutShardConsumer only deal with one shard.
// For more info see: https://docs.aws.amazon.com/streams/latest/dev/enhanced-consumers.html
type FanOutShardConsumer struct {
	commonShardConsumer
	consumerARN string
	consumerID  string
	stop        *chan struct{}
}

// getRecords subscribes to a shard and reads events from it.
// Precondition: it currently has the lease on the shard.
func (sc *FanOutShardConsumer) getRecords() error {
	defer sc.releaseLease()

	log := sc.kclConfig.Logger

	// If the shard is child shard, need to wait until the parent finished.
	if err := sc.waitOnParentShard(); err != nil {
		// If parent shard has been deleted by Kinesis system already, just ignore the error.
		if err != chk.ErrSequenceIDNotFound {
			log.Errorf("Error in waiting for parent shard: %v to finish. Error: %+v", sc.shard.ParentShardId, err)
			return err
		}
	}

	shardSub, err := sc.subscribeToShard()
	if err != nil {
		log.Errorf("Unable to subscribe to shard %s: %v", sc.shard.ID, err)
		return err
	}
	defer func() {
		if shardSub == nil || shardSub.EventStream == nil {
			log.Debugf("Nothing to close, EventStream is nil")
			return
		}
		err = shardSub.EventStream.Close()
		if err != nil {
			log.Errorf("Unable to close event stream for %s: %v", sc.shard.ID, err)
		}
	}()

	input := &kcl.InitializationInput{
		ShardId:                sc.shard.ID,
		ExtendedSequenceNumber: &kcl.ExtendedSequenceNumber{SequenceNumber: aws.String(sc.shard.GetCheckpoint())},
	}
	sc.recordProcessor.Initialize(input)
	recordCheckpointer := NewRecordProcessorCheckpoint(sc.shard, sc.checkpointer)

	var continuationSequenceNumber *string
	refreshLeaseTimer := time.After(time.Until(sc.shard.LeaseTimeout.Add(-time.Duration(sc.kclConfig.LeaseRefreshPeriodMillis) * time.Millisecond)))
	for {
		getRecordsStartTime := time.Now()
		select {
		case <-*sc.stop:
			shutdownInput := &kcl.ShutdownInput{ShutdownReason: kcl.REQUESTED, Checkpointer: recordCheckpointer}
			sc.recordProcessor.Shutdown(shutdownInput)
			return nil
		case <-refreshLeaseTimer:
			log.Debugf("Refreshing lease on shard: %s for worker: %s", sc.shard.ID, sc.consumerID)
			err = sc.checkpointer.GetLease(sc.shard, sc.consumerID)
			if err != nil {
				if errors.As(err, &chk.ErrLeaseNotAcquired{}) {
					log.Warnf("Failed in acquiring lease on shard: %s for worker: %s", sc.shard.ID, sc.consumerID)
					return nil
				}
				log.Errorf("Error in refreshing lease on shard: %s for worker: %s. Error: %+v", sc.shard.ID, sc.consumerID, err)
				return err
			}
			refreshLeaseTimer = time.After(time.Until(sc.shard.LeaseTimeout.Add(-time.Duration(sc.kclConfig.LeaseRefreshPeriodMillis) * time.Millisecond)))
		case event, ok := <-shardSub.EventStream.Events():
			if !ok {
				// need to resubscribe to shard
				log.Debugf("Event stream ended, refreshing subscription on shard: %s for worker: %s", sc.shard.ID, sc.consumerID)
				if continuationSequenceNumber == nil || *continuationSequenceNumber == "" {
					log.Debugf("No continuation sequence number")
					return nil
				}
				shardSub, err = sc.resubscribe(shardSub, continuationSequenceNumber)
				if err != nil {
					return err
				}
				continue
			}
			subEvent, ok := event.(*kinesis.SubscribeToShardEvent)
			if !ok {
				log.Errorf("Received unexpected event type: %T", event)
				continue
			}
			continuationSequenceNumber = subEvent.ContinuationSequenceNumber
			sc.processRecords(getRecordsStartTime, subEvent.Records, subEvent.MillisBehindLatest, recordCheckpointer)

			// The shard has been closed, so no new records can be read from it
			if continuationSequenceNumber == nil {
				log.Infof("Shard %s closed", sc.shard.ID)
				shutdownInput := &kcl.ShutdownInput{ShutdownReason: kcl.TERMINATE, Checkpointer: recordCheckpointer}
				sc.recordProcessor.Shutdown(shutdownInput)
				return nil
			}
		}
	}
}

func (sc *FanOutShardConsumer) subscribeToShard() (*kinesis.SubscribeToShardOutput, error) {
	startPosition, err := sc.getStartingPosition()
	if err != nil {
		return nil, err
	}

	return sc.kc.SubscribeToShard(&kinesis.SubscribeToShardInput{
		ConsumerARN:      &sc.consumerARN,
		ShardId:          &sc.shard.ID,
		StartingPosition: startPosition,
	})
}

func (sc *FanOutShardConsumer) resubscribe(shardSub *kinesis.SubscribeToShardOutput, continuationSequence *string) (*kinesis.SubscribeToShardOutput, error) {
	err := shardSub.EventStream.Close()
	if err != nil {
		sc.kclConfig.Logger.Errorf("Unable to close event stream for %s: %v", sc.shard.ID, err)
		return nil, err
	}
	startPosition := &kinesis.StartingPosition{
		Type:           aws.String("AFTER_SEQUENCE_NUMBER"),
		SequenceNumber: continuationSequence,
	}
	shardSub, err = sc.kc.SubscribeToShard(&kinesis.SubscribeToShardInput{
		ConsumerARN:      &sc.consumerARN,
		ShardId:          &sc.shard.ID,
		StartingPosition: startPosition,
	})
	if err != nil {
		sc.kclConfig.Logger.Errorf("Unable to resubscribe to shard %s: %v", sc.shard.ID, err)
		return nil, err
	}
	return shardSub, nil
}
