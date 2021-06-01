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
// The implementation is derived from https://github.com/awslabs/amazon-kinesis-client
/*
 * Copyright 2014-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://aws.amazon.com/asl/
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package config

import (
	"log"
	"time"

	"github.com/aws/aws-sdk-go/aws/credentials"

	"github.com/vmware/vmware-go-kcl/clientlibrary/metrics"
	"github.com/vmware/vmware-go-kcl/clientlibrary/utils"
	"github.com/vmware/vmware-go-kcl/logger"
)

// NewKinesisClientLibConfig creates a default KinesisClientLibConfiguration based on the required fields.
func NewKinesisClientLibConfig(applicationName, streamName, regionName, workerID string) *KinesisClientLibConfiguration {
	return NewKinesisClientLibConfigWithCredentials(applicationName, streamName, regionName, workerID,
		nil, nil)
}

// NewKinesisClientLibConfigWithCredential creates a default KinesisClientLibConfiguration based on the required fields and unique credentials.
func NewKinesisClientLibConfigWithCredential(applicationName, streamName, regionName, workerID string,
	creds *credentials.Credentials) *KinesisClientLibConfiguration {
	return NewKinesisClientLibConfigWithCredentials(applicationName, streamName, regionName, workerID, creds, creds)
}

// NewKinesisClientLibConfigWithCredentials creates a default KinesisClientLibConfiguration based on the required fields and specific credentials for each service.
func NewKinesisClientLibConfigWithCredentials(applicationName, streamName, regionName, workerID string,
	kiniesisCreds, dynamodbCreds *credentials.Credentials) *KinesisClientLibConfiguration {
	checkIsValueNotEmpty("ApplicationName", applicationName)
	checkIsValueNotEmpty("StreamName", streamName)
	checkIsValueNotEmpty("RegionName", regionName)

	if empty(workerID) {
		workerID = utils.MustNewUUID()
	}

	// populate the KCL configuration with default values
	return &KinesisClientLibConfiguration{
		ApplicationName:                                  applicationName,
		KinesisCredentials:                               kiniesisCreds,
		DynamoDBCredentials:                              dynamodbCreds,
		TableName:                                        applicationName,
		EnhancedFanOutConsumerName:                       applicationName,
		StreamName:                                       streamName,
		RegionName:                                       regionName,
		WorkerID:                                         workerID,
		InitialPositionInStream:                          DefaultInitialPositionInStream,
		InitialPositionInStreamExtended:                  *newInitialPosition(DefaultInitialPositionInStream),
		FailoverTimeMillis:                               DefaultFailoverTimeMillis,
		LeaseRefreshPeriodMillis:                         DefaultLeaseRefreshPeriodMillis,
		MaxRecords:                                       DefaultMaxRecords,
		IdleTimeBetweenReadsInMillis:                     DefaultIdletimeBetweenReadsMillis,
		CallProcessRecordsEvenForEmptyRecordList:         DefaultDontCallProcessRecordsForEmptyRecordList,
		ParentShardPollIntervalMillis:                    DefaultParentShardPollIntervalMillis,
		ShardSyncIntervalMillis:                          DefaultShardSyncIntervalMillis,
		CleanupTerminatedShardsBeforeExpiry:              DefaultCleanupLeasesUponShardsCompletion,
		TaskBackoffTimeMillis:                            DefaultTaskBackoffTimeMillis,
		ValidateSequenceNumberBeforeCheckpointing:        DefaultValidateSequenceNumberBeforeCheckpointing,
		ShutdownGraceMillis:                              DefaultShutdownGraceMillis,
		MaxLeasesForWorker:                               DefaultMaxLeasesForWorker,
		MaxLeasesToStealAtOneTime:                        DefaultMaxLeasesToStealAtOneTime,
		InitialLeaseTableReadCapacity:                    DefaultInitialLeaseTableReadCapacity,
		InitialLeaseTableWriteCapacity:                   DefaultInitialLeaseTableWriteCapacity,
		SkipShardSyncAtWorkerInitializationIfLeasesExist: DefaultSkipShardSyncAtStartupIfLeasesExist,
		EnableLeaseStealing:                              DefaultEnableLeaseStealing,
		LeaseStealingIntervalMillis:                      DefaultLeaseStealingIntervalMillis,
		LeaseStealingClaimTimeoutMillis:                  DefaultLeaseStealingClaimTimeoutMillis,
		LeaseSyncingTimeIntervalMillis:                   DefaultLeaseSyncingIntervalMillis,
		Logger:                                           logger.GetDefaultLogger(),
	}
}

// WithKinesisEndpoint is used to provide an alternative Kinesis endpoint
func (c *KinesisClientLibConfiguration) WithKinesisEndpoint(kinesisEndpoint string) *KinesisClientLibConfiguration {
	c.KinesisEndpoint = kinesisEndpoint
	return c
}

// WithDynamoDBEndpoint is used to provide an alternative DynamoDB endpoint
func (c *KinesisClientLibConfiguration) WithDynamoDBEndpoint(dynamoDBEndpoint string) *KinesisClientLibConfiguration {
	c.DynamoDBEndpoint = dynamoDBEndpoint
	return c
}

// WithTableName to provide alternative lease table in DynamoDB
func (c *KinesisClientLibConfiguration) WithTableName(tableName string) *KinesisClientLibConfiguration {
	c.TableName = tableName
	return c
}

func (c *KinesisClientLibConfiguration) WithInitialPositionInStream(initialPositionInStream InitialPositionInStream) *KinesisClientLibConfiguration {
	c.InitialPositionInStream = initialPositionInStream
	c.InitialPositionInStreamExtended = *newInitialPosition(initialPositionInStream)
	return c
}

func (c *KinesisClientLibConfiguration) WithTimestampAtInitialPositionInStream(timestamp *time.Time) *KinesisClientLibConfiguration {
	c.InitialPositionInStream = AT_TIMESTAMP
	c.InitialPositionInStreamExtended = *newInitialPositionAtTimestamp(timestamp)
	return c
}

func (c *KinesisClientLibConfiguration) WithFailoverTimeMillis(failoverTimeMillis int) *KinesisClientLibConfiguration {
	checkIsValuePositive("FailoverTimeMillis", failoverTimeMillis)
	c.FailoverTimeMillis = failoverTimeMillis
	return c
}

func (c *KinesisClientLibConfiguration) WithLeaseRefreshPeriodMillis(leaseRefreshPeriodMillis int) *KinesisClientLibConfiguration {
	checkIsValuePositive("LeaseRefreshPeriodMillis", leaseRefreshPeriodMillis)
	c.LeaseRefreshPeriodMillis = leaseRefreshPeriodMillis
	return c
}

func (c *KinesisClientLibConfiguration) WithShardSyncIntervalMillis(shardSyncIntervalMillis int) *KinesisClientLibConfiguration {
	checkIsValuePositive("ShardSyncIntervalMillis", shardSyncIntervalMillis)
	c.ShardSyncIntervalMillis = shardSyncIntervalMillis
	return c
}

func (c *KinesisClientLibConfiguration) WithMaxRecords(maxRecords int) *KinesisClientLibConfiguration {
	checkIsValuePositive("MaxRecords", maxRecords)
	c.MaxRecords = maxRecords
	return c
}

// WithMaxLeasesForWorker configures maximum lease this worker can handles. It determines how maximun number of shards
// this worker can handle.
func (c *KinesisClientLibConfiguration) WithMaxLeasesForWorker(n int) *KinesisClientLibConfiguration {
	checkIsValuePositive("MaxLeasesForWorker", n)
	c.MaxLeasesForWorker = n
	return c
}

/**
 * Controls how long the KCL will sleep if no records are returned from Kinesis
 *
 * <p>
 * This value is only used when no records are returned; if records are returned, the {@link com.amazonaws.services.kinesis.clientlibrary.lib.worker.ProcessTask} will
 * immediately retrieve the next set of records after the call to
 * {@link com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor#processRecords(ProcessRecordsInput)}
 * has returned. Setting this value to high may result in the KCL being unable to catch up. If you are changing this
 * value it's recommended that you enable {@link #withCallProcessRecordsEvenForEmptyRecordList(boolean)}, and
 * monitor how far behind the records retrieved are by inspecting
 * {@link com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput#getMillisBehindLatest()}, and the
 * <a href=
 * "http://docs.aws.amazon.com/streams/latest/dev/monitoring-with-cloudwatch.html#kinesis-metrics-stream">CloudWatch
 * Metric: GetRecords.MillisBehindLatest</a>
 * </p>
 *
 * @param IdleTimeBetweenReadsInMillis
 *            how long to sleep between GetRecords calls when no records are returned.
 * @return KinesisClientLibConfiguration
 */
func (c *KinesisClientLibConfiguration) WithIdleTimeBetweenReadsInMillis(idleTimeBetweenReadsInMillis int) *KinesisClientLibConfiguration {
	checkIsValuePositive("IdleTimeBetweenReadsInMillis", idleTimeBetweenReadsInMillis)
	c.IdleTimeBetweenReadsInMillis = idleTimeBetweenReadsInMillis
	return c
}

func (c *KinesisClientLibConfiguration) WithCallProcessRecordsEvenForEmptyRecordList(callProcessRecordsEvenForEmptyRecordList bool) *KinesisClientLibConfiguration {
	c.CallProcessRecordsEvenForEmptyRecordList = callProcessRecordsEvenForEmptyRecordList
	return c
}

func (c *KinesisClientLibConfiguration) WithTaskBackoffTimeMillis(taskBackoffTimeMillis int) *KinesisClientLibConfiguration {
	checkIsValuePositive("TaskBackoffTimeMillis", taskBackoffTimeMillis)
	c.TaskBackoffTimeMillis = taskBackoffTimeMillis
	return c
}

func (c *KinesisClientLibConfiguration) WithLogger(logger logger.Logger) *KinesisClientLibConfiguration {
	if logger == nil {
		log.Panic("Logger cannot be null")
	}
	c.Logger = logger
	return c
}

// WithMonitoringService sets the monitoring service to use to publish metrics.
func (c *KinesisClientLibConfiguration) WithMonitoringService(mService metrics.MonitoringService) *KinesisClientLibConfiguration {
	// Nil case is handled downward (at worker creation) so no need to do it here.
	// Plus the user might want to be explicit about passing a nil monitoring service here.
	c.MonitoringService = mService
	return c
}

// WithEnhancedFanOutConsumer sets EnableEnhancedFanOutConsumer. If enhanced fan-out is enabled and ConsumerName is not specified ApplicationName is used as ConsumerName.
// For more info see: https://docs.aws.amazon.com/streams/latest/dev/enhanced-consumers.html
// Note: You can register up to twenty consumers per stream to use enhanced fan-out.
func (c *KinesisClientLibConfiguration) WithEnhancedFanOutConsumer(enable bool) *KinesisClientLibConfiguration {
	c.EnableEnhancedFanOutConsumer = enable
	return c
}

// WithEnhancedFanOutConsumerName enables enhanced fan-out consumer with the specified name
// For more info see: https://docs.aws.amazon.com/streams/latest/dev/enhanced-consumers.html
// Note: You can register up to twenty consumers per stream to use enhanced fan-out.
func (c *KinesisClientLibConfiguration) WithEnhancedFanOutConsumerName(consumerName string) *KinesisClientLibConfiguration {
	checkIsValueNotEmpty("EnhancedFanOutConsumerName", consumerName)
	c.EnhancedFanOutConsumerName = consumerName
	c.EnableEnhancedFanOutConsumer = true
	return c
}

// WithEnhancedFanOutConsumerARN enables enhanced fan-out consumer with the specified consumer ARN
// For more info see: https://docs.aws.amazon.com/streams/latest/dev/enhanced-consumers.html
// Note: You can register up to twenty consumers per stream to use enhanced fan-out.
func (c *KinesisClientLibConfiguration) WithEnhancedFanOutConsumerARN(consumerARN string) *KinesisClientLibConfiguration {
	checkIsValueNotEmpty("EnhancedFanOutConsumerARN", consumerARN)
	c.EnhancedFanOutConsumerARN = consumerARN
	c.EnableEnhancedFanOutConsumer = true
	return c
}

func (c *KinesisClientLibConfiguration) WithLeaseStealing(enableLeaseStealing bool) *KinesisClientLibConfiguration {
	c.EnableLeaseStealing = enableLeaseStealing
	return c
}

func (c *KinesisClientLibConfiguration) WithLeaseStealingIntervalMillis(leaseStealingIntervalMillis int) *KinesisClientLibConfiguration {
	c.LeaseStealingIntervalMillis = leaseStealingIntervalMillis
	return c
}

func (c *KinesisClientLibConfiguration) WithLeaseSyncingIntervalMillis(leaseSyncingIntervalMillis int) *KinesisClientLibConfiguration {
	c.LeaseSyncingTimeIntervalMillis = leaseSyncingIntervalMillis
	return c
}
