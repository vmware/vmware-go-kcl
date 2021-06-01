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
	"math"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	creds "github.com/aws/aws-sdk-go/aws/credentials"

	"github.com/vmware/vmware-go-kcl/clientlibrary/metrics"
	"github.com/vmware/vmware-go-kcl/logger"
)

const (
	// LATEST start after the most recent data record (fetch new data).
	LATEST InitialPositionInStream = iota + 1
	// TRIM_HORIZON start from the oldest available data record
	TRIM_HORIZON
	// AT_TIMESTAMP start from the record at or after the specified server-side Timestamp.
	AT_TIMESTAMP

	// The location in the shard from which the KinesisClientLibrary will start fetching records from
	// when the application starts for the first time and there is no checkpoint for the shard.
	DefaultInitialPositionInStream = LATEST

	// Fail over time in milliseconds. A worker which does not renew it's lease within this time interval
	// will be regarded as having problems and it's shards will be assigned to other workers.
	// For applications that have a large number of shards, this may be set to a higher number to reduce
	// the number of DynamoDB IOPS required for tracking leases.
	DefaultFailoverTimeMillis = 10000

	// Period before the end of lease during which a lease is refreshed by the owner.
	DefaultLeaseRefreshPeriodMillis = 5000

	// Max records to fetch from Kinesis in a single GetRecords call.
	DefaultMaxRecords = 10000

	// The default value for how long the {@link ShardConsumer} should sleep if no records are returned
	// from the call to
	DefaultIdletimeBetweenReadsMillis = 1000

	// Don't call processRecords() on the record processor for empty record lists.
	DefaultDontCallProcessRecordsForEmptyRecordList = false

	// Interval in milliseconds between polling to check for parent shard completion.
	// Polling frequently will take up more DynamoDB IOPS (when there are leases for shards waiting on
	// completion of parent shards).
	DefaultParentShardPollIntervalMillis = 10000

	// Shard sync interval in milliseconds - e.g. wait for this long between shard sync tasks.
	DefaultShardSyncIntervalMillis = 60000

	// Cleanup leases upon shards completion (don't wait until they expire in Kinesis).
	// Keeping leases takes some tracking/resources (e.g. they need to be renewed, assigned), so by
	// default we try to delete the ones we don't need any longer.
	DefaultCleanupLeasesUponShardsCompletion = true

	// Backoff time in milliseconds for Amazon Kinesis Client Library tasks (in the event of failures).
	DefaultTaskBackoffTimeMillis = 500

	// KCL will validate client provided sequence numbers with a call to Amazon Kinesis before
	// checkpointing for calls to {@link RecordProcessorCheckpointer#checkpoint(String)} by default.
	DefaultValidateSequenceNumberBeforeCheckpointing = true

	// The max number of leases (shards) this worker should process.
	// This can be useful to avoid overloading (and thrashing) a worker when a host has resource constraints
	// or during deployment.
	// NOTE: Setting this to a low value can cause data loss if workers are not able to pick up all shards in the
	// stream due to the max limit.
	DefaultMaxLeasesForWorker = math.MaxInt16

	// Max leases to steal from another worker at one time (for load balancing).
	// Setting this to a higher number can allow for faster load convergence (e.g. during deployments, cold starts),
	// but can cause higher churn in the system.
	DefaultMaxLeasesToStealAtOneTime = 1

	// The Amazon DynamoDB table used for tracking leases will be provisioned with this read capacity.
	DefaultInitialLeaseTableReadCapacity = 10

	// The Amazon DynamoDB table used for tracking leases will be provisioned with this write capacity.
	DefaultInitialLeaseTableWriteCapacity = 10

	// The Worker will skip shard sync during initialization if there are one or more leases in the lease table. This
	// assumes that the shards and leases are in-sync. This enables customers to choose faster startup times (e.g.
	// during incremental deployments of an application).
	DefaultSkipShardSyncAtStartupIfLeasesExist = false

	// The amount of milliseconds to wait before graceful shutdown forcefully terminates.
	DefaultShutdownGraceMillis = 5000

	// Lease stealing defaults to false for backwards compatibility.
	DefaultEnableLeaseStealing = false

	// Interval between rebalance tasks defaults to 5 seconds.
	DefaultLeaseStealingIntervalMillis = 5000

	// Number of milliseconds to wait before another worker can aquire a claimed shard
	DefaultLeaseStealingClaimTimeoutMillis = 120000

	// Number of milliseconds to wait before syncing with lease table (dynamodDB)
	DefaultLeaseSyncingIntervalMillis = 60000
)

type (
	// InitialPositionInStream Used to specify the Position in the stream where a new application should start from
	// This is used during initial application bootstrap (when a checkpoint doesn't exist for a shard or its parents)
	InitialPositionInStream int

	// Class that houses the entities needed to specify the Position in the stream from where a new application should
	// start.
	InitialPositionInStreamExtended struct {
		Position InitialPositionInStream

		// The time stamp of the data record from which to start reading. Used with
		// shard iterator type AT_TIMESTAMP. A time stamp is the Unix epoch date with
		// precision in milliseconds. For example, 2016-04-04T19:58:46.480-00:00 or
		// 1459799926.480. If a record with this exact time stamp does not exist, the
		// iterator returned is for the next (later) record. If the time stamp is older
		// than the current trim horizon, the iterator returned is for the oldest untrimmed
		// data record (TRIM_HORIZON).
		Timestamp *time.Time `type:"Timestamp" timestampFormat:"unix"`
	}

	// Configuration for the Kinesis Client Library.
	// Note: There is no need to configure credential provider. Credential can be get from InstanceProfile.
	KinesisClientLibConfiguration struct {
		// ApplicationName is name of application. Kinesis allows multiple applications to consume the same stream.
		ApplicationName string

		// DynamoDBEndpoint is an optional endpoint URL that overrides the default generated endpoint for a DynamoDB client.
		// If this is empty, the default generated endpoint will be used.
		DynamoDBEndpoint string

		// KinesisEndpoint is an optional endpoint URL that overrides the default generated endpoint for a Kinesis client.
		// If this is empty, the default generated endpoint will be used.
		KinesisEndpoint string

		// KinesisCredentials is used to access Kinesis
		KinesisCredentials *creds.Credentials

		// DynamoDBCredentials is used to access DynamoDB
		DynamoDBCredentials *creds.Credentials

		// TableName is name of the dynamo db table for managing kinesis stream default to ApplicationName
		TableName string

		// StreamName is the name of Kinesis stream
		StreamName string

		// EnableEnhancedFanOutConsumer enables enhanced fan-out consumer
		// See: https://docs.aws.amazon.com/streams/latest/dev/enhanced-consumers.html
		// Either consumer name or consumer ARN must be specified when Enhanced Fan-Out is enabled.
		EnableEnhancedFanOutConsumer bool

		// EnhancedFanOutConsumerName is the name of the enhanced fan-out consumer to create. If this isn't set the ApplicationName will be used.
		EnhancedFanOutConsumerName string

		// EnhancedFanOutConsumerARN is the ARN of an already created enhanced fan-out consumer, if this is set no automatic consumer creation will be attempted
		EnhancedFanOutConsumerARN string

		// WorkerID used to distinguish different workers/processes of a Kinesis application
		WorkerID string

		// InitialPositionInStream specifies the Position in the stream where a new application should start from
		InitialPositionInStream InitialPositionInStream

		// InitialPositionInStreamExtended provides actual AT_TIMESTAMP value
		InitialPositionInStreamExtended InitialPositionInStreamExtended

		// credentials to access Kinesis/Dynamo: https://docs.aws.amazon.com/sdk-for-go/api/aws/credentials/
		// Note: No need to configure here. Use NewEnvCredentials for testing and EC2RoleProvider for production

		// FailoverTimeMillis Lease duration (leases not renewed within this period will be claimed by others)
		FailoverTimeMillis int

		// LeaseRefreshPeriodMillis is the period before the end of lease during which a lease is refreshed by the owner.
		LeaseRefreshPeriodMillis int

		// MaxRecords Max records to read per Kinesis getRecords() call
		MaxRecords int

		// IdleTimeBetweenReadsInMillis Idle time between calls to fetch data from Kinesis
		IdleTimeBetweenReadsInMillis int

		// CallProcessRecordsEvenForEmptyRecordList Call the IRecordProcessor::processRecords() API even if
		// GetRecords returned an empty record list.
		CallProcessRecordsEvenForEmptyRecordList bool

		// ParentShardPollIntervalMillis Wait for this long between polls to check if parent shards are done
		ParentShardPollIntervalMillis int

		// ShardSyncIntervalMillis Time between tasks to sync leases and Kinesis shards
		ShardSyncIntervalMillis int

		// CleanupTerminatedShardsBeforeExpiry Clean up shards we've finished processing (don't wait for expiration)
		CleanupTerminatedShardsBeforeExpiry bool

		// kinesisClientConfig Client Configuration used by Kinesis client
		// dynamoDBClientConfig Client Configuration used by DynamoDB client
		// Note: we will use default client provided by AWS SDK

		// TaskBackoffTimeMillis Backoff period when tasks encounter an exception
		TaskBackoffTimeMillis int

		// ValidateSequenceNumberBeforeCheckpointing whether KCL should validate client provided sequence numbers
		ValidateSequenceNumberBeforeCheckpointing bool

		// RegionName The region name for the service
		RegionName string

		// ShutdownGraceMillis The number of milliseconds before graceful shutdown terminates forcefully
		ShutdownGraceMillis int

		// Operation parameters

		// Max leases this Worker can handle at a time
		MaxLeasesForWorker int

		// Max leases to steal at one time (for load balancing)
		MaxLeasesToStealAtOneTime int

		// Read capacity to provision when creating the lease table (dynamoDB).
		InitialLeaseTableReadCapacity int

		// Write capacity to provision when creating the lease table.
		InitialLeaseTableWriteCapacity int

		// Worker should skip syncing shards and leases at startup if leases are present
		// This is useful for optimizing deployments to large fleets working on a stable stream.
		SkipShardSyncAtWorkerInitializationIfLeasesExist bool

		// Logger used to log message.
		Logger logger.Logger

		// MonitoringService publishes per worker-scoped metrics.
		MonitoringService metrics.MonitoringService

		// EnableLeaseStealing turns on lease stealing
		EnableLeaseStealing bool

		// LeaseStealingIntervalMillis The number of milliseconds between rebalance tasks
		LeaseStealingIntervalMillis int

		// LeaseStealingClaimTimeoutMillis The number of milliseconds to wait before another worker can aquire a claimed shard
		LeaseStealingClaimTimeoutMillis int

		// LeaseSyncingTimeInterval The number of milliseconds to wait before syncing with lease table (dynamoDB)
		LeaseSyncingTimeIntervalMillis int
	}
)

var positionMap = map[InitialPositionInStream]*string{
	LATEST:       aws.String("LATEST"),
	TRIM_HORIZON: aws.String("TRIM_HORIZON"),
	AT_TIMESTAMP: aws.String("AT_TIMESTAMP"),
}

func InitalPositionInStreamToShardIteratorType(pos InitialPositionInStream) *string {
	return positionMap[pos]
}

func empty(s string) bool {
	return len(strings.TrimSpace(s)) == 0
}

// checkIsValueNotEmpty makes sure the value is not empty.
func checkIsValueNotEmpty(key string, value string) {
	if empty(value) {
		// There is no point to continue for incorrect configuration. Fail fast!
		log.Panicf("Non-empty value expected for %v, actual: %v", key, value)
	}
}

// checkIsValuePositive makes sure the value is possitive.
func checkIsValuePositive(key string, value int) {
	if value <= 0 {
		// There is no point to continue for incorrect configuration. Fail fast!
		log.Panicf("Positive value expected for %v, actual: %v", key, value)
	}
}
