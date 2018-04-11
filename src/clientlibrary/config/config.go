package config

import (
	"log"
	"math"
	"strings"
	"time"
)

const (
	EPSILON_MS = 25

	// LATEST start after the most recent data record (fetch new data).
	LATEST = InitialPositionInStream(1)
	// TRIM_HORIZON start from the oldest available data record
	TRIM_HORIZON = LATEST + 1
	// AT_TIMESTAMP start from the record at or after the specified server-side timestamp.
	AT_TIMESTAMP = TRIM_HORIZON + 1

	// The location in the shard from which the KinesisClientLibrary will start fetching records from
	// when the application starts for the first time and there is no checkpoint for the shard.
	DEFAULT_INITIAL_POSITION_IN_STREAM = LATEST

	// Fail over time in milliseconds. A worker which does not renew it's lease within this time interval
	// will be regarded as having problems and it's shards will be assigned to other workers.
	// For applications that have a large number of shards, this may be set to a higher number to reduce
	// the number of DynamoDB IOPS required for tracking leases.
	DEFAULT_FAILOVER_TIME_MILLIS = 10000

	// Max records to fetch from Kinesis in a single GetRecords call.
	DEFAULT_MAX_RECORDS = 10000

	// The default value for how long the {@link ShardConsumer} should sleep if no records are returned from the call to
	DEFAULT_IDLETIME_BETWEEN_READS_MILLIS = 1000

	// Don't call processRecords() on the record processor for empty record lists.
	DEFAULT_DONT_CALL_PROCESS_RECORDS_FOR_EMPTY_RECORD_LIST = false

	// Interval in milliseconds between polling to check for parent shard completion.
	// Polling frequently will take up more DynamoDB IOPS (when there are leases for shards waiting on
	// completion of parent shards).
	DEFAULT_PARENT_SHARD_POLL_INTERVAL_MILLIS = 10000

	// Shard sync interval in milliseconds - e.g. wait for this long between shard sync tasks.
	DEFAULT_SHARD_SYNC_INTERVAL_MILLIS = 60000

	// Cleanup leases upon shards completion (don't wait until they expire in Kinesis).
	// Keeping leases takes some tracking/resources (e.g. they need to be renewed, assigned), so by default we try
	// to delete the ones we don't need any longer.
	DEFAULT_CLEANUP_LEASES_UPON_SHARDS_COMPLETION = true

	// Backoff time in milliseconds for Amazon Kinesis Client Library tasks (in the event of failures).
	DEFAULT_TASK_BACKOFF_TIME_MILLIS = 500

	// Buffer metrics for at most this long before publishing to CloudWatch.
	DEFAULT_METRICS_BUFFER_TIME_MILLIS = 10000

	// Buffer at most this many metrics before publishing to CloudWatch.
	DEFAULT_METRICS_MAX_QUEUE_SIZE = 10000

	// KCL will validate client provided sequence numbers with a call to Amazon Kinesis before checkpointing for calls
	// to {@link RecordProcessorCheckpointer#checkpoint(String)} by default.
	DEFAULT_VALIDATE_SEQUENCE_NUMBER_BEFORE_CHECKPOINTING = true

	// The max number of leases (shards) this worker should process.
	// This can be useful to avoid overloading (and thrashing) a worker when a host has resource constraints
	// or during deployment.
	// NOTE: Setting this to a low value can cause data loss if workers are not able to pick up all shards in the
	// stream due to the max limit.
	DEFAULT_MAX_LEASES_FOR_WORKER = math.MaxInt16

	// Max leases to steal from another worker at one time (for load balancing).
	// Setting this to a higher number can allow for faster load convergence (e.g. during deployments, cold starts),
	// but can cause higher churn in the system.
	DEFAULT_MAX_LEASES_TO_STEAL_AT_ONE_TIME = 1

	// The Amazon DynamoDB table used for tracking leases will be provisioned with this read capacity.
	DEFAULT_INITIAL_LEASE_TABLE_READ_CAPACITY = 10

	// The Amazon DynamoDB table used for tracking leases will be provisioned with this write capacity.
	DEFAULT_INITIAL_LEASE_TABLE_WRITE_CAPACITY = 10

	// The Worker will skip shard sync during initialization if there are one or more leases in the lease table. This
	// assumes that the shards and leases are in-sync. This enables customers to choose faster startup times (e.g.
	// during incremental deployments of an application).
	DEFAULT_SKIP_SHARD_SYNC_AT_STARTUP_IF_LEASES_EXIST = false

	// The amount of milliseconds to wait before graceful shutdown forcefully terminates.
	DEFAULT_SHUTDOWN_GRACE_MILLIS = 5000

	// The size of the thread pool to create for the lease renewer to use.
	DEFAULT_MAX_LEASE_RENEWAL_THREADS = 20

	// The sleep time between two listShards calls from the proxy when throttled.
	DEFAULT_LIST_SHARDS_BACKOFF_TIME_IN_MILLIS = 1500

	// The number of times the Proxy will retry listShards call when throttled.
	DEFAULT_MAX_LIST_SHARDS_RETRY_ATTEMPTS = 50
)

type (
	// InitialPositionInStream Used to specify the position in the stream where a new application should start from
	// This is used during initial application bootstrap (when a checkpoint doesn't exist for a shard or its parents)
	InitialPositionInStream int

	// Class that houses the entities needed to specify the position in the stream from where a new application should
	// start.
	InitialPositionInStreamExtended struct {
		position InitialPositionInStream

		// The time stamp of the data record from which to start reading. Used with
		// shard iterator type AT_TIMESTAMP. A time stamp is the Unix epoch date with
		// precision in milliseconds. For example, 2016-04-04T19:58:46.480-00:00 or
		// 1459799926.480. If a record with this exact time stamp does not exist, the
		// iterator returned is for the next (later) record. If the time stamp is older
		// than the current trim horizon, the iterator returned is for the oldest untrimmed
		// data record (TRIM_HORIZON).
		timestamp *time.Time `type:"timestamp" timestampFormat:"unix"`
	}

	// Configuration for the Kinesis Client Library.
	KinesisClientLibConfiguration struct {
		// applicationName is name of application. Kinesis allows multiple applications to consume the same stream.
		applicationName string

		// tableName is name of the dynamo db table for managing kinesis stream default to applicationName
		tableName string

		// streamName is the name of Kinesis stream
		streamName string

		// workerID used to distinguish different workers/processes of a Kinesis application
		workerID string

		// kinesisEndpoint endpoint
		kinesisEndpoint string

		// dynamoDB endpoint
		dynamoDBEndpoint string

		// initialPositionInStream specifies the position in the stream where a new application should start from
		initialPositionInStream InitialPositionInStream

		// initialPositionInStreamExtended provides actual AT_TMESTAMP value
		initialPositionInStreamExtended InitialPositionInStreamExtended

		// credentials to access Kinesis/Dynamo/CloudWatch: https://docs.aws.amazon.com/sdk-for-go/api/aws/credentials/
		// Note: No need to configure here. Use NewEnvCredentials for testing and EC2RoleProvider for production

		// failoverTimeMillis Lease duration (leases not renewed within this period will be claimed by others)
		failoverTimeMillis int

		/// maxRecords Max records to read per Kinesis getRecords() call
		maxRecords int

		// idleTimeBetweenReadsInMillis Idle time between calls to fetch data from Kinesis
		idleTimeBetweenReadsInMillis int

		// callProcessRecordsEvenForEmptyRecordList Call the IRecordProcessor::processRecords() API even if
		// GetRecords returned an empty record list.
		callProcessRecordsEvenForEmptyRecordList bool

		// parentShardPollIntervalMillis Wait for this long between polls to check if parent shards are done
		parentShardPollIntervalMillis int

		// shardSyncIntervalMillis Time between tasks to sync leases and Kinesis shards
		shardSyncIntervalMillis int

		// cleanupTerminatedShardsBeforeExpiry Clean up shards we've finished processing (don't wait for expiration)
		cleanupTerminatedShardsBeforeExpiry bool

		// kinesisClientConfig Client Configuration used by Kinesis client
		// dynamoDBClientConfig Client Configuration used by DynamoDB client
		// cloudWatchClientConfig Client Configuration used by CloudWatch client
		// Note: we will use default client provided by AWS SDK

		// taskBackoffTimeMillis Backoff period when tasks encounter an exception
		taskBackoffTimeMillis int

		// metricsBufferTimeMillis Metrics are buffered for at most this long before publishing to CloudWatch
		metricsBufferTimeMillis int

		// metricsMaxQueueSize Max number of metrics to buffer before publishing to CloudWatch
		metricsMaxQueueSize int

		// validateSequenceNumberBeforeCheckpointing whether KCL should validate client provided sequence numbers
		validateSequenceNumberBeforeCheckpointing bool

		// regionName The region name for the service
		regionName string

		// shutdownGraceMillis The number of milliseconds before graceful shutdown terminates forcefully
		shutdownGraceMillis int

		// Operation parameters

		// Max leases this Worker can handle at a time
		maxLeasesForWorker int

		// Max leases to steal at one time (for load balancing)
		maxLeasesToStealAtOneTime int

		// Read capacity to provision when creating the lease table (dynamoDB).
		initialLeaseTableReadCapacity int

		// Write capacity to provision when creating the lease table.
		initialLeaseTableWriteCapacity int

		// Worker should skip syncing shards and leases at startup if leases are present
		// This is useful for optimizing deployments to large fleets working on a stable stream.
		skipShardSyncAtWorkerInitializationIfLeasesExist bool
	}
)

func empty(s string) bool {
	return len(strings.TrimSpace(s)) == 0
}

// checkIsValuePositive make sure the value is possitive.
func checkIsValueNotEmpty(key string, value string) {
	if empty(value) {
		// There is no point to continue for incorrect configuration. Fail fast!
		log.Panicf("Non-empty value exepected for %v, actual: %v", key, value)
	}
}

// checkIsValuePositive make sure the value is possitive.
func checkIsValuePositive(key string, value int) {
	if value <= 0 {
		// There is no point to continue for incorrect configuration. Fail fast!
		log.Panicf("Positive value exepected for %v, actual: %v", key, value)
	}
}
