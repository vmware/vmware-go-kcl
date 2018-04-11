package config

import (
	"clientlibrary/utils"
	"time"
)

// NewKinesisClientLibConfig to create a default KinesisClientLibConfiguration based on the required fields.
func NewKinesisClientLibConfig(applicationName, streamName, workerID string) *KinesisClientLibConfiguration {
	checkIsValueNotEmpty("applicationName", applicationName)
	checkIsValueNotEmpty("streamName", streamName)
	checkIsValueNotEmpty("applicationName", applicationName)

	if empty(workerID) {
		workerID = utils.MustNewUUID()
	}

	// populate the KCL configuration with default values
	return &KinesisClientLibConfiguration{
		applicationName:                           applicationName,
		tableName:                                 applicationName,
		streamName:                                streamName,
		workerID:                                  workerID,
		kinesisEndpoint:                           "",
		initialPositionInStream:                   DEFAULT_INITIAL_POSITION_IN_STREAM,
		initialPositionInStreamExtended:           *newInitialPosition(DEFAULT_INITIAL_POSITION_IN_STREAM),
		failoverTimeMillis:                        DEFAULT_FAILOVER_TIME_MILLIS,
		maxRecords:                                DEFAULT_MAX_RECORDS,
		idleTimeBetweenReadsInMillis:              DEFAULT_IDLETIME_BETWEEN_READS_MILLIS,
		callProcessRecordsEvenForEmptyRecordList:  DEFAULT_DONT_CALL_PROCESS_RECORDS_FOR_EMPTY_RECORD_LIST,
		parentShardPollIntervalMillis:             DEFAULT_PARENT_SHARD_POLL_INTERVAL_MILLIS,
		shardSyncIntervalMillis:                   DEFAULT_SHARD_SYNC_INTERVAL_MILLIS,
		cleanupTerminatedShardsBeforeExpiry:       DEFAULT_CLEANUP_LEASES_UPON_SHARDS_COMPLETION,
		taskBackoffTimeMillis:                     DEFAULT_TASK_BACKOFF_TIME_MILLIS,
		metricsBufferTimeMillis:                   DEFAULT_METRICS_BUFFER_TIME_MILLIS,
		metricsMaxQueueSize:                       DEFAULT_METRICS_MAX_QUEUE_SIZE,
		validateSequenceNumberBeforeCheckpointing: DEFAULT_VALIDATE_SEQUENCE_NUMBER_BEFORE_CHECKPOINTING,
		regionName:                                       "",
		shutdownGraceMillis:                              DEFAULT_SHUTDOWN_GRACE_MILLIS,
		maxLeasesForWorker:                               DEFAULT_MAX_LEASES_FOR_WORKER,
		maxLeasesToStealAtOneTime:                        DEFAULT_MAX_LEASES_TO_STEAL_AT_ONE_TIME,
		initialLeaseTableReadCapacity:                    DEFAULT_INITIAL_LEASE_TABLE_READ_CAPACITY,
		initialLeaseTableWriteCapacity:                   DEFAULT_INITIAL_LEASE_TABLE_WRITE_CAPACITY,
		skipShardSyncAtWorkerInitializationIfLeasesExist: DEFAULT_SKIP_SHARD_SYNC_AT_STARTUP_IF_LEASES_EXIST,
	}
}

// WithTableName to provide alternative lease table in DynamoDB
func (c *KinesisClientLibConfiguration) WithTableName(tableName string) *KinesisClientLibConfiguration {
	c.tableName = tableName
	return c
}

func (c *KinesisClientLibConfiguration) WithKinesisEndpoint(kinesisEndpoint string) *KinesisClientLibConfiguration {
	c.kinesisEndpoint = kinesisEndpoint
	return c
}

func (c *KinesisClientLibConfiguration) WithInitialPositionInStream(initialPositionInStream InitialPositionInStream) *KinesisClientLibConfiguration {
	c.initialPositionInStream = initialPositionInStream
	c.initialPositionInStreamExtended = *newInitialPosition(initialPositionInStream)
	return c
}

func (c *KinesisClientLibConfiguration) WithTimestampAtInitialPositionInStream(timestamp *time.Time) *KinesisClientLibConfiguration {
	c.initialPositionInStream = AT_TIMESTAMP
	c.initialPositionInStreamExtended = *newInitialPositionAtTimestamp(timestamp)
	return c
}

func (c *KinesisClientLibConfiguration) WithFailoverTimeMillis(failoverTimeMillis int) *KinesisClientLibConfiguration {
	checkIsValuePositive("FailoverTimeMillis", failoverTimeMillis)
	c.failoverTimeMillis = failoverTimeMillis
	return c
}

func (c *KinesisClientLibConfiguration) WithShardSyncIntervalMillis(shardSyncIntervalMillis int) *KinesisClientLibConfiguration {
	checkIsValuePositive("ShardSyncIntervalMillis", shardSyncIntervalMillis)
	c.shardSyncIntervalMillis = shardSyncIntervalMillis
	return c
}

func (c *KinesisClientLibConfiguration) WithMaxRecords(maxRecords int) *KinesisClientLibConfiguration {
	checkIsValuePositive("MaxRecords", maxRecords)
	c.maxRecords = maxRecords
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
 * @param idleTimeBetweenReadsInMillis
 *            how long to sleep between GetRecords calls when no records are returned.
 * @return KinesisClientLibConfiguration
 */
func (c *KinesisClientLibConfiguration) WithIdleTimeBetweenReadsInMillis(idleTimeBetweenReadsInMillis int) *KinesisClientLibConfiguration {
	checkIsValuePositive("IdleTimeBetweenReadsInMillis", idleTimeBetweenReadsInMillis)
	c.idleTimeBetweenReadsInMillis = idleTimeBetweenReadsInMillis
	return c
}

func (c *KinesisClientLibConfiguration) WithCallProcessRecordsEvenForEmptyRecordList(callProcessRecordsEvenForEmptyRecordList bool) *KinesisClientLibConfiguration {
	c.callProcessRecordsEvenForEmptyRecordList = callProcessRecordsEvenForEmptyRecordList
	return c
}

func (c *KinesisClientLibConfiguration) WithTaskBackoffTimeMillis(taskBackoffTimeMillis int) *KinesisClientLibConfiguration {
	checkIsValuePositive("taskBackoffTimeMillis", taskBackoffTimeMillis)
	c.taskBackoffTimeMillis = taskBackoffTimeMillis
	return c
}

// WithMetricsBufferTimeMillis configures Metrics are buffered for at most this long before publishing to CloudWatch
func (c *KinesisClientLibConfiguration) WithMetricsBufferTimeMillis(metricsBufferTimeMillis int) *KinesisClientLibConfiguration {
	checkIsValuePositive("metricsBufferTimeMillis", metricsBufferTimeMillis)
	c.metricsBufferTimeMillis = metricsBufferTimeMillis
	return c
}

// WithMetricsMaxQueueSize configures Max number of metrics to buffer before publishing to CloudWatch
func (c *KinesisClientLibConfiguration) WithMetricsMaxQueueSize(metricsMaxQueueSize int) *KinesisClientLibConfiguration {
	checkIsValuePositive("metricsMaxQueueSize", metricsMaxQueueSize)
	c.metricsMaxQueueSize = metricsMaxQueueSize
	return c
}

// WithRegionName configures region for the stream
func (c *KinesisClientLibConfiguration) WithRegionName(regionName string) *KinesisClientLibConfiguration {
	checkIsValueNotEmpty("regionName", regionName)
	c.regionName = regionName
	return c
}

// Getters
