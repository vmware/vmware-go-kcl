package interfaces

import (
	"time"

	ks "github.com/aws/aws-sdk-go/service/kinesis"
)

const (
	/**
	 * Indicates that the entire application is being shutdown, and if desired the record processor will be given a
	 * final chance to checkpoint. This state will not trigger a direct call to
	 * {@link com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor#shutdown(ShutdownInput)}, but
	 * instead depend on a different interface for backward compatibility.
	 */
	REQUESTED ShutdownReason = iota + 1
	/**
	 * Terminate processing for this RecordProcessor (resharding use case).
	 * Indicates that the shard is closed and all records from the shard have been delivered to the application.
	 * Applications SHOULD checkpoint their progress to indicate that they have successfully processed all records
	 * from this shard and processing of child shards can be started.
	 */
	TERMINATE
	/**
	 * Processing will be moved to a different record processor (fail over, load balancing use cases).
	 * Applications SHOULD NOT checkpoint their progress (as another record processor may have already started
	 * processing data).
	 */
	ZOMBIE
)

// Containers for the parameters to the IRecordProcessor
type (
	/**
	 * Reason the RecordProcessor is being shutdown.
	 * Used to distinguish between a fail-over vs. a termination (shard is closed and all records have been delivered).
	 * In case of a fail over, applications should NOT checkpoint as part of shutdown,
	 * since another record processor may have already started processing records for that shard.
	 * In case of termination (resharding use case), applications SHOULD checkpoint their progress to indicate
	 * that they have successfully processed all the records (processing of child shards can then begin).
	 */
	ShutdownReason int

	InitializationInput struct {
		ShardId                         string
		ExtendedSequenceNumber          *ExtendedSequenceNumber
		PendingCheckpointSequenceNumber *ExtendedSequenceNumber
	}

	ProcessRecordsInput struct {
		CacheEntryTime     *time.Time
		CacheExitTime      *time.Time
		Records            []*ks.Record
		Checkpointer       IRecordProcessorCheckpointer
		MillisBehindLatest int64
	}

	ShutdownInput struct {
		ShutdownReason ShutdownReason
		Checkpointer   IRecordProcessorCheckpointer
	}
)
