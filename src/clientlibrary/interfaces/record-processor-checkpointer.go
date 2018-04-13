package interfaces

import (
	ks "github.com/aws/aws-sdk-go/service/kinesis"
)

type (
	IPreparedCheckpointer interface {
		getPendingCheckpoint() ExtendedSequenceNumber

		/**
		 * This method will record a pending checkpoint.
		 *
		 * @error ThrottlingError Can't store checkpoint. Can be caused by checkpointing too frequently.
		 *         Consider increasing the throughput/capacity of the checkpoint store or reducing checkpoint frequency.
		 * @error ShutdownError The record processor instance has been shutdown. Another instance may have
		 *         started processing some of these records already.
		 *         The application should abort processing via this RecordProcessor instance.
		 * @error InvalidStateError Can't store checkpoint.
		 *         Unable to store the checkpoint in the DynamoDB table (e.g. table doesn't exist).
		 * @error KinesisClientLibDependencyError Encountered an issue when storing the checkpoint. The application can
		 *         backoff and retry.
		 * @error IllegalArgumentError The sequence number being checkpointed is invalid because it is out of range,
		 *         i.e. it is smaller than the last check point value (prepared or committed), or larger than the greatest
		 *         sequence number seen by the associated record processor.
		 */
		checkpoint() error
	}

	/**
	 * Used by RecordProcessors when they want to checkpoint their progress.
	 * The Kinesis Client Library will pass an object implementing this interface to RecordProcessors, so they can
	 * checkpoint their progress.
	 */
	IRecordProcessorCheckpointer interface {

		/**
		 * This method will checkpoint the progress at the last data record that was delivered to the record processor.
		 * Upon fail over (after a successful checkpoint() call), the new/replacement RecordProcessor instance
		 * will receive data records whose sequenceNumber > checkpoint position (for each partition key).
		 * In steady state, applications should checkpoint periodically (e.g. once every 5 minutes).
		 * Calling this API too frequently can slow down the application (because it puts pressure on the underlying
		 * checkpoint storage layer).
		 *
		 * @error ThrottlingError Can't store checkpoint. Can be caused by checkpointing too frequently.
		 *         Consider increasing the throughput/capacity of the checkpoint store or reducing checkpoint frequency.
		 * @error ShutdownError The record processor instance has been shutdown. Another instance may have
		 *         started processing some of these records already.
		 *         The application should abort processing via this RecordProcessor instance.
		 * @error InvalidStateError Can't store checkpoint.
		 *         Unable to store the checkpoint in the DynamoDB table (e.g. table doesn't exist).
		 * @error KinesisClientLibDependencyError Encountered an issue when storing the checkpoint. The application can
		 *         backoff and retry.
		 */
		checkpoint() error

		/**
		 * This method will checkpoint the progress at the provided record. This method is analogous to
		 * {@link #checkpoint()} but provides the ability to specify the record at which to
		 * checkpoint.
		 *
		 * @param record A record at which to checkpoint in this shard. Upon failover,
		 *        the Kinesis Client Library will start fetching records after this record's sequence number.
		 * @error ThrottlingError Can't store checkpoint. Can be caused by checkpointing too frequently.
		 *         Consider increasing the throughput/capacity of the checkpoint store or reducing checkpoint frequency.
		 * @error ShutdownError The record processor instance has been shutdown. Another instance may have
		 *         started processing some of these records already.
		 *         The application should abort processing via this RecordProcessor instance.
		 * @error InvalidStateError Can't store checkpoint.
		 *         Unable to store the checkpoint in the DynamoDB table (e.g. table doesn't exist).
		 * @error KinesisClientLibDependencyError Encountered an issue when storing the checkpoint. The application can
		 *         backoff and retry.
		 */
		checkpointByRecord(record *ks.Record) error

		/**
		 * This method will checkpoint the progress at the provided sequenceNumber. This method is analogous to
		 * {@link #checkpoint()} but provides the ability to specify the sequence number at which to
		 * checkpoint.
		 *
		 * @param sequenceNumber A sequence number at which to checkpoint in this shard. Upon failover,
		 *        the Kinesis Client Library will start fetching records after this sequence number.
		 * @error ThrottlingError Can't store checkpoint. Can be caused by checkpointing too frequently.
		 *         Consider increasing the throughput/capacity of the checkpoint store or reducing checkpoint frequency.
		 * @error ShutdownError The record processor instance has been shutdown. Another instance may have
		 *         started processing some of these records already.
		 *         The application should abort processing via this RecordProcessor instance.
		 * @error InvalidStateError Can't store checkpoint.
		 *         Unable to store the checkpoint in the DynamoDB table (e.g. table doesn't exist).
		 * @error KinesisClientLibDependencyError Encountered an issue when storing the checkpoint. The application can
		 *         backoff and retry.
		 * @error IllegalArgumentError The sequence number is invalid for one of the following reasons:
		 *         1.) It appears to be out of range, i.e. it is smaller than the last check point value, or larger than the
		 *         greatest sequence number seen by the associated record processor.
		 *         2.) It is not a valid sequence number for a record in this shard.
		 */
		checkpointBySequenceNumber(sequenceNumber string) error

		/**
		 * This method will checkpoint the progress at the provided sequenceNumber and subSequenceNumber, the latter for
		 * aggregated records produced with the Producer Library. This method is analogous to {@link #checkpoint()}
		 * but provides the ability to specify the sequence and subsequence numbers at which to checkpoint.
		 *
		 * @param sequenceNumber A sequence number at which to checkpoint in this shard. Upon failover, the Kinesis
		 *        Client Library will start fetching records after the given sequence and subsequence numbers.
		 * @param subSequenceNumber A subsequence number at which to checkpoint within this shard. Upon failover, the
		 *        Kinesis Client Library will start fetching records after the given sequence and subsequence numbers.
		 * @error ThrottlingError Can't store checkpoint. Can be caused by checkpointing too frequently.
		 *         Consider increasing the throughput/capacity of the checkpoint store or reducing checkpoint frequency.
		 * @error ShutdownError The record processor instance has been shutdown. Another instance may have
		 *         started processing some of these records already.
		 *         The application should abort processing via this RecordProcessor instance.
		 * @error InvalidStateError Can't store checkpoint.
		 *         Unable to store the checkpoint in the DynamoDB table (e.g. table doesn't exist).
		 * @error KinesisClientLibDependencyError Encountered an issue when storing the checkpoint. The application can
		 *         backoff and retry.
		 * @error IllegalArgumentError The sequence number is invalid for one of the following reasons:
		 *         1.) It appears to be out of range, i.e. it is smaller than the last check point value, or larger than the
		 *         greatest sequence number seen by the associated record processor.
		 *         2.) It is not a valid sequence number for a record in this shard.
		 */
		checkpointBySequenceNumberEx(sequenceNumber string, subSequenceNumber int64) error

		/**
		 * This method will record a pending checkpoint at the last data record that was delivered to the record processor.
		 * If the application fails over between calling prepareCheckpoint() and checkpoint(), the init() method of the next
		 * IRecordProcessor for this shard will be informed of the prepared sequence number
		 *
		 * Application should use this to assist with idempotency across failover by calling prepareCheckpoint before having
		 * side effects, then by calling checkpoint on the returned PreparedCheckpointer after side effects are complete.
		 * Use the sequence number passed in to init() to behave idempotently.
		 *
		 * @return an IPreparedCheckpointer object that can be called later to persist the checkpoint.
		 *
		 * @error ThrottlingError Can't store pending checkpoint. Can be caused by checkpointing too frequently.
		 *         Consider increasing the throughput/capacity of the checkpoint store or reducing checkpoint frequency.
		 * @error ShutdownError The record processor instance has been shutdown. Another instance may have
		 *         started processing some of these records already.
		 *         The application should abort processing via this RecordProcessor instance.
		 * @error InvalidStateError Can't store pending checkpoint.
		 *         Unable to store the checkpoint in the DynamoDB table (e.g. table doesn't exist).
		 * @error KinesisClientLibDependencyError Encountered an issue when storing the pending checkpoint. The
		 *         application can backoff and retry.
		 */
		prepareCheckpoint() (*IPreparedCheckpointer, error)

		/**
		 * This method will record a pending checkpoint at the at the provided record. This method is analogous to
		 * {@link #prepareCheckpoint()} but provides the ability to specify the record at which to prepare the checkpoint.
		 *
		 * @param record A record at which to prepare checkpoint in this shard.
		 *
		 * Application should use this to assist with idempotency across failover by calling prepareCheckpoint before having
		 * side effects, then by calling checkpoint on the returned PreparedCheckpointer after side effects are complete.
		 * Use the sequence number and application state passed in to init() to behave idempotently.
		 *
		 * @return an IPreparedCheckpointer object that can be called later to persist the checkpoint.
		 *
		 * @error ThrottlingError Can't store pending checkpoint. Can be caused by checkpointing too frequently.
		 *         Consider increasing the throughput/capacity of the checkpoint store or reducing checkpoint frequency.
		 * @error ShutdownError The record processor instance has been shutdown. Another instance may have
		 *         started processing some of these records already.
		 *         The application should abort processing via this RecordProcessor instance.
		 * @error InvalidStateError Can't store pending checkpoint.
		 *         Unable to store the checkpoint in the DynamoDB table (e.g. table doesn't exist).
		 * @error KinesisClientLibDependencyError Encountered an issue when storing the pending checkpoint. The
		 *         application can backoff and retry.
		 * @error IllegalArgumentError The sequence number is invalid for one of the following reasons:
		 *         1.) It appears to be out of range, i.e. it is smaller than the last check point value, or larger than the
		 *         greatest sequence number seen by the associated record processor.
		 *         2.) It is not a valid sequence number for a record in this shard.
		 */
		prepareCheckpointByRecord(record *ks.Record) (*IPreparedCheckpointer, error)

		/**
		 * This method will record a pending checkpoint at the provided sequenceNumber. This method is analogous to
		 * {@link #prepareCheckpoint()} but provides the ability to specify the sequence number at which to checkpoint.
		 *
		 * @param sequenceNumber A sequence number at which to prepare checkpoint in this shard.

		 * @return an IPreparedCheckpointer object that can be called later to persist the checkpoint.
		 *
		 * @error ThrottlingError Can't store pending checkpoint. Can be caused by checkpointing too frequently.
		 *         Consider increasing the throughput/capacity of the checkpoint store or reducing checkpoint frequency.
		 * @error ShutdownError The record processor instance has been shutdown. Another instance may have
		 *         started processing some of these records already.
		 *         The application should abort processing via this RecordProcessor instance.
		 * @error InvalidStateError Can't store pending checkpoint.
		 *         Unable to store the checkpoint in the DynamoDB table (e.g. table doesn't exist).
		 * @error KinesisClientLibDependencyError Encountered an issue when storing the pending checkpoint. The
		 *         application can backoff and retry.
		 * @error IllegalArgumentError The sequence number is invalid for one of the following reasons:
		 *         1.) It appears to be out of range, i.e. it is smaller than the last check point value, or larger than the
		 *         greatest sequence number seen by the associated record processor.
		 *         2.) It is not a valid sequence number for a record in this shard.
		 */
		prepareCheckpointBySequenceNumber(sequenceNumber string) (*IPreparedCheckpointer, error)

		/**
		 * This method will record a pending checkpoint at the provided sequenceNumber and subSequenceNumber, the latter for
		 * aggregated records produced with the Producer Library. This method is analogous to  {@link #prepareCheckpoint()}
		 * but provides the ability to specify the sequence number at which to checkpoint
		 *
		 * @param sequenceNumber A sequence number at which to prepare checkpoint in this shard.
		 * @param subSequenceNumber A subsequence number at which to prepare checkpoint within this shard.
		 *
		 * @return an IPreparedCheckpointer object that can be called later to persist the checkpoint.
		 *
		 * @error ThrottlingError Can't store pending checkpoint. Can be caused by checkpointing too frequently.
		 *         Consider increasing the throughput/capacity of the checkpoint store or reducing checkpoint frequency.
		 * @error ShutdownError The record processor instance has been shutdown. Another instance may have
		 *         started processing some of these records already.
		 *         The application should abort processing via this RecordProcessor instance.
		 * @error InvalidStateError Can't store pending checkpoint.
		 *         Unable to store the checkpoint in the DynamoDB table (e.g. table doesn't exist).
		 * @error KinesisClientLibDependencyError Encountered an issue when storing the pending checkpoint. The
		 *         application can backoff and retry.
		 * @error IllegalArgumentError The sequence number is invalid for one of the following reasons:
		 *         1.) It appears to be out of range, i.e. it is smaller than the last check point value, or larger than the
		 *         greatest sequence number seen by the associated record processor.
		 *         2.) It is not a valid sequence number for a record in this shard.
		 */
		prepareCheckpointBySequenceNumberEx(sequenceNumber string, subSequenceNumber int64) (*IPreparedCheckpointer, error)
	}
)
