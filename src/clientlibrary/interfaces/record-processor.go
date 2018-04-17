package interfaces

type (
	// IRecordProcessor is the interface for some callback functions invoked by KCL will
	// The main task of using KCL is to provide implementation on IRecordProcessor interface.
	// Note: This is exactly the same interface as Amazon KCL IRecordProcessor v2
	IRecordProcessor interface {
		/**
		 * Invoked by the Amazon Kinesis Client Library before data records are delivered to the RecordProcessor instance
		 * (via processRecords).
		 *
		 * @param initializationInput Provides information related to initialization
		 */
		Initialize(initializationInput *InitializationInput)

		/**
		 * Process data records. The Amazon Kinesis Client Library will invoke this method to deliver data records to the
		 * application.
		 * Upon fail over, the new instance will get records with sequence number > checkpoint position
		 * for each partition key.
		 *
		 * @param processRecordsInput Provides the records to be processed as well as information and capabilities related
		 *        to them (eg checkpointing).
		 */
		ProcessRecords(processRecordsInput *ProcessRecordsInput)

		/**
		 * Invoked by the Amazon Kinesis Client Library to indicate it will no longer send data records to this
		 * RecordProcessor instance.
		 *
		 * <h2><b>Warning</b></h2>
		 *
		 * When the value of {@link ShutdownInput#getShutdownReason()} is
		 * {@link com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason#TERMINATE} it is required that you
		 * checkpoint. Failure to do so will result in an IllegalArgumentException, and the KCL no longer making progress.
		 *
		 * @param shutdownInput
		 *            Provides information and capabilities (eg checkpointing) related to shutdown of this record processor.
		 */
		Shutdown(shutdownInput *ShutdownInput)
	}

	// IRecordProcessorFactory is interface for creating IRecordProcessor. Each Worker can have multiple threads
	// for processing shard. Client can choose either creating one processor per shard or sharing them.
	IRecordProcessorFactory interface {

		/**
		 * Returns a record processor to be used for processing data records for a (assigned) shard.
		 *
		 * @return Returns a processor object.
		 */
		CreateProcessor() IRecordProcessor
	}
)
