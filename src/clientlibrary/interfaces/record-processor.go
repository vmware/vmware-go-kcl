package interfaces

// IRecordProcessor is the interface for some callback functions invoked by KCL will
// The main task of using KCL is to provide implementation on IRecordProcessor interface.
// Note: This is exactly the same interface as Amazon KCL IRecordProcessor v2
type IRecordProcessor interface {
	/**
	 * Invoked by the Amazon Kinesis Client Library before data records are delivered to the RecordProcessor instance
	 * (via processRecords).
	 *
	 * @param initializationInput Provides information related to initialization
	 */
	initialize(initializationInput InitializationInput)

	/**
	 * Process data records. The Amazon Kinesis Client Library will invoke this method to deliver data records to the
	 * application.
	 * Upon fail over, the new instance will get records with sequence number > checkpoint position
	 * for each partition key.
	 *
	 * @param processRecordsInput Provides the records to be processed as well as information and capabilities related
	 *        to them (eg checkpointing).
	 */
	processRecords(processRecordsInput ProcessRecordsInput)

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
	shutdown(shutdownInput ShutdownInput)
}
