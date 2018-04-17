package worker

import (
	"github.com/aws/aws-sdk-go/aws"

	kcl "clientlibrary/interfaces"
)

type (

	/* Objects of this class are prepared to checkpoint at a specific sequence number. They use an
	 * IRecordProcessorCheckpointer to do the actual checkpointing, so their checkpoint is subject to the same 'didn't go
	 * backwards' validation as a normal checkpoint.
	 */
	PreparedCheckpointer struct {
		pendingCheckpointSequenceNumber *kcl.ExtendedSequenceNumber
		checkpointer                    kcl.IRecordProcessorCheckpointer
	}

	/**
	 * This class is used to enable RecordProcessors to checkpoint their progress.
	 * The Amazon Kinesis Client Library will instantiate an object and provide a reference to the application
	 * RecordProcessor instance. Amazon Kinesis Client Library will create one instance per shard assignment.
	 */
	RecordProcessorCheckpointer struct {
		shard      *shardStatus
		checkpoint Checkpointer
	}
)

func NewRecordProcessorCheckpoint(shard *shardStatus, checkpoint Checkpointer) kcl.IRecordProcessorCheckpointer {
	return &RecordProcessorCheckpointer{
		shard:      shard,
		checkpoint: checkpoint,
	}
}

func (pc *PreparedCheckpointer) GetPendingCheckpoint() *kcl.ExtendedSequenceNumber {
	return pc.pendingCheckpointSequenceNumber
}

func (pc *PreparedCheckpointer) Checkpoint() error {
	return pc.checkpointer.Checkpoint(pc.pendingCheckpointSequenceNumber.SequenceNumber)
}

func (rc *RecordProcessorCheckpointer) Checkpoint(sequenceNumber *string) error {
	rc.shard.mux.Lock()
	rc.shard.Checkpoint = aws.StringValue(sequenceNumber)
	rc.shard.mux.Unlock()
	return rc.checkpoint.CheckpointSequence(rc.shard)
}

func (rc *RecordProcessorCheckpointer) PrepareCheckpoint(sequenceNumber *string) (kcl.IPreparedCheckpointer, error) {
	return &PreparedCheckpointer{}, nil

}
