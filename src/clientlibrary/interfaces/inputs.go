package interfaces

import (
	"time"

	ks "github.com/aws/aws-sdk-go/service/kinesis"
)

const (
	REQUESTED = ShutdownReason(1)
	TERMINATE = REQUESTED + 1
	ZOMBIE    = TERMINATE + 1
)

// Containers for the parameters to the IRecordProcessor
type (
	ShutdownReason int

	InitializationInput struct {
		shardId                         string
		extendedSequenceNumber          *ExtendedSequenceNumber
		pendingCheckpointSequenceNumber *ExtendedSequenceNumber
	}

	ProcessRecordsInput struct {
		cacheEntryTime     *time.Time
		cacheExitTime      *time.Time
		records            []*ks.Record
		checkpointer       *IRecordProcessorCheckpointer
		millisBehindLatest int64
	}

	ShutdownInput struct {
		shutdownReason ShutdownReason
		checkpointer   *IRecordProcessorCheckpointer
	}
)
