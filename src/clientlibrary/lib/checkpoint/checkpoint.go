package checkpoint

import (
	. "clientlibrary/interfaces"
)

const (
	// TRIM_HORIZON starts from the first available record in the shard.
	TRIM_HORIZON = SentinelCheckpoint(iota + 1)
	// LATEST starts from the latest record in the shard.
	LATEST
	// SHARD_END We've completely processed all records in this shard.
	SHARD_END
	// AT_TIMESTAMP starts from the record at or after the specified server-side timestamp.
	AT_TIMESTAMP
)

type (
	SentinelCheckpoint int

	// Checkpoint: a class encapsulating the 2 pieces of state stored in a checkpoint.
	Checkpoint struct {
		checkpoint        *ExtendedSequenceNumber
		pendingCheckpoint *ExtendedSequenceNumber
	}
)
