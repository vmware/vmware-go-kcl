package impl

import (
	. "clientlibrary/types"
)

// KinesisClientLease is a Lease subclass containing KinesisClientLibrary related fields for checkpoints.
type KinesisClientLease struct {
	checkpoint                   *ExtendedSequenceNumber
	pendingCheckpoint            *ExtendedSequenceNumber
	ownerSwitchesSinceCheckpoint int64
	parentShardIds               *[]string

	// coreLease to hold lease information
	// Note: golang doesn't support inheritance, use composition instead.
	coreLease Lease
}

// GetCheckpoint returns most recently application-supplied checkpoint value. During fail over, the new worker
// will pick up after the old worker's last checkpoint.
func (l *KinesisClientLease) GetCheckpoint() *ExtendedSequenceNumber {
	return l.checkpoint
}

// GetPendingCheckpoint returns pending checkpoint, possibly null.
func (l *KinesisClientLease) GetPendingCheckpoint() *ExtendedSequenceNumber {
	return l.pendingCheckpoint
}

// GetOwnerSwitchesSinceCheckpoint counts of distinct lease holders between checkpoints.
func (l *KinesisClientLease) GetOwnerSwitchesSinceCheckpoint() int64 {
	return l.ownerSwitchesSinceCheckpoint
}

// GetParentShardIds returns shardIds that parent this lease. Used for resharding.
func (l *KinesisClientLease) GetParentShardIds() *[]string {
	return l.parentShardIds
}

// SetCheckpoint
func (l *KinesisClientLease) SetCheckpoint(checkpoint *ExtendedSequenceNumber) {
	l.checkpoint = checkpoint
}

// SetPendingCheckpoint
func (l *KinesisClientLease) SetPendingCheckpoint(pendingCheckpoint *ExtendedSequenceNumber) {
	l.pendingCheckpoint = pendingCheckpoint
}

// SetOwnerSwitchesSinceCheckpoint
func (l *KinesisClientLease) SetOwnerSwitchesSinceCheckpoint(ownerSwitchesSinceCheckpoint int64) {
	l.ownerSwitchesSinceCheckpoint = ownerSwitchesSinceCheckpoint
}

// SetParentShardIds
func (l *KinesisClientLease) SetParentShardIds(parentShardIds *[]string) {
	l.parentShardIds = parentShardIds
}

// GetLeaseKey retrieves leaseKey - identifies the unit of work associated with this lease.
func (l *KinesisClientLease) GetLeaseKey() string {
	return l.coreLease.GetLeaseKey()
}

// GetLeaseOwner gets current owner of the lease, may be "".
func (l *KinesisClientLease) GetLeaseOwner() string {
	return l.coreLease.GetLeaseOwner()
}

// GetLeaseCounter retrieves leaseCounter which is incremented periodically by the holder of the lease. Used for optimistic locking.
func (l *KinesisClientLease) GetLeaseCounter() int64 {
	return l.coreLease.GetLeaseCounter()
}

// GetConcurrencyToken returns concurrency token
func (l *KinesisClientLease) GetConcurrencyToken() string {
	return l.coreLease.GetConcurrencyToken()
}

// GetLastCounterIncrementNanos returns concurrency token
func (l *KinesisClientLease) GetLastCounterIncrementNanos() int64 {
	return l.coreLease.GetLastCounterIncrementNanos()
}

// SetLeaseKey sets leaseKey - LeaseKey is immutable once set.
func (l *KinesisClientLease) SetLeaseKey(leaseKey string) error {
	return l.coreLease.SetLeaseKey(leaseKey)
}

// SetLeaseOwner set current owner of the lease, may be "".
func (l *KinesisClientLease) SetLeaseOwner(leaseOwner string) {
	l.coreLease.SetLeaseOwner(leaseOwner)
}

// SetLeaseCounter sets leaseCounter which is incremented periodically by the holder of the lease. Used for optimistic locking.
func (l *KinesisClientLease) SetLeaseCounter(leaseCounter int64) {
	l.coreLease.SetLeaseCounter(leaseCounter)
}

// SetConcurrencyToken
func (l *KinesisClientLease) SetConcurrencyToken(concurrencyToken string) {
	l.coreLease.SetConcurrencyToken(concurrencyToken)
}

// SetLastCounterIncrementNanos returns concurrency token
func (l *KinesisClientLease) SetLastCounterIncrementNanos(lastCounterIncrementNanos int64) {
	l.coreLease.SetLastCounterIncrementNanos(lastCounterIncrementNanos)
}

// IsExpired to check whether lease expired using
// @param leaseDurationNanos duration of lease in nanoseconds
// @param asOfNanos time in nanoseconds to check expiration as-of
// @return true if lease is expired as-of given time, false otherwise
func (l *KinesisClientLease) IsExpired(leaseDurationNanos, asOfNanos int64) bool {
	return l.coreLease.IsExpired(leaseDurationNanos, asOfNanos)
}
