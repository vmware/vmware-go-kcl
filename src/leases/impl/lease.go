package impl

import (
	cc "clientlibrary/common"
	"time"
)

const (
	// We will consider leases to be expired if they are more than 90 days.
	MAX_ABS_AGE_NANOS = int64(90 * 24 * time.Hour)
)

// Lease structure contains data pertaining to a Lease. Distributed systems may use leases to partition work across a
// fleet of workers. Each unit of work (identified by a leaseKey) has a corresponding Lease. Every worker will contend
// for all leases - only one worker will successfully take each one. The worker should hold the lease until it is ready to stop
// processing the corresponding unit of work, or until it fails. When the worker stops holding the lease, another worker will
// take and hold the lease.
type Lease struct {
	// shard-id
	leaseKey string
	// worker#
	leaseOwner string
	// ccounter incremented periodically
	leaseCounter int64

	// This field is used to prevent updates to leases that we have lost and re-acquired. It is deliberately not
	// persisted in DynamoDB and excluded from hashCode and equals.
	concurrencyToken string

	// This field is used by LeaseRenewer and LeaseTaker to track the last time a lease counter was incremented. It is
	// deliberately not persisted in DynamoDB and excluded from hashCode and equals.
	lastCounterIncrementNanos int64
}

// CloneLease to clone a lease object
func CopyLease(lease *Lease) *Lease {
	return &Lease{
		leaseKey:                  lease.leaseKey,
		leaseOwner:                lease.leaseOwner,
		leaseCounter:              lease.leaseCounter,
		concurrencyToken:          lease.concurrencyToken,
		lastCounterIncrementNanos: lease.lastCounterIncrementNanos,
	}
}

// GetLeaseKey retrieves leaseKey - identifies the unit of work associated with this lease.
func (l *Lease) GetLeaseKey() string {
	return l.leaseKey
}

// GetLeaseOwner gets current owner of the lease, may be "".
func (l *Lease) GetLeaseOwner() string {
	return l.leaseOwner
}

// GetLeaseCounter retrieves leaseCounter which is incremented periodically by the holder of the lease. Used for optimistic locking.
func (l *Lease) GetLeaseCounter() int64 {
	return l.leaseCounter
}

// GetConcurrencyToken returns concurrency token
func (l *Lease) GetConcurrencyToken() string {
	return l.concurrencyToken
}

// GetLastCounterIncrementNanos returns concurrency token
func (l *Lease) GetLastCounterIncrementNanos() int64 {
	return l.lastCounterIncrementNanos
}

// SetLeaseKey sets leaseKey - LeaseKey is immutable once set.
func (l *Lease) SetLeaseKey(leaseKey string) error {
	if len(l.leaseKey) > 0 {
		return cc.IllegalArgumentError.MakeErr().WithDetail("LeaseKey is immutable once set")
	}

	l.leaseKey = leaseKey
	return nil
}

// SetLeaseOwner set current owner of the lease, may be "".
func (l *Lease) SetLeaseOwner(leaseOwner string) {
	l.leaseOwner = leaseOwner
}

// SetLeaseCounter sets leaseCounter which is incremented periodically by the holder of the lease. Used for optimistic locking.
func (l *Lease) SetLeaseCounter(leaseCounter int64) {
	l.leaseCounter = leaseCounter
}

// SetConcurrencyToken
func (l *Lease) SetConcurrencyToken(concurrencyToken string) {
	l.concurrencyToken = concurrencyToken
}

// SetLastCounterIncrementNanos returns concurrency token
func (l *Lease) SetLastCounterIncrementNanos(lastCounterIncrementNanos int64) {
	l.lastCounterIncrementNanos = lastCounterIncrementNanos
}

// IsExpired to check whether lease expired using
// @param leaseDurationNanos duration of lease in nanoseconds
// @param asOfNanos time in nanoseconds to check expiration as-of
// @return true if lease is expired as-of given time, false otherwise
func (l *Lease) IsExpired(leaseDurationNanos, asOfNanos int64) bool {
	if l.lastCounterIncrementNanos == 0 {
		return true
	}

	age := asOfNanos - l.lastCounterIncrementNanos
	if age > MAX_ABS_AGE_NANOS {
		return true
	} else {
		return age > leaseDurationNanos
	}
}
