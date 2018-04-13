package interfaces

// ILease is the interface for all Leases
type ILease interface {
	GetLeaseKey() string
	SetLeaseKey(leaseKey string) error

	GetLeaseOwner() string
	SetLeaseOwner(leaseOwner string)

	GetLeaseCounter() int64
	SetLeaseCounter(leaseCounter int64)

	GetConcurrencyToken() string
	SetConcurrencyToken(concurrencyToken string)

	GetLastCounterIncrementNanos() int64
	SetLastCounterIncrementNanos(lastCounterIncrementNanos int64)

	IsExpired(leaseDurationNanos, asOfNanos int64) bool
}
