package interfaces

// LeaseTable hold current lease mapping shardId --> Lease
type LeaseTable map[string]*ILease

/**
 * ILeaseRenewer objects are used by LeaseCoordinator to renew leases held by the LeaseCoordinator. Each
 * LeaseCoordinator instance corresponds to one worker, and uses exactly one ILeaseRenewer to manage lease renewal for
 * that worker.
 */
type ILeaseRenewer interface {

	/**
	 * Bootstrap initial set of leases from the LeaseManager (e.g. upon process restart, pick up leases we own)
	 * @error LeasingDependencyError on unexpected DynamoDB failures
	 * @error LeasingInvalidStateError if lease table doesn't exist
	 * @error ProvisionedThroughputError if DynamoDB reads fail due to insufficient capacity
	 */
	Initialize() error

	/**
	 * Attempt to renew all currently held leases.
	 *
	 * @error LeasingDependencyError on unexpected DynamoDB failures
	 * @error LeasingInvalidStateError if lease table does not exist
	 */
	RenewLeases() error

	/**
	 * @return currently held leases. Key is shardId, value is corresponding Lease object. A lease is currently held if
	 *         we successfully renewed it on the last run of renewLeases(). Lease objects returned are deep copies -
	 *         their lease counters will not tick.
	 */
	GetCurrentlyHeldLeases() *LeaseTable

	/**
	 * @param leaseKey key of the lease to retrieve
	 *
	 * @return a deep copy of a currently held lease, or null if we don't hold the lease
	 */
	GetCurrentlyHeldLease(leaseKey string) *ILease

	/**
	 * Adds leases to this LeaseRenewer's set of currently held leases. Leases must have lastRenewalNanos set to the
	 * last time the lease counter was incremented before being passed to this method.
	 *
	 * @param newLeases new leases.
	 */
	AddLeasesToRenew(newLeases []ILease)

	/**
	 * Clears this LeaseRenewer's set of currently held leases.
	 */
	ClearCurrentlyHeldLeases()

	/**
	 * Stops the lease renewer from continunig to maintain the given lease.
	 *
	 * @param lease the lease to drop.
	 */
	DropLease(lease ILease)

	/**
	 * Update application-specific fields in a currently held lease. Cannot be used to update internal fields such as
	 * leaseCounter, leaseOwner, etc. Fails if we do not hold the lease, or if the concurrency token does not match
	 * the concurrency token on the internal authoritative copy of the lease (ie, if we lost and re-acquired the lease).
	 *
	 * @param lease lease object containing updated data
	 * @param concurrencyToken obtained by calling Lease.getConcurrencyToken for a currently held lease
	 *
	 * @return true if update succeeds, false otherwise
	 *
	 * @error LeasingInvalidStateError if lease table does not exist
	 * @error ProvisionedThroughputError if DynamoDB update fails due to lack of capacity
	 * @error LeasingDependencyError if DynamoDB update fails in an unexpected way
	 */
	UpdateLease(lease ILease, concurrencyToken string) (bool, error)
}
