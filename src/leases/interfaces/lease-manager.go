package interfaces

// ILeaseManager supports basic CRUD operations for Leases.
type ILeaseManager interface {

	/**
	 * Creates the table that will store leases. Succeeds if table already exists.
	 *
	 * @param readCapacity
	 * @param writeCapacity
	 *
	 * @return true if we created a new table (table didn't exist before)
	 *
	 * @error ProvisionedThroughputError if we cannot create the lease table due to per-AWS-account capacity
	 *         restrictions.
	 * @error LeasingDependencyError if DynamoDB createTable fails in an unexpected way
	 */
	CreateLeaseTableIfNotExists(readCapacity, writeCapacity int64) (bool, error)

	/**
	 * @return true if the lease table already exists.
	 *
	 * @error LeasingDependencyError if DynamoDB describeTable fails in an unexpected way
	 */
	LeaseTableExists() (bool, error)

	/**
	 * Blocks until the lease table exists by polling leaseTableExists.
	 *
	 * @param secondsBetweenPolls time to wait between polls in seconds
	 * @param timeoutSeconds total time to wait in seconds
	 *
	 * @return true if table exists, false if timeout was reached
	 *
	 * @error LeasingDependencyError if DynamoDB describeTable fails in an unexpected way
	 */
	WaitUntilLeaseTableExists(secondsBetweenPolls, timeoutSeconds int64) (bool, error)

	/**
	 * List all objects in table synchronously.
	 *
	 * @error LeasingDependencyError if DynamoDB scan fails in an unexpected way
	 * @error LeasingInvalidStateError if lease table does not exist
	 * @error ProvisionedThroughputError if DynamoDB scan fails due to lack of capacity
	 *
	 * @return list of leases
	 */
	ListLeases() ([]ILease, error)

	/**
	 * Create a new lease. Conditional on a lease not already existing with this shardId.
	 *
	 * @param lease the lease to create
	 *
	 * @return true if lease was created, false if lease already exists
	 *
	 * @error LeasingDependencyError if DynamoDB put fails in an unexpected way
	 * @error LeasingInvalidStateError if lease table does not exist
	 * @error ProvisionedThroughputError if DynamoDB put fails due to lack of capacity
	 */
	CreateLeaseIfNotExists(lease ILease) (bool, error)

	/**
	 * @param shardId Get the lease for this shardId
	 *
	 * @error LeasingInvalidStateError if lease table does not exist
	 * @error ProvisionedThroughputError if DynamoDB get fails due to lack of capacity
	 * @error LeasingDependencyError if DynamoDB get fails in an unexpected way
	 *
	 * @return lease for the specified shardId, or null if one doesn't exist
	 */
	GetLease(shardId string) (ILease, error)

	/**
	 * Renew a lease by incrementing the lease counter. Conditional on the leaseCounter in DynamoDB matching the leaseCounter
	 * of the input. Mutates the leaseCounter of the passed-in lease object after updating the record in DynamoDB.
	 *
	 * @param lease the lease to renew
	 *
	 * @return true if renewal succeeded, false otherwise
	 *
	 * @error LeasingInvalidStateError if lease table does not exist
	 * @error ProvisionedThroughputError if DynamoDB update fails due to lack of capacity
	 * @error LeasingDependencyError if DynamoDB update fails in an unexpected way
	 */
	RenewLease(lease ILease) (bool, error)

	/**
	 * Take a lease for the given owner by incrementing its leaseCounter and setting its owner field. Conditional on
	 * the leaseCounter in DynamoDB matching the leaseCounter of the input. Mutates the leaseCounter and owner of the
	 * passed-in lease object after updating DynamoDB.
	 *
	 * @param lease the lease to take
	 * @param owner the new owner
	 *
	 * @return true if lease was successfully taken, false otherwise
	 *
	 * @error LeasingInvalidStateError if lease table does not exist
	 * @error ProvisionedThroughputError if DynamoDB update fails due to lack of capacity
	 * @error LeasingDependencyError if DynamoDB update fails in an unexpected way
	 */
	TakeLease(lease ILease, owner string) (bool, error)

	/**
	 * Evict the current owner of lease by setting owner to null. Conditional on the owner in DynamoDB matching the owner of
	 * the input. Mutates the lease counter and owner of the passed-in lease object after updating the record in DynamoDB.
	 *
	 * @param lease the lease to void
	 *
	 * @return true if eviction succeeded, false otherwise
	 *
	 * @error LeasingInvalidStateError if lease table does not exist
	 * @error ProvisionedThroughputError if DynamoDB update fails due to lack of capacity
	 * @error LeasingDependencyError if DynamoDB update fails in an unexpected way
	 */
	EvictLease(lease ILease) (bool, error)

	/**
	 * Delete the given lease from DynamoDB. Does nothing when passed a lease that does not exist in DynamoDB.
	 *
	 * @param lease the lease to delete
	 *
	 * @error LeasingInvalidStateError if lease table does not exist
	 * @error ProvisionedThroughputError if DynamoDB delete fails due to lack of capacity
	 * @error LeasingDependencyError if DynamoDB delete fails in an unexpected way
	 */
	DeleteLease(lease ILease) error

	/**
	 * Delete all leases from DynamoDB. Useful for tools/utils and testing.
	 *
	 * @error LeasingInvalidStateError if lease table does not exist
	 * @error ProvisionedThroughputError if DynamoDB scan or delete fail due to lack of capacity
	 * @error LeasingDependencyError if DynamoDB scan or delete fail in an unexpected way
	 */
	DeleteAll() error

	/**
	 * Update application-specific fields of the given lease in DynamoDB. Does not update fields managed by the leasing
	 * library such as leaseCounter, leaseOwner, or leaseKey. Conditional on the leaseCounter in DynamoDB matching the
	 * leaseCounter of the input. Increments the lease counter in DynamoDB so that updates can be contingent on other
	 * updates. Mutates the lease counter of the passed-in lease object.
	 *
	 * @return true if update succeeded, false otherwise
	 *
	 * @error LeasingInvalidStateError if lease table does not exist
	 * @error ProvisionedThroughputError if DynamoDB update fails due to lack of capacity
	 * @error LeasingDependencyError if DynamoDB update fails in an unexpected way
	 */
	UpdateLease(lease ILease) (bool, error)

	/**
	 * Check (synchronously) if there are any leases in the lease table.
	 *
	 * @return true if there are no leases in the lease table
	 *
	 * @error LeasingDependencyError if DynamoDB scan fails in an unexpected way
	 * @error LeasingInvalidStateError if lease table does not exist
	 * @error ProvisionedThroughputError if DynamoDB scan fails due to lack of capacity
	 */
	IsLeaseTableEmpty() (bool, error)
}
