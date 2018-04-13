package interfaces

/**
 * ILeaseTaker is used by LeaseCoordinator to take new leases, or leases that other workers fail to renew. Each
 * LeaseCoordinator instance corresponds to one worker and uses exactly one ILeaseTaker to take leases for that worker.
 */
type ILeaseTaker interface {

	/**
	 * Compute the set of leases available to be taken and attempt to take them. Lease taking rules are:
	 *
	 * 1) If a lease's counter hasn't changed in long enough, try to take it.
	 * 2) If we see a lease we've never seen before, take it only if owner == null. If it's owned, odds are the owner is
	 * holding it. We can't tell until we see it more than once.
	 * 3) For load balancing purposes, you may violate rules 1 and 2 for EXACTLY ONE lease per call of takeLeases().
	 *
	 * @return map of shardId to Lease object for leases we just successfully took.
	 *
	 * @error LeasingDependencyError on unexpected DynamoDB failures
	 * @error LeasingInvalidStateError if lease table does not exist
	 */
	TakeLeases() map[string]ILease

	/**
	 * @return workerIdentifier for this LeaseTaker
	 */
	GetWorkerIdentifier() string
}
