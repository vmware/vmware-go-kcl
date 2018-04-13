package impl

import (
	"log"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"

	. "leases/interfaces"
)

const (
	// CREATING - The table is being created.
	TABLE_CREATING = "CREATING"

	// UPDATING - The table is being updated.
	TABLE_UPDATING = "UPDATING"

	// DELETING - The table is being deleted.
	TABLE_DELETING = "DELETING"

	// ACTIVE - The table is ready for use.
	TABLE_ACTIVE = "ACTIVE"
)

// LeaseManager is an implementation of ILeaseManager that uses DynamoDB.
type LeaseManager struct {
	tableName       string
	dynamoDBClient  dynamodbiface.DynamoDBAPI
	serializer      ILeaseSerializer
	consistentReads bool
}

func NewLeaseManager(tableName string, dynamoDBClient dynamodbiface.DynamoDBAPI, serializer ILeaseSerializer) *LeaseManager {
	return &LeaseManager{
		tableName:       tableName,
		dynamoDBClient:  dynamoDBClient,
		serializer:      serializer,
		consistentReads: false,
	}
}

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
func (l *LeaseManager) CreateLeaseTableIfNotExists(readCapacity, writeCapacity int64) (bool, error) {
	status, _ := l.tableStatus()

	if status != nil {
		return false, nil
	}

	input := &dynamodb.CreateTableInput{
		AttributeDefinitions: l.serializer.GetAttributeDefinitions(),
		KeySchema:            l.serializer.GetKeySchema(),
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(readCapacity),
			WriteCapacityUnits: aws.Int64(writeCapacity),
		},
		TableName: aws.String(l.tableName),
	}
	_, err := l.dynamoDBClient.CreateTable(input)

	if err != nil {
		return false, err
	}
	return true, nil
}

/**
 * @return true if the lease table already exists.
 *
 * @error LeasingDependencyError if DynamoDB describeTable fails in an unexpected way
 */
func (l *LeaseManager) LeaseTableExists() (bool, error) {
	status, _ := l.tableStatus()

	if status != nil || aws.StringValue(status) == TABLE_ACTIVE {
		return true, nil
	}
	return false, nil
}

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
func (l *LeaseManager) WaitUntilLeaseTableExists(secondsBetweenPolls, timeoutSeconds int64) (bool, error) {
	delay := time.Duration(secondsBetweenPolls) * time.Second
	deadline := time.Now().Add(time.Duration(timeoutSeconds) * time.Second)

	var err error
	for time.Now().Before(deadline) {
		flag := false
		flag, err = l.LeaseTableExists()

		if flag {
			return true, nil
		}

		time.Sleep(delay)
	}

	return false, err
}

/**
 * List all objects in table synchronously.
 *
 * @error LeasingDependencyError if DynamoDB scan fails in an unexpected way
 * @error LeasingInvalidStateError if lease table does not exist
 * @error ProvisionedThroughputError if DynamoDB scan fails due to lack of capacity
 *
 * @return list of leases
 */
func (l *LeaseManager) ListLeases() ([]ILease, error) {
	return l.list(0)
}

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
func (l *LeaseManager) CreateLeaseIfNotExists(lease ILease) (bool, error) {
	input := &dynamodb.PutItemInput{
		TableName: aws.String(l.tableName),
		Item:      l.serializer.ToDynamoRecord(lease),
		Expected:  l.serializer.GetDynamoNonexistantExpectation(),
	}
	_, err := l.dynamoDBClient.PutItem(input)
	return err != nil, err
}

/**
 * @param shardId Get the lease for this shardId and it is the leaseKey
 *
 * @error LeasingInvalidStateError if lease table does not exist
 * @error ProvisionedThroughputError if DynamoDB get fails due to lack of capacity
 * @error LeasingDependencyError if DynamoDB get fails in an unexpected way
 *
 * @return lease for the specified shardId, or null if one doesn't exist
 */
func (l *LeaseManager) GetLease(shardId string) (ILease, error) {
	input := &dynamodb.GetItemInput{
		TableName:      aws.String(l.tableName),
		Key:            l.serializer.GetDynamoHashKey(shardId),
		ConsistentRead: aws.Bool(l.consistentReads),
	}
	result, err := l.dynamoDBClient.GetItem(input)
	if err != nil {
		return nil, err
	}
	dynamoRecord := result.Item
	if dynamoRecord == nil {
		return nil, nil
	}
	lease := l.serializer.FromDynamoRecord(dynamoRecord)
	return lease, nil
}

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
func (l *LeaseManager) RenewLease(lease ILease) (bool, error) {
	input := &dynamodb.UpdateItemInput{
		TableName: aws.String(l.tableName),
		Key:       l.serializer.GetDynamoHashKey(lease.GetLeaseKey()),
		Expected:  l.serializer.GetDynamoLeaseCounterExpectation(lease),
	}
	_, err := l.dynamoDBClient.UpdateItem(input)

	if err != nil {
		// If we had a spurious retry during the Dynamo update, then this conditional PUT failure
		// might be incorrect. So, we get the item straight away and check if the lease owner + lease counter
		// are what we expected.
		expectedOwner := lease.GetLeaseOwner()
		expectedCounter := lease.GetLeaseCounter() + 1
		updatedLease, _ := l.GetLease(lease.GetLeaseKey())
		if updatedLease == nil || expectedOwner != updatedLease.GetLeaseOwner() ||
			expectedCounter != updatedLease.GetLeaseCounter() {
			return false, nil
		}

		log.Println("Detected spurious renewal failure for lease with key " + lease.GetLeaseKey() + ", but recovered")
	}

	lease.SetLeaseCounter(lease.GetLeaseCounter() + 1)
	return err != nil, err

}

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
func (l *LeaseManager) TakeLease(lease ILease, owner string) (bool, error) {
	input := &dynamodb.UpdateItemInput{
		TableName: aws.String(l.tableName),
		Key:       l.serializer.GetDynamoHashKey(lease.GetLeaseKey()),
		Expected:  l.serializer.GetDynamoLeaseCounterExpectation(lease),
	}

	updates := l.serializer.GetDynamoLeaseCounterUpdate(lease)

	// putAll to updates
	for k, v := range l.serializer.GetDynamoTakeLeaseUpdate(lease, owner) {
		updates[k] = v
	}
	input.SetAttributeUpdates(updates)
	_, err := l.dynamoDBClient.UpdateItem(input)

	if err != nil {
		return false, err
	}

	lease.SetLeaseCounter(lease.GetLeaseCounter() + 1)
	lease.SetLeaseOwner(owner)
	return true, nil
}

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
func (l *LeaseManager) EvictLease(lease ILease) (bool, error) {
	input := &dynamodb.UpdateItemInput{
		TableName: aws.String(l.tableName),
		Key:       l.serializer.GetDynamoHashKey(lease.GetLeaseKey()),
		Expected:  l.serializer.GetDynamoLeaseCounterExpectation(lease),
	}

	updates := l.serializer.GetDynamoLeaseCounterUpdate(lease)

	// putAll to updates
	for k, v := range l.serializer.GetDynamoEvictLeaseUpdate(lease) {
		updates[k] = v
	}
	input.SetAttributeUpdates(updates)
	_, err := l.dynamoDBClient.UpdateItem(input)

	if err != nil {
		return false, err
	}

	lease.SetLeaseCounter(lease.GetLeaseCounter() + 1)
	lease.SetLeaseOwner("")
	return true, nil
}

/**
 * Delete the given lease from DynamoDB. Does nothing when passed a lease that does not exist in DynamoDB.
 *
 * @param lease the lease to delete
 *
 * @error LeasingInvalidStateError if lease table does not exist
 * @error ProvisionedThroughputError if DynamoDB delete fails due to lack of capacity
 * @error LeasingDependencyError if DynamoDB delete fails in an unexpected way
 */
func (l *LeaseManager) DeleteLease(lease ILease) error {
	input := &dynamodb.DeleteItemInput{
		TableName: aws.String(l.tableName),
		Key:       l.serializer.GetDynamoHashKey(lease.GetLeaseKey()),
	}
	_, err := l.dynamoDBClient.DeleteItem(input)
	return err
}

/**
 * Delete all leases from DynamoDB. Useful for tools/utils and testing.
 *
 * @error LeasingInvalidStateError if lease table does not exist
 * @error ProvisionedThroughputError if DynamoDB scan or delete fail due to lack of capacity
 * @error LeasingDependencyError if DynamoDB scan or delete fail in an unexpected way
 */
func (l *LeaseManager) DeleteAll() error {
	allLeases, err := l.ListLeases()
	if err != nil {
		return err
	}

	for _, v := range allLeases {
		err := l.DeleteLease(v)
		if err != nil {
			return err
		}
	}
	return nil
}

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
func (l *LeaseManager) UpdateLease(lease ILease) (bool, error) {
	input := &dynamodb.UpdateItemInput{
		TableName: aws.String(l.tableName),
		Key:       l.serializer.GetDynamoHashKey(lease.GetLeaseKey()),
		Expected:  l.serializer.GetDynamoLeaseCounterExpectation(lease),
	}

	updates := l.serializer.GetDynamoLeaseCounterUpdate(lease)

	// putAll to updates
	for k, v := range l.serializer.GetDynamoUpdateLeaseUpdate(lease) {
		updates[k] = v
	}
	input.SetAttributeUpdates(updates)
	_, err := l.dynamoDBClient.UpdateItem(input)

	if err != nil {
		return false, err
	}

	lease.SetLeaseCounter(lease.GetLeaseCounter() + 1)
	return true, nil
}

/**
 * Check (synchronously) if there are any leases in the lease table.
 *
 * @return true if there are no leases in the lease table
 *
 * @error LeasingDependencyError if DynamoDB scan fails in an unexpected way
 * @error LeasingInvalidStateError if lease table does not exist
 * @error ProvisionedThroughputError if DynamoDB scan fails due to lack of capacity
 */
func (l *LeaseManager) IsLeaseTableEmpty() (bool, error) {
	result, err := l.list(1)
	if err != nil {
		return true, err
	}
	return len(result) > 0, nil
}

// tableStatus check the current lease table status
func (l *LeaseManager) tableStatus() (*string, error) {
	input := &dynamodb.DescribeTableInput{
		TableName: aws.String(l.tableName),
	}

	result, err := l.dynamoDBClient.DescribeTable(input)
	if err != nil {
		return nil, err
	}

	return result.Table.TableStatus, nil
}

// List with the given page size (number of items to consider at a time). Package access for integration testing.
func (l *LeaseManager) list(limit int64) ([]ILease, error) {
	input := &dynamodb.ScanInput{
		TableName: aws.String(l.tableName),
	}

	if limit > 0 {
		input.SetLimit(limit)
	}

	result := []ILease{}

	for {
		scanResult, err := l.dynamoDBClient.Scan(input)
		if err != nil || scanResult == nil {
			break
		}

		for _, v := range scanResult.Items {
			result = append(result, l.serializer.FromDynamoRecord(v))
		}

		lastEvaluatedKey := scanResult.LastEvaluatedKey
		if lastEvaluatedKey == nil {
			scanResult = nil
			break
		} else {
			input.SetExclusiveStartKey(lastEvaluatedKey)
		}
	}

	return result, nil
}
