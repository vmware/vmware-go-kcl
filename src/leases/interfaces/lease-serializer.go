package interfaces

import (
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

// ILeaseSerializer an utility class that manages the mapping of Lease objects/operations to records in DynamoDB.
type ILeaseSerializer interface {

	/**
	 * Construct a DynamoDB record out of a Lease object
	 *
	 * @param lease lease object to serialize
	 * @return an attribute value map representing the lease object
	 */
	ToDynamoRecord(lease ILease) map[string]*dynamodb.AttributeValue

	/**
	 * Construct a Lease object out of a DynamoDB record.
	 *
	 * @param dynamoRecord attribute value map from DynamoDB
	 * @return a deserialized lease object representing the attribute value map
	 */
	FromDynamoRecord(dynamoRecord map[string]*dynamodb.AttributeValue) ILease

	/**
	 * Special getDynamoHashKey implementation used by ILeaseManager.getLease().
	 *
	 * @param leaseKey
	 * @return the attribute value map representing a Lease's hash key given a string.
	 */
	GetDynamoHashKey(leaseKey string) map[string]*dynamodb.AttributeValue

	/**
	 * @param lease
	 * @return the attribute value map asserting that a lease counter is what we expect.
	 */
	GetDynamoLeaseCounterExpectation(lease ILease) map[string]*dynamodb.ExpectedAttributeValue

	/**
	 * @param lease
	 * @return the attribute value map asserting that the lease owner is what we expect.
	 */
	GetDynamoLeaseOwnerExpectation(lease ILease) map[string]*dynamodb.ExpectedAttributeValue

	/**
	 * @return the attribute value map asserting that a lease does not exist.
	 */
	GetDynamoNonexistantExpectation() map[string]*dynamodb.ExpectedAttributeValue

	/**
	 * @param lease
	 * @return the attribute value map that increments a lease counter
	 */
	GetDynamoLeaseCounterUpdate(lease ILease) map[string]*dynamodb.AttributeValueUpdate

	/**
	 * @param lease
	 * @param newOwner
	 * @return the attribute value map that takes a lease for a new owner
	 */
	GetDynamoTakeLeaseUpdate(lease ILease, newOwner string) map[string]*dynamodb.AttributeValueUpdate

	/**
	 * @param lease
	 * @return the attribute value map that voids a lease
	 */
	GetDynamoEvictLeaseUpdate(lease ILease) map[string]*dynamodb.AttributeValueUpdate

	/**
	 * @param lease
	 * @return the attribute value map that updates application-specific data for a lease and increments the lease
	 *         counter
	 */
	GetDynamoUpdateLeaseUpdate(lease ILease) map[string]*dynamodb.AttributeValueUpdate

	/**
	 * @return the key schema for creating a DynamoDB table to store leases
	 */
	GetKeySchema() []*dynamodb.KeySchemaElement

	/**
	 * @return attribute definitions for creating a DynamoDB table to store leases
	 */
	GetAttributeDefinitions() []*dynamodb.AttributeDefinition
}
