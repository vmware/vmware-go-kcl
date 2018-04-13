package impl

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"

	dynamoutils "leases/dynamoutils"
	. "leases/interfaces"
)

const (
	LEASE_KEY_KEY     = "leaseKey"
	LEASE_OWNER_KEY   = "leaseOwner"
	LEASE_COUNTER_KEY = "leaseCounter"
)

/**
 * An implementation of ILeaseSerializer for basic Lease objects. Can also instantiate subclasses of Lease so that
 * LeaseSerializer can be decorated by other classes if you need to add fields to leases.
 */
type LeaseSerializer struct {
}

/**
 * Construct a DynamoDB record out of a Lease object
 *
 * @param lease lease object to serialize
 * @return an attribute value map representing the lease object
 */
func (lc *LeaseSerializer) ToDynamoRecord(lease ILease) map[string]*dynamodb.AttributeValue {
	result := map[string]*dynamodb.AttributeValue{}

	result[LEASE_KEY_KEY], _ = dynamoutils.CreateAttributeValueFromString(lease.GetLeaseKey())
	result[LEASE_COUNTER_KEY], _ = dynamoutils.CreateAttributeValueFromLong(lease.GetLeaseCounter())

	if len(lease.GetLeaseOwner()) > 0 {
		result[LEASE_OWNER_KEY], _ = dynamoutils.CreateAttributeValueFromString(lease.GetLeaseOwner())
	}

	return result
}

/**
 * Construct a Lease object out of a DynamoDB record.
 *
 * @param dynamoRecord attribute value map from DynamoDB
 * @return a deserialized lease object representing the attribute value map
 */
func (lc *LeaseSerializer) FromDynamoRecord(dynamoRecord map[string]*dynamodb.AttributeValue) ILease {
	result := &Lease{}

	result.SetLeaseKey(aws.StringValue(dynamoutils.SafeGetString(dynamoRecord, LEASE_KEY_KEY)))
	result.SetLeaseOwner(aws.StringValue(dynamoutils.SafeGetString(dynamoRecord, LEASE_OWNER_KEY)))
	result.SetLeaseCounter(dynamoutils.SafeGetLong(dynamoRecord, LEASE_COUNTER_KEY))
	return result
}

/**
 * Special getDynamoHashKey implementation used by ILeaseManager.getLease().
 *
 * @param leaseKey
 * @return the attribute value map representing a Lease's hash key given a string.
 */
func (lc *LeaseSerializer) GetDynamoHashKey(leaseKey string) map[string]*dynamodb.AttributeValue {
	result := map[string]*dynamodb.AttributeValue{}
	result[LEASE_KEY_KEY], _ = dynamoutils.CreateAttributeValueFromString(leaseKey)
	return result
}

/**
 * @param lease
 * @return the attribute value map asserting that a lease counter is what we expect.
 */
func (lc *LeaseSerializer) GetDynamoLeaseCounterExpectation(lease ILease) map[string]*dynamodb.ExpectedAttributeValue {
	result := map[string]*dynamodb.ExpectedAttributeValue{}
	expectedAV := &dynamodb.ExpectedAttributeValue{}
	val, _ := dynamoutils.CreateAttributeValueFromLong(lease.GetLeaseCounter())
	expectedAV.SetValue(val)
	result[LEASE_COUNTER_KEY] = expectedAV
	return result
}

/**
 * @param lease
 * @return the attribute value map asserting that the lease owner is what we expect.
 */
func (lc *LeaseSerializer) GetDynamoLeaseOwnerExpectation(lease ILease) map[string]*dynamodb.ExpectedAttributeValue {
	result := map[string]*dynamodb.ExpectedAttributeValue{}
	expectedAV := &dynamodb.ExpectedAttributeValue{}
	val, _ := dynamoutils.CreateAttributeValueFromString(lease.GetLeaseOwner())
	expectedAV.SetValue(val)
	result[LEASE_OWNER_KEY] = expectedAV
	return result

}

/**
 * @return the attribute value map asserting that a lease does not exist.
 */
func (lc *LeaseSerializer) GetDynamoNonexistantExpectation() map[string]*dynamodb.ExpectedAttributeValue {
	result := map[string]*dynamodb.ExpectedAttributeValue{}
	expectedAV := &dynamodb.ExpectedAttributeValue{}
	expectedAV.SetExists(false)
	result[LEASE_KEY_KEY] = expectedAV

	return result
}

/**
 * @param lease
 * @return the attribute value map that increments a lease counter
 */
func (lc *LeaseSerializer) GetDynamoLeaseCounterUpdate(lease ILease) map[string]*dynamodb.AttributeValueUpdate {
	result := map[string]*dynamodb.AttributeValueUpdate{}
	updatedAV := &dynamodb.AttributeValueUpdate{}
	// Increase the lease counter by 1
	val, _ := dynamoutils.CreateAttributeValueFromLong(lease.GetLeaseCounter() + 1)
	updatedAV.SetValue(val)
	updatedAV.SetAction(dynamodb.AttributeActionPut)
	result[LEASE_COUNTER_KEY] = updatedAV
	return result
}

/**
 * @param lease
 * @param newOwner
 * @return the attribute value map that takes a lease for a new owner
 */
func (lc *LeaseSerializer) GetDynamoTakeLeaseUpdate(lease ILease, newOwner string) map[string]*dynamodb.AttributeValueUpdate {
	result := map[string]*dynamodb.AttributeValueUpdate{}
	updatedAV := &dynamodb.AttributeValueUpdate{}
	val, _ := dynamoutils.CreateAttributeValueFromString(lease.GetLeaseOwner())
	updatedAV.SetValue(val)
	updatedAV.SetAction(dynamodb.AttributeActionPut)
	result[LEASE_OWNER_KEY] = updatedAV
	return result
}

/**
 * @param lease
 * @return the attribute value map that voids a lease
 */
func (lc *LeaseSerializer) GetDynamoEvictLeaseUpdate(lease ILease) map[string]*dynamodb.AttributeValueUpdate {
	result := map[string]*dynamodb.AttributeValueUpdate{}
	updatedAV := &dynamodb.AttributeValueUpdate{}
	updatedAV.SetValue(nil)
	updatedAV.SetAction(dynamodb.AttributeActionDelete)
	result[LEASE_OWNER_KEY] = updatedAV
	return result
}

/**
 * @param lease
 * @return the attribute value map that updates application-specific data for a lease and increments the lease
 *         counter
 */
func (lc *LeaseSerializer) GetDynamoUpdateLeaseUpdate(lease ILease) map[string]*dynamodb.AttributeValueUpdate {
	result := map[string]*dynamodb.AttributeValueUpdate{}
	return result
}

/**
 * @return the key schema for creating a DynamoDB table to store leases
 */
func (lc *LeaseSerializer) GetKeySchema() []*dynamodb.KeySchemaElement {
	keySchema := []*dynamodb.KeySchemaElement{}
	schemaElement := &dynamodb.KeySchemaElement{}
	schemaElement.SetAttributeName(LEASE_KEY_KEY)
	schemaElement.SetKeyType(dynamodb.KeyTypeHash)
	keySchema = append(keySchema, schemaElement)
	return keySchema
}

/**
 * @return attribute definitions for creating a DynamoDB table to store leases
 */
func (lc *LeaseSerializer) GetAttributeDefinitions() []*dynamodb.AttributeDefinition {
	definitions := []*dynamodb.AttributeDefinition{}
	definition := &dynamodb.AttributeDefinition{}
	definition.SetAttributeName(LEASE_KEY_KEY)
	definition.SetAttributeType(dynamodb.ScalarAttributeTypeS)
	definitions = append(definitions, definition)
	return definitions
}
