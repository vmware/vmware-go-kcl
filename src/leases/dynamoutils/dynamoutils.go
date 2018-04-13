package util

import (
	"strconv"

	"clientlibrary/common"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

/**
 * Some static utility functions used by our LeaseSerializers.
 */

func CreateAttributeValueFromSS(collectionValue []*string) (*dynamodb.AttributeValue, error) {
	if len(collectionValue) == 0 {
		return nil, common.IllegalArgumentError.MakeErr().WithDetail("Collection attributeValues cannot be null or empty.")
	}

	attrib := &dynamodb.AttributeValue{}
	attrib.SetSS(collectionValue)

	return attrib, nil
}

func CreateAttributeValueFromString(stringValue string) (*dynamodb.AttributeValue, error) {
	if len(stringValue) == 0 {
		return nil, common.IllegalArgumentError.MakeErr().WithDetail("String attributeValues cannot be null or empty.")
	}

	attrib := &dynamodb.AttributeValue{}
	attrib.SetS(stringValue)

	return attrib, nil
}

func CreateAttributeValueFromLong(longValue int64) (*dynamodb.AttributeValue, error) {
	attrib := &dynamodb.AttributeValue{}
	attrib.SetN(strconv.FormatInt(longValue, 10))

	return attrib, nil
}

func SafeGetLong(dynamoRecord map[string]*dynamodb.AttributeValue, key string) int64 {
	av := dynamoRecord[key]

	if av == nil || av.N == nil {
		return 0
	}

	var val int64
	val, err := strconv.ParseInt(*av.N, 10, 64)

	if err != nil {
		return 0
	}

	return val
}

func SafeGetString(dynamoRecord map[string]*dynamodb.AttributeValue, key string) *string {
	av := dynamoRecord[key]
	if av == nil {
		return nil
	}

	return av.S
}

func SafeGetSS(dynamoRecord map[string]*dynamodb.AttributeValue, key string) []*string {
	av := dynamoRecord[key]

	if av == nil {
		var emptyslice []*string
		return emptyslice
	}

	return av.SS
}
