/*
 * Copyright (c) 2018 VMware, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and
 * associated documentation files (the "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is furnished to do
 * so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies or substantial
 * portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT
 * NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
 * WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */
// The implementation is derived from https://github.com/patrobinson/gokini
//
// Copyright 2018 Patrick robinson
//
// Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
package checkpoint

import (
	"errors"
	"math"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/matryer/try"
	log "github.com/sirupsen/logrus"

	"github.com/vmware/vmware-go-kcl/clientlibrary/config"
	par "github.com/vmware/vmware-go-kcl/clientlibrary/partition"
)

const (
	// ErrInvalidDynamoDBSchema is returned when there are one or more fields missing from the table
	ErrInvalidDynamoDBSchema = "The DynamoDB schema is invalid and may need to be re-created"
)

// DynamoCheckpoint implements the Checkpoint interface using DynamoDB as a backend
type DynamoCheckpoint struct {
	TableName               string
	leaseTableReadCapacity  int64
	leaseTableWriteCapacity int64

	LeaseDuration int
	svc           dynamodbiface.DynamoDBAPI
	kclConfig     *config.KinesisClientLibConfiguration
	Retries       int
}

func NewDynamoCheckpoint(dynamo dynamodbiface.DynamoDBAPI, kclConfig *config.KinesisClientLibConfiguration) Checkpointer {
	checkpointer := &DynamoCheckpoint{
		TableName:               kclConfig.TableName,
		leaseTableReadCapacity:  int64(kclConfig.InitialLeaseTableReadCapacity),
		leaseTableWriteCapacity: int64(kclConfig.InitialLeaseTableWriteCapacity),
		LeaseDuration:           kclConfig.FailoverTimeMillis,
		svc:                     dynamo,
		kclConfig:               kclConfig,
		Retries:                 5,
	}
	return checkpointer
}

// Init initialises the DynamoDB Checkpoint
func (checkpointer *DynamoCheckpoint) Init() error {
	if !checkpointer.doesTableExist() {
		return checkpointer.createTable()
	}
	return nil
}

// GetLease attempts to gain a lock on the given shard
func (checkpointer *DynamoCheckpoint) GetLease(shard *par.ShardStatus, newAssignTo string) error {
	newLeaseTimeout := time.Now().Add(time.Duration(checkpointer.LeaseDuration) * time.Millisecond).UTC()
	newLeaseTimeoutString := newLeaseTimeout.Format(time.RFC3339)
	currentCheckpoint, err := checkpointer.getItem(shard.GetLeaseID())
	if err != nil {
		return err
	}

	assignedVar, assignedToOk := currentCheckpoint[LEASE_OWNER_KEY]
	leaseVar, leaseTimeoutOk := currentCheckpoint[LEASE_TIMEOUT_KEY]
	var conditionalExpression string
	var expressionAttributeValues map[string]*dynamodb.AttributeValue

	id := shard.GetLeaseID()

	if !leaseTimeoutOk || !assignedToOk {
		conditionalExpression = "attribute_not_exists(AssignedTo)"
	} else {
		assignedTo := *assignedVar.S
		leaseTimeout := *leaseVar.S

		currentLeaseTimeout, err := time.Parse(time.RFC3339, leaseTimeout)
		if err != nil {
			return err
		}
		if !time.Now().UTC().After(currentLeaseTimeout) && assignedTo != newAssignTo {
			return errors.New(ErrLeaseNotAquired)
		}
		log.Debugf("Attempting to get a lock for shard: %s, leaseTimeout: %s, assignedTo: %s", id, currentLeaseTimeout, assignedTo)
		conditionalExpression = "ShardID = :id AND AssignedTo = :assigned_to AND LeaseTimeout = :lease_timeout"
		expressionAttributeValues = map[string]*dynamodb.AttributeValue{
			":id": {
				S: &id,
			},
			":assigned_to": {
				S: &assignedTo,
			},
			":lease_timeout": {
				S: &leaseTimeout,
			},
		}
	}

	marshalledCheckpoint := map[string]*dynamodb.AttributeValue{
		LEASE_KEY_KEY: {
			S: &id,
		},
		LEASE_OWNER_KEY: {
			S: &newAssignTo,
		},
		LEASE_TIMEOUT_KEY: {
			S: &newLeaseTimeoutString,
		},
	}

	if len(shard.ParentShardId) > 0 {
		marshalledCheckpoint[PARENT_SHARD_ID_KEY] = &dynamodb.AttributeValue{S: &shard.ParentShardId}
	}

	if shard.Checkpoint != "" {
		marshalledCheckpoint[CHECKPOINT_SEQUENCE_NUMBER_KEY] = &dynamodb.AttributeValue{
			S: &shard.Checkpoint,
		}
	}

	err = checkpointer.conditionalUpdate(conditionalExpression, expressionAttributeValues, marshalledCheckpoint)
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			if awsErr.Code() == dynamodb.ErrCodeConditionalCheckFailedException {
				return errors.New(ErrLeaseNotAquired)
			}
		}
		return err
	}

	shard.Mux.Lock()
	shard.AssignedTo = newAssignTo
	shard.LeaseTimeout = newLeaseTimeout
	shard.Mux.Unlock()

	return nil
}

// CheckpointSequence writes a checkpoint at the designated sequence ID
func (checkpointer *DynamoCheckpoint) CheckpointSequence(shard *par.ShardStatus) error {
	leaseTimeout := shard.LeaseTimeout.UTC().Format(time.RFC3339)
	id := shard.GetLeaseID()
	marshalledCheckpoint := map[string]*dynamodb.AttributeValue{
		LEASE_KEY_KEY: {
			S: &id,
		},
		CHECKPOINT_SEQUENCE_NUMBER_KEY: {
			S: &shard.Checkpoint,
		},
		LEASE_OWNER_KEY: {
			S: &shard.AssignedTo,
		},
		LEASE_TIMEOUT_KEY: {
			S: &leaseTimeout,
		},
	}

	if len(shard.ParentShardId) > 0 {
		marshalledCheckpoint[PARENT_SHARD_ID_KEY] = &dynamodb.AttributeValue{S: &shard.ParentShardId}
	}

	return checkpointer.saveItem(marshalledCheckpoint)
}

// FetchCheckpoint retrieves the checkpoint for the given shard
func (checkpointer *DynamoCheckpoint) FetchCheckpoint(shard *par.ShardStatus) error {
	checkpoint, err := checkpointer.getItem(shard.GetLeaseID())
	if err != nil {
		return err
	}

	sequenceID, ok := checkpoint[CHECKPOINT_SEQUENCE_NUMBER_KEY]
	if !ok {
		return ErrSequenceIDNotFound
	}
	log.Debugf("Retrieved Shard Iterator %s", *sequenceID.S)
	shard.Mux.Lock()
	defer shard.Mux.Unlock()
	shard.Checkpoint = *sequenceID.S

	if assignedTo, ok := checkpoint[LEASE_OWNER_KEY]; ok {
		shard.AssignedTo = *assignedTo.S
	}
	return nil
}

// RemoveLeaseInfo to remove lease info for shard entry in dynamoDB because the shard no longer exists in Kinesis
func (checkpointer *DynamoCheckpoint) RemoveLeaseInfo(leaseID string) error {
	err := checkpointer.removeItem(leaseID)

	if err != nil {
		log.Errorf("Error in removing lease info for shard: %s, Error: %+v", leaseID, err)
	} else {
		log.Infof("Lease info for shard: %s has been removed.", leaseID)
	}

	return err
}

func (checkpointer *DynamoCheckpoint) createTable() error {
	input := &dynamodb.CreateTableInput{
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{
				AttributeName: aws.String(LEASE_KEY_KEY),
				AttributeType: aws.String("S"),
			},
		},
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: aws.String(LEASE_KEY_KEY),
				KeyType:       aws.String("HASH"),
			},
		},
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(checkpointer.leaseTableReadCapacity),
			WriteCapacityUnits: aws.Int64(checkpointer.leaseTableWriteCapacity),
		},
		TableName: aws.String(checkpointer.TableName),
	}
	_, err := checkpointer.svc.CreateTable(input)
	return err
}

func (checkpointer *DynamoCheckpoint) doesTableExist() bool {
	input := &dynamodb.DescribeTableInput{
		TableName: aws.String(checkpointer.TableName),
	}
	_, err := checkpointer.svc.DescribeTable(input)
	return err == nil
}

func (checkpointer *DynamoCheckpoint) saveItem(item map[string]*dynamodb.AttributeValue) error {
	return checkpointer.putItem(&dynamodb.PutItemInput{
		TableName: aws.String(checkpointer.TableName),
		Item:      item,
	})
}

func (checkpointer *DynamoCheckpoint) conditionalUpdate(conditionExpression string, expressionAttributeValues map[string]*dynamodb.AttributeValue, item map[string]*dynamodb.AttributeValue) error {
	return checkpointer.putItem(&dynamodb.PutItemInput{
		ConditionExpression:       aws.String(conditionExpression),
		TableName:                 aws.String(checkpointer.TableName),
		Item:                      item,
		ExpressionAttributeValues: expressionAttributeValues,
	})
}

func (checkpointer *DynamoCheckpoint) putItem(input *dynamodb.PutItemInput) error {
	return try.Do(func(attempt int) (bool, error) {
		_, err := checkpointer.svc.PutItem(input)
		if awsErr, ok := err.(awserr.Error); ok {
			if awsErr.Code() == dynamodb.ErrCodeProvisionedThroughputExceededException ||
				awsErr.Code() == dynamodb.ErrCodeInternalServerError &&
					attempt < checkpointer.Retries {
				// Backoff time as recommended by https://docs.aws.amazon.com/general/latest/gr/api-retries.html
				time.Sleep(time.Duration(math.Exp2(float64(attempt))*100) * time.Millisecond)
				return true, err
			}
		}
		return false, err
	})
}

func (checkpointer *DynamoCheckpoint) getItem(leaseID string) (map[string]*dynamodb.AttributeValue, error) {
	var item *dynamodb.GetItemOutput
	err := try.Do(func(attempt int) (bool, error) {
		var err error
		item, err = checkpointer.svc.GetItem(&dynamodb.GetItemInput{
			TableName: aws.String(checkpointer.TableName),
			Key: map[string]*dynamodb.AttributeValue{
				LEASE_KEY_KEY: {
					S: aws.String(leaseID),
				},
			},
		})
		if awsErr, ok := err.(awserr.Error); ok {
			if awsErr.Code() == dynamodb.ErrCodeProvisionedThroughputExceededException ||
				awsErr.Code() == dynamodb.ErrCodeInternalServerError &&
					attempt < checkpointer.Retries {
				// Backoff time as recommended by https://docs.aws.amazon.com/general/latest/gr/api-retries.html
				time.Sleep(time.Duration(math.Exp2(float64(attempt))*100) * time.Millisecond)
				return true, err
			}
		}
		return false, err
	})
	return item.Item, err
}

func (checkpointer *DynamoCheckpoint) removeItem(leaseID string) error {
	err := try.Do(func(attempt int) (bool, error) {
		var err error
		_, err = checkpointer.svc.DeleteItem(&dynamodb.DeleteItemInput{
			TableName: aws.String(checkpointer.TableName),
			Key: map[string]*dynamodb.AttributeValue{
				LEASE_KEY_KEY: {
					S: aws.String(leaseID),
				},
			},
		})
		if awsErr, ok := err.(awserr.Error); ok {
			if awsErr.Code() == dynamodb.ErrCodeProvisionedThroughputExceededException ||
				awsErr.Code() == dynamodb.ErrCodeInternalServerError &&
					attempt < checkpointer.Retries {
				// Backoff time as recommended by https://docs.aws.amazon.com/general/latest/gr/api-retries.html
				time.Sleep(time.Duration(math.Exp2(float64(attempt))*100) * time.Millisecond)
				return true, err
			}
		}
		return false, err
	})
	return err
}
