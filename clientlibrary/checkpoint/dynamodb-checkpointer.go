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
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/client"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"

	"github.com/vmware/vmware-go-kcl/clientlibrary/config"
	par "github.com/vmware/vmware-go-kcl/clientlibrary/partition"
	"github.com/vmware/vmware-go-kcl/clientlibrary/utils"
	"github.com/vmware/vmware-go-kcl/logger"
)

const (
	// ErrInvalidDynamoDBSchema is returned when there are one or more fields missing from the table
	ErrInvalidDynamoDBSchema = "The DynamoDB schema is invalid and may need to be re-created"

	// NumMaxRetries is the max times of doing retry
	NumMaxRetries = 10
)

// DynamoCheckpoint implements the Checkpoint interface using DynamoDB as a backend
type DynamoCheckpoint struct {
	log                     logger.Logger
	TableName               string
	leaseTableReadCapacity  int64
	leaseTableWriteCapacity int64

	LeaseDuration int
	svc           dynamodbiface.DynamoDBAPI
	kclConfig     *config.KinesisClientLibConfiguration
	Retries       int
	lastLeaseSync time.Time
}

func NewDynamoCheckpoint(kclConfig *config.KinesisClientLibConfiguration) *DynamoCheckpoint {
	checkpointer := &DynamoCheckpoint{
		log:                     kclConfig.Logger,
		TableName:               kclConfig.TableName,
		leaseTableReadCapacity:  int64(kclConfig.InitialLeaseTableReadCapacity),
		leaseTableWriteCapacity: int64(kclConfig.InitialLeaseTableWriteCapacity),
		LeaseDuration:           kclConfig.FailoverTimeMillis,
		kclConfig:               kclConfig,
		Retries:                 NumMaxRetries,
	}

	return checkpointer
}

// WithDynamoDB is used to provide DynamoDB service
func (checkpointer *DynamoCheckpoint) WithDynamoDB(svc dynamodbiface.DynamoDBAPI) *DynamoCheckpoint {
	checkpointer.svc = svc
	return checkpointer
}

// Init initialises the DynamoDB Checkpoint
func (checkpointer *DynamoCheckpoint) Init() error {
	checkpointer.log.Infof("Creating DynamoDB session")

	s, err := session.NewSession(&aws.Config{
		Region:      aws.String(checkpointer.kclConfig.RegionName),
		Endpoint:    aws.String(checkpointer.kclConfig.DynamoDBEndpoint),
		Credentials: checkpointer.kclConfig.DynamoDBCredentials,
		Retryer: client.DefaultRetryer{
			NumMaxRetries:    checkpointer.Retries,
			MinRetryDelay:    client.DefaultRetryerMinRetryDelay,
			MinThrottleDelay: client.DefaultRetryerMinThrottleDelay,
			MaxRetryDelay:    client.DefaultRetryerMaxRetryDelay,
			MaxThrottleDelay: client.DefaultRetryerMaxRetryDelay,
		},
	})

	if err != nil {
		// no need to move forward
		checkpointer.log.Fatalf("Failed in getting DynamoDB session for creating Worker: %+v", err)
	}

	if checkpointer.svc == nil {
		checkpointer.svc = dynamodb.New(s)
	}

	if !checkpointer.doesTableExist() {
		return checkpointer.createTable()
	}
	return nil
}

// GetLease attempts to gain a lock on the given shard
func (checkpointer *DynamoCheckpoint) GetLease(shard *par.ShardStatus, newAssignTo string) error {
	newLeaseTimeout := time.Now().Add(time.Duration(checkpointer.LeaseDuration) * time.Millisecond).UTC()
	newLeaseTimeoutString := newLeaseTimeout.Format(time.RFC3339)
	currentCheckpoint, err := checkpointer.getItem(shard.ID)
	if err != nil {
		return err
	}

	isClaimRequestExpired := shard.IsClaimRequestExpired(checkpointer.kclConfig)

	var claimRequest string
	if checkpointer.kclConfig.EnableLeaseStealing {
		if currentCheckpointClaimRequest, ok := currentCheckpoint[ClaimRequestKey]; ok && currentCheckpointClaimRequest.S != nil {
			claimRequest = *currentCheckpointClaimRequest.S
			if newAssignTo != claimRequest && !isClaimRequestExpired {
				checkpointer.log.Debugf("another worker: %s has a claim on this shard. Not going to renew the lease", claimRequest)
				return errors.New(ErrShardClaimed)
			}
		}
	}

	assignedVar, assignedToOk := currentCheckpoint[LeaseOwnerKey]
	leaseVar, leaseTimeoutOk := currentCheckpoint[LeaseTimeoutKey]

	var conditionalExpression string
	var expressionAttributeValues map[string]*dynamodb.AttributeValue

	if !leaseTimeoutOk || !assignedToOk {
		conditionalExpression = "attribute_not_exists(AssignedTo)"
	} else {
		assignedTo := *assignedVar.S
		leaseTimeout := *leaseVar.S

		currentLeaseTimeout, err := time.Parse(time.RFC3339, leaseTimeout)
		if err != nil {
			return err
		}

		if checkpointer.kclConfig.EnableLeaseStealing {
			if time.Now().UTC().Before(currentLeaseTimeout) && assignedTo != newAssignTo && !isClaimRequestExpired {
				return ErrLeaseNotAcquired{"current lease timeout not yet expired"}
			}
		} else {
			if time.Now().UTC().Before(currentLeaseTimeout) && assignedTo != newAssignTo {
				return ErrLeaseNotAcquired{"current lease timeout not yet expired"}
			}
		}

		checkpointer.log.Debugf("Attempting to get a lock for shard: %s, leaseTimeout: %s, assignedTo: %s, newAssignedTo: %s", shard.ID, currentLeaseTimeout, assignedTo, newAssignTo)
		conditionalExpression = "ShardID = :id AND AssignedTo = :assigned_to AND LeaseTimeout = :lease_timeout"
		expressionAttributeValues = map[string]*dynamodb.AttributeValue{
			":id": {
				S: aws.String(shard.ID),
			},
			":assigned_to": {
				S: aws.String(assignedTo),
			},
			":lease_timeout": {
				S: aws.String(leaseTimeout),
			},
		}
	}

	marshalledCheckpoint := map[string]*dynamodb.AttributeValue{
		LeaseKeyKey: {
			S: aws.String(shard.ID),
		},
		LeaseOwnerKey: {
			S: aws.String(newAssignTo),
		},
		LeaseTimeoutKey: {
			S: aws.String(newLeaseTimeoutString),
		},
	}

	if len(shard.ParentShardId) > 0 {
		marshalledCheckpoint[ParentShardIdKey] = &dynamodb.AttributeValue{S: aws.String(shard.ParentShardId)}
	}

	if checkpoint := shard.GetCheckpoint(); checkpoint != "" {
		marshalledCheckpoint[SequenceNumberKey] = &dynamodb.AttributeValue{
			S: aws.String(checkpoint),
		}
	}

	if checkpointer.kclConfig.EnableLeaseStealing {
		if claimRequest != "" && claimRequest == newAssignTo && !isClaimRequestExpired {
			if expressionAttributeValues == nil {
				expressionAttributeValues = make(map[string]*dynamodb.AttributeValue)
			}
			conditionalExpression = conditionalExpression + " AND ClaimRequest = :claim_request"
			expressionAttributeValues[":claim_request"] = &dynamodb.AttributeValue{
				S: &claimRequest,
			}
		}
	}

	err = checkpointer.conditionalUpdate(conditionalExpression, expressionAttributeValues, marshalledCheckpoint)
	if err != nil {
		if utils.AWSErrCode(err) == dynamodb.ErrCodeConditionalCheckFailedException {
			return ErrLeaseNotAcquired{dynamodb.ErrCodeConditionalCheckFailedException}
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
	leaseTimeout := shard.GetLeaseTimeout().UTC().Format(time.RFC3339)
	marshalledCheckpoint := map[string]*dynamodb.AttributeValue{
		LeaseKeyKey: {
			S: aws.String(shard.ID),
		},
		SequenceNumberKey: {
			S: aws.String(shard.GetCheckpoint()),
		},
		LeaseOwnerKey: {
			S: aws.String(shard.GetLeaseOwner()),
		},
		LeaseTimeoutKey: {
			S: aws.String(leaseTimeout),
		},
	}

	if len(shard.ParentShardId) > 0 {
		marshalledCheckpoint[ParentShardIdKey] = &dynamodb.AttributeValue{S: &shard.ParentShardId}
	}

	return checkpointer.saveItem(marshalledCheckpoint)
}

// FetchCheckpoint retrieves the checkpoint for the given shard
func (checkpointer *DynamoCheckpoint) FetchCheckpoint(shard *par.ShardStatus) error {
	checkpoint, err := checkpointer.getItem(shard.ID)
	if err != nil {
		return err
	}

	sequenceID, ok := checkpoint[SequenceNumberKey]
	if !ok {
		return ErrSequenceIDNotFound
	}
	checkpointer.log.Debugf("Retrieved Shard Iterator %s", *sequenceID.S)
	shard.SetCheckpoint(aws.StringValue(sequenceID.S))

	if assignedTo, ok := checkpoint[LeaseOwnerKey]; ok {
		shard.SetLeaseOwner(aws.StringValue(assignedTo.S))
	}

	// Use up-to-date leaseTimeout to avoid ConditionalCheckFailedException when claiming
	if leaseTimeout, ok := checkpoint[LeaseTimeoutKey]; ok && leaseTimeout.S != nil {
		currentLeaseTimeout, err := time.Parse(time.RFC3339, aws.StringValue(leaseTimeout.S))
		if err != nil {
			return err
		}
		shard.LeaseTimeout = currentLeaseTimeout
	}

	return nil
}

// RemoveLeaseInfo to remove lease info for shard entry in dynamoDB because the shard no longer exists in Kinesis
func (checkpointer *DynamoCheckpoint) RemoveLeaseInfo(shardID string) error {
	err := checkpointer.removeItem(shardID)

	if err != nil {
		checkpointer.log.Errorf("Error in removing lease info for shard: %s, Error: %+v", shardID, err)
	} else {
		checkpointer.log.Infof("Lease info for shard: %s has been removed.", shardID)
	}

	return err
}

// RemoveLeaseOwner to remove lease owner for the shard entry
func (checkpointer *DynamoCheckpoint) RemoveLeaseOwner(shardID string) error {
	input := &dynamodb.UpdateItemInput{
		TableName: aws.String(checkpointer.TableName),
		Key: map[string]*dynamodb.AttributeValue{
			LeaseKeyKey: {
				S: aws.String(shardID),
			},
		},
		UpdateExpression: aws.String("remove " + LeaseOwnerKey),
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":assigned_to": {
				S: aws.String(checkpointer.kclConfig.WorkerID),
			},
		},
		ConditionExpression: aws.String("AssignedTo = :assigned_to"),
	}

	_, err := checkpointer.svc.UpdateItem(input)

	return err
}

// ListActiveWorkers returns a map of workers and their shards
func (checkpointer *DynamoCheckpoint) ListActiveWorkers(shardStatus map[string]*par.ShardStatus) (map[string][]*par.ShardStatus, error) {
	err := checkpointer.syncLeases(shardStatus)
	if err != nil {
		return nil, err
	}

	workers := map[string][]*par.ShardStatus{}
	for _, shard := range shardStatus {
		if shard.GetCheckpoint() == ShardEnd {
			continue
		}

		leaseOwner := shard.GetLeaseOwner()
		if leaseOwner == "" {
			checkpointer.log.Debugf("Shard Not Assigned Error. ShardID: %s, WorkerID: %s", shard.ID, checkpointer.kclConfig.WorkerID)
			return nil, ErrShardNotAssigned
		}
		if w, ok := workers[leaseOwner]; ok {
			workers[leaseOwner] = append(w, shard)
		} else {
			workers[leaseOwner] = []*par.ShardStatus{shard}
		}
	}
	return workers, nil
}

// ClaimShard places a claim request on a shard to signal a steal attempt
func (checkpointer *DynamoCheckpoint) ClaimShard(shard *par.ShardStatus, claimID string) error {
	err := checkpointer.FetchCheckpoint(shard)
	if err != nil && err != ErrSequenceIDNotFound {
		return err
	}
	leaseTimeoutString := shard.GetLeaseTimeout().Format(time.RFC3339)

	conditionalExpression := `ShardID = :id AND LeaseTimeout = :lease_timeout AND attribute_not_exists(ClaimRequest)`
	expressionAttributeValues := map[string]*dynamodb.AttributeValue{
		":id": {
			S: aws.String(shard.ID),
		},
		":lease_timeout": {
			S: aws.String(leaseTimeoutString),
		},
	}

	marshalledCheckpoint := map[string]*dynamodb.AttributeValue{
		LeaseKeyKey: {
			S: &shard.ID,
		},
		LeaseTimeoutKey: {
			S: &leaseTimeoutString,
		},
		SequenceNumberKey: {
			S: &shard.Checkpoint,
		},
		ClaimRequestKey: {
			S: &claimID,
		},
	}

	if leaseOwner := shard.GetLeaseOwner(); leaseOwner == "" {
		conditionalExpression += " AND attribute_not_exists(AssignedTo)"
	} else {
		marshalledCheckpoint[LeaseOwnerKey] = &dynamodb.AttributeValue{S: &leaseOwner}
		conditionalExpression += "AND AssignedTo = :assigned_to"
		expressionAttributeValues[":assigned_to"] = &dynamodb.AttributeValue{S: &leaseOwner}
	}

	if checkpoint := shard.GetCheckpoint(); checkpoint == "" {
		conditionalExpression += " AND attribute_not_exists(Checkpoint)"
	} else if checkpoint == ShardEnd {
		conditionalExpression += " AND Checkpoint <> :checkpoint"
		expressionAttributeValues[":checkpoint"] = &dynamodb.AttributeValue{S: aws.String(ShardEnd)}
	} else {
		conditionalExpression += " AND Checkpoint = :checkpoint"
		expressionAttributeValues[":checkpoint"] = &dynamodb.AttributeValue{S: &checkpoint}
	}

	if shard.ParentShardId == "" {
		conditionalExpression += " AND attribute_not_exists(ParentShardId)"
	} else {
		marshalledCheckpoint[ParentShardIdKey] = &dynamodb.AttributeValue{S: aws.String(shard.ParentShardId)}
		conditionalExpression += " AND ParentShardId = :parent_shard"
		expressionAttributeValues[":parent_shard"] = &dynamodb.AttributeValue{S: &shard.ParentShardId}
	}

	return checkpointer.conditionalUpdate(conditionalExpression, expressionAttributeValues, marshalledCheckpoint)
}

func (checkpointer *DynamoCheckpoint) syncLeases(shardStatus map[string]*par.ShardStatus) error {
	log := checkpointer.kclConfig.Logger

	if (checkpointer.lastLeaseSync.Add(time.Duration(checkpointer.kclConfig.LeaseSyncingTimeIntervalMillis) * time.Millisecond)).After(time.Now()) {
		return nil
	}

	checkpointer.lastLeaseSync = time.Now()
	input := &dynamodb.ScanInput{
		ProjectionExpression: aws.String(fmt.Sprintf("%s,%s,%s", LeaseKeyKey, LeaseOwnerKey, SequenceNumberKey)),
		Select:               aws.String("SPECIFIC_ATTRIBUTES"),
		TableName:            aws.String(checkpointer.kclConfig.TableName),
	}

	err := checkpointer.svc.ScanPages(input,
		func(pages *dynamodb.ScanOutput, lastPage bool) bool {
			results := pages.Items
			for _, result := range results {
				shardId, foundShardId := result[LeaseKeyKey]
				assignedTo, foundAssignedTo := result[LeaseOwnerKey]
				checkpoint, foundCheckpoint := result[SequenceNumberKey]
				if !foundShardId || !foundAssignedTo || !foundCheckpoint {
					continue
				}
				if shard, ok := shardStatus[aws.StringValue(shardId.S)]; ok {
					shard.SetLeaseOwner(aws.StringValue(assignedTo.S))
					shard.SetCheckpoint(aws.StringValue(checkpoint.S))
				}
			}
			return !lastPage
		})

	if err != nil {
		log.Debugf("Error performing SyncLeases. Error: %+v ", err)
		return err
	}
	log.Debugf("Lease sync completed. Next lease sync will occur in %s", time.Duration(checkpointer.kclConfig.LeaseSyncingTimeIntervalMillis)*time.Millisecond)
	return nil
}

func (checkpointer *DynamoCheckpoint) createTable() error {
	input := &dynamodb.CreateTableInput{
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{
				AttributeName: aws.String(LeaseKeyKey),
				AttributeType: aws.String("S"),
			},
		},
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: aws.String(LeaseKeyKey),
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
	_, err := checkpointer.svc.PutItem(input)
	return err
}

func (checkpointer *DynamoCheckpoint) getItem(shardID string) (map[string]*dynamodb.AttributeValue, error) {
	item, err := checkpointer.svc.GetItem(&dynamodb.GetItemInput{
		TableName: aws.String(checkpointer.TableName),
		Key: map[string]*dynamodb.AttributeValue{
			LeaseKeyKey: {
				S: aws.String(shardID),
			},
		},
	})
	return item.Item, err
}

func (checkpointer *DynamoCheckpoint) removeItem(shardID string) error {
	_, err := checkpointer.svc.DeleteItem(&dynamodb.DeleteItemInput{
		TableName: aws.String(checkpointer.TableName),
		Key: map[string]*dynamodb.AttributeValue{
			LeaseKeyKey: {
				S: aws.String(shardID),
			},
		},
	})
	return err
}
