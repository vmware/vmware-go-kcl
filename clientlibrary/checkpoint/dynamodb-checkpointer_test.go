/*
 * Copyright (c) 2019 VMware, Inc.
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
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/stretchr/testify/assert"

	cfg "github.com/vmware/vmware-go-kcl/clientlibrary/config"
	par "github.com/vmware/vmware-go-kcl/clientlibrary/partition"
)

func TestDoesTableExist(t *testing.T) {
	svc := &mockDynamoDB{tableExist: true, item: map[string]*dynamodb.AttributeValue{}}
	checkpoint := &DynamoCheckpoint{
		TableName: "TableName",
		svc:       svc,
	}
	if !checkpoint.doesTableExist() {
		t.Error("Table exists but returned false")
	}

	svc = &mockDynamoDB{tableExist: false}
	checkpoint.svc = svc
	if checkpoint.doesTableExist() {
		t.Error("Table does not exist but returned true")
	}
}

func TestGetLeaseNotAquired(t *testing.T) {
	svc := &mockDynamoDB{tableExist: true, item: map[string]*dynamodb.AttributeValue{}}
	kclConfig := cfg.NewKinesisClientLibConfig("appName", "test", "us-west-2", "abc").
		WithInitialPositionInStream(cfg.LATEST).
		WithMaxRecords(10).
		WithMaxLeasesForWorker(1).
		WithShardSyncIntervalMillis(5000).
		WithFailoverTimeMillis(300000)

	checkpoint := NewDynamoCheckpoint(kclConfig).WithDynamoDB(svc)
	checkpoint.Init()
	err := checkpoint.GetLease(&par.ShardStatus{
		ID:         "0001",
		Checkpoint: "",
		Mux:        &sync.RWMutex{},
	}, "abcd-efgh")
	if err != nil {
		t.Errorf("Error getting lease %s", err)
	}

	err = checkpoint.GetLease(&par.ShardStatus{
		ID:         "0001",
		Checkpoint: "",
		Mux:        &sync.RWMutex{},
	}, "ijkl-mnop")

	if err == nil || !errors.As(err, &ErrLeaseNotAcquired{}) {
		t.Errorf("Got a lease when it was already held by abcd-efgh: %s", err)
	}
}

func TestGetLeaseAquired(t *testing.T) {
	svc := &mockDynamoDB{tableExist: true, item: map[string]*dynamodb.AttributeValue{}}
	kclConfig := cfg.NewKinesisClientLibConfig("appName", "test", "us-west-2", "abc").
		WithInitialPositionInStream(cfg.LATEST).
		WithMaxRecords(10).
		WithMaxLeasesForWorker(1).
		WithShardSyncIntervalMillis(5000).
		WithFailoverTimeMillis(300000)

	checkpoint := NewDynamoCheckpoint(kclConfig).WithDynamoDB(svc)
	checkpoint.Init()
	marshalledCheckpoint := map[string]*dynamodb.AttributeValue{
		LeaseKeyKey: {
			S: aws.String("0001"),
		},
		LeaseOwnerKey: {
			S: aws.String("abcd-efgh"),
		},
		LeaseTimeoutKey: {
			S: aws.String(time.Now().AddDate(0, -1, 0).UTC().Format(time.RFC3339)),
		},
		SequenceNumberKey: {
			S: aws.String("deadbeef"),
		},
	}
	input := &dynamodb.PutItemInput{
		TableName: aws.String("TableName"),
		Item:      marshalledCheckpoint,
	}
	checkpoint.svc.PutItem(input)
	shard := &par.ShardStatus{
		ID:         "0001",
		Checkpoint: "deadbeef",
		Mux:        &sync.RWMutex{},
	}
	err := checkpoint.GetLease(shard, "ijkl-mnop")

	if err != nil {
		t.Errorf("Lease not aquired after timeout %s", err)
	}

	id, ok := svc.item[SequenceNumberKey]
	if !ok {
		t.Error("Expected checkpoint to be set by GetLease")
	} else if *id.S != "deadbeef" {
		t.Errorf("Expected checkpoint to be deadbeef. Got '%s'", *id.S)
	}

	// release owner info
	err = checkpoint.RemoveLeaseOwner(shard.ID)
	assert.Nil(t, err)

	status := &par.ShardStatus{
		ID:  shard.ID,
		Mux: &sync.RWMutex{},
	}
	checkpoint.FetchCheckpoint(status)

	// checkpointer and parent shard id should be the same
	assert.Equal(t, shard.Checkpoint, status.Checkpoint)
	assert.Equal(t, shard.ParentShardId, status.ParentShardId)

	// Only the lease owner has been wiped out
	assert.Equal(t, "", status.GetLeaseOwner())
}

func TestGetLeaseShardClaimed(t *testing.T) {
	leaseTimeout := time.Now().Add(-100 * time.Second).UTC()
	svc := &mockDynamoDB{
		tableExist: true,
		item: map[string]*dynamodb.AttributeValue{
			ClaimRequestKey: {S: aws.String("ijkl-mnop")},
			LeaseTimeoutKey: {S: aws.String(leaseTimeout.Format(time.RFC3339))},
		},
	}
	kclConfig := cfg.NewKinesisClientLibConfig("appName", "test", "us-west-2", "abc").
		WithInitialPositionInStream(cfg.LATEST).
		WithMaxRecords(10).
		WithMaxLeasesForWorker(1).
		WithShardSyncIntervalMillis(5000).
		WithFailoverTimeMillis(300000).
		WithLeaseStealing(true)

	checkpoint := NewDynamoCheckpoint(kclConfig).WithDynamoDB(svc)
	checkpoint.Init()
	err := checkpoint.GetLease(&par.ShardStatus{
		ID:           "0001",
		Checkpoint:   "",
		LeaseTimeout: leaseTimeout,
		Mux:          &sync.RWMutex{},
	}, "abcd-efgh")
	if err == nil || err.Error() != ErrShardClaimed {
		t.Errorf("Got a lease when it was already claimed by by ijkl-mnop: %s", err)
	}

	err = checkpoint.GetLease(&par.ShardStatus{
		ID:           "0001",
		Checkpoint:   "",
		LeaseTimeout: leaseTimeout,
		Mux:          &sync.RWMutex{},
	}, "ijkl-mnop")
	if err != nil {
		t.Errorf("Error getting lease %s", err)
	}
}

func TestGetLeaseClaimRequestExpiredOwner(t *testing.T) {
	kclConfig := cfg.NewKinesisClientLibConfig("appName", "test", "us-west-2", "abc").
		WithInitialPositionInStream(cfg.LATEST).
		WithMaxRecords(10).
		WithMaxLeasesForWorker(1).
		WithShardSyncIntervalMillis(5000).
		WithFailoverTimeMillis(300000).
		WithLeaseStealing(true)

	// Not expired
	leaseTimeout := time.Now().
		Add(-time.Duration(kclConfig.LeaseStealingClaimTimeoutMillis) * time.Millisecond).
		Add(1 * time.Second).
		UTC()

	svc := &mockDynamoDB{
		tableExist: true,
		item: map[string]*dynamodb.AttributeValue{
			LeaseOwnerKey:   {S: aws.String("abcd-efgh")},
			ClaimRequestKey: {S: aws.String("ijkl-mnop")},
			LeaseTimeoutKey: {S: aws.String(leaseTimeout.Format(time.RFC3339))},
		},
	}

	checkpoint := NewDynamoCheckpoint(kclConfig).WithDynamoDB(svc)
	checkpoint.Init()
	err := checkpoint.GetLease(&par.ShardStatus{
		ID:           "0001",
		Checkpoint:   "",
		LeaseTimeout: leaseTimeout,
		Mux:          &sync.RWMutex{},
	}, "abcd-efgh")
	if err == nil || err.Error() != ErrShardClaimed {
		t.Errorf("Got a lease when it was already claimed by ijkl-mnop: %s", err)
	}
}

func TestGetLeaseClaimRequestExpiredClaimer(t *testing.T) {
	kclConfig := cfg.NewKinesisClientLibConfig("appName", "test", "us-west-2", "abc").
		WithInitialPositionInStream(cfg.LATEST).
		WithMaxRecords(10).
		WithMaxLeasesForWorker(1).
		WithShardSyncIntervalMillis(5000).
		WithFailoverTimeMillis(300000).
		WithLeaseStealing(true)

	// Not expired
	leaseTimeout := time.Now().
		Add(-time.Duration(kclConfig.LeaseStealingClaimTimeoutMillis) * time.Millisecond).
		Add(121 * time.Second).
		UTC()

	svc := &mockDynamoDB{
		tableExist: true,
		item: map[string]*dynamodb.AttributeValue{
			LeaseOwnerKey:   {S: aws.String("abcd-efgh")},
			ClaimRequestKey: {S: aws.String("ijkl-mnop")},
			LeaseTimeoutKey: {S: aws.String(leaseTimeout.Format(time.RFC3339))},
		},
	}

	checkpoint := NewDynamoCheckpoint(kclConfig).WithDynamoDB(svc)
	checkpoint.Init()
	err := checkpoint.GetLease(&par.ShardStatus{
		ID:           "0001",
		Checkpoint:   "",
		LeaseTimeout: leaseTimeout,
		Mux:          &sync.RWMutex{},
	}, "ijkl-mnop")
	if err == nil || !errors.As(err, &ErrLeaseNotAcquired{}) {
		t.Errorf("Got a lease when it was already claimed by ijkl-mnop: %s", err)
	}
}

func TestFetchCheckpointWithStealing(t *testing.T) {
	future := time.Now().AddDate(0, 1, 0)

	svc := &mockDynamoDB{
		tableExist: true,
		item: map[string]*dynamodb.AttributeValue{
			SequenceNumberKey: {S: aws.String("deadbeef")},
			LeaseOwnerKey:     {S: aws.String("abcd-efgh")},
			LeaseTimeoutKey: {
				S: aws.String(future.Format(time.RFC3339)),
			},
		},
	}

	kclConfig := cfg.NewKinesisClientLibConfig("appName", "test", "us-west-2", "abc").
		WithInitialPositionInStream(cfg.LATEST).
		WithMaxRecords(10).
		WithMaxLeasesForWorker(1).
		WithShardSyncIntervalMillis(5000).
		WithFailoverTimeMillis(300000).
		WithLeaseStealing(true)

	checkpoint := NewDynamoCheckpoint(kclConfig).WithDynamoDB(svc)
	checkpoint.Init()

	status := &par.ShardStatus{
		ID:           "0001",
		Checkpoint:   "",
		LeaseTimeout: time.Now(),
		Mux:          &sync.RWMutex{},
	}

	checkpoint.FetchCheckpoint(status)

	leaseTimeout, _ := time.Parse(time.RFC3339, *svc.item[LeaseTimeoutKey].S)
	assert.Equal(t, leaseTimeout, status.LeaseTimeout)
}

func TestGetLeaseConditional(t *testing.T) {
	svc := &mockDynamoDB{tableExist: true, item: map[string]*dynamodb.AttributeValue{}}
	kclConfig := cfg.NewKinesisClientLibConfig("appName", "test", "us-west-2", "abc").
		WithInitialPositionInStream(cfg.LATEST).
		WithMaxRecords(10).
		WithMaxLeasesForWorker(1).
		WithShardSyncIntervalMillis(5000).
		WithFailoverTimeMillis(300000).
		WithLeaseStealing(true)

	checkpoint := NewDynamoCheckpoint(kclConfig).WithDynamoDB(svc)
	checkpoint.Init()
	marshalledCheckpoint := map[string]*dynamodb.AttributeValue{
		LeaseKeyKey: {
			S: aws.String("0001"),
		},
		LeaseOwnerKey: {
			S: aws.String("abcd-efgh"),
		},
		LeaseTimeoutKey: {
			S: aws.String(time.Now().Add(-1 * time.Second).UTC().Format(time.RFC3339)),
		},
		SequenceNumberKey: {
			S: aws.String("deadbeef"),
		},
		ClaimRequestKey: {
			S: aws.String("ijkl-mnop"),
		},
	}
	input := &dynamodb.PutItemInput{
		TableName: aws.String("TableName"),
		Item:      marshalledCheckpoint,
	}
	checkpoint.svc.PutItem(input)
	shard := &par.ShardStatus{
		ID:           "0001",
		Checkpoint:   "deadbeef",
		ClaimRequest: "ijkl-mnop",
		Mux:          &sync.RWMutex{},
	}
	err := checkpoint.FetchCheckpoint(shard)
	if err != nil {
		t.Errorf("Could not fetch checkpoint %s", err)
	}

	err = checkpoint.GetLease(shard, "ijkl-mnop")
	if err != nil {
		t.Errorf("Lease not aquired after timeout %s", err)
	}
	assert.Equal(t, *svc.expressionAttributeValues[":claim_request"].S, "ijkl-mnop")
	assert.Contains(t, svc.conditionalExpression, " AND ClaimRequest = :claim_request")
}

type mockDynamoDB struct {
	dynamodbiface.DynamoDBAPI
	tableExist                bool
	item                      map[string]*dynamodb.AttributeValue
	conditionalExpression     string
	expressionAttributeValues map[string]*dynamodb.AttributeValue
}

func (m *mockDynamoDB) ScanPages(*dynamodb.ScanInput, func(*dynamodb.ScanOutput, bool) bool) error {
	return nil
}

func (m *mockDynamoDB) DescribeTable(*dynamodb.DescribeTableInput) (*dynamodb.DescribeTableOutput, error) {
	if !m.tableExist {
		return &dynamodb.DescribeTableOutput{}, awserr.New(dynamodb.ErrCodeResourceNotFoundException, "doesNotExist", errors.New(""))
	}
	return &dynamodb.DescribeTableOutput{}, nil
}

func (m *mockDynamoDB) PutItem(input *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error) {
	item := input.Item

	if shardID, ok := item[LeaseKeyKey]; ok {
		m.item[LeaseKeyKey] = shardID
	}

	if owner, ok := item[LeaseOwnerKey]; ok {
		m.item[LeaseOwnerKey] = owner
	}

	if timeout, ok := item[LeaseTimeoutKey]; ok {
		m.item[LeaseTimeoutKey] = timeout
	}

	if checkpoint, ok := item[SequenceNumberKey]; ok {
		m.item[SequenceNumberKey] = checkpoint
	}

	if parent, ok := item[ParentShardIdKey]; ok {
		m.item[ParentShardIdKey] = parent
	}

	if claimRequest, ok := item[ClaimRequestKey]; ok {
		m.item[ClaimRequestKey] = claimRequest
	}

	if input.ConditionExpression != nil {
		m.conditionalExpression = *input.ConditionExpression
	}

	m.expressionAttributeValues = input.ExpressionAttributeValues

	return nil, nil
}

func (m *mockDynamoDB) GetItem(input *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
	return &dynamodb.GetItemOutput{
		Item: m.item,
	}, nil
}

func (m *mockDynamoDB) UpdateItem(input *dynamodb.UpdateItemInput) (*dynamodb.UpdateItemOutput, error) {
	exp := input.UpdateExpression

	if aws.StringValue(exp) == "remove "+LeaseOwnerKey {
		delete(m.item, LeaseOwnerKey)
	}

	return nil, nil
}

func (m *mockDynamoDB) CreateTable(input *dynamodb.CreateTableInput) (*dynamodb.CreateTableOutput, error) {
	return &dynamodb.CreateTableOutput{}, nil
}

func TestListActiveWorkers(t *testing.T) {
	svc := &mockDynamoDB{tableExist: true, item: map[string]*dynamodb.AttributeValue{}}
	kclConfig := cfg.NewKinesisClientLibConfig("appName", "test", "us-west-2", "abc").
		WithLeaseStealing(true)

	checkpoint := NewDynamoCheckpoint(kclConfig).WithDynamoDB(svc)
	err := checkpoint.Init()
	if err != nil {
		t.Errorf("Checkpoint initialization failed: %+v", err)
	}

	shardStatus := map[string]*par.ShardStatus{
		"0000": {ID: "0000", AssignedTo: "worker_1", Checkpoint: "", Mux: &sync.RWMutex{}},
		"0001": {ID: "0001", AssignedTo: "worker_2", Checkpoint: "", Mux: &sync.RWMutex{}},
		"0002": {ID: "0002", AssignedTo: "worker_4", Checkpoint: "", Mux: &sync.RWMutex{}},
		"0003": {ID: "0003", AssignedTo: "worker_0", Checkpoint: "", Mux: &sync.RWMutex{}},
		"0004": {ID: "0004", AssignedTo: "worker_1", Checkpoint: "", Mux: &sync.RWMutex{}},
		"0005": {ID: "0005", AssignedTo: "worker_3", Checkpoint: "", Mux: &sync.RWMutex{}},
		"0006": {ID: "0006", AssignedTo: "worker_3", Checkpoint: "", Mux: &sync.RWMutex{}},
		"0007": {ID: "0007", AssignedTo: "worker_0", Checkpoint: "", Mux: &sync.RWMutex{}},
		"0008": {ID: "0008", AssignedTo: "worker_4", Checkpoint: "", Mux: &sync.RWMutex{}},
		"0009": {ID: "0009", AssignedTo: "worker_2", Checkpoint: "", Mux: &sync.RWMutex{}},
		"0010": {ID: "0010", AssignedTo: "worker_0", Checkpoint: ShardEnd, Mux: &sync.RWMutex{}},
	}

	workers, err := checkpoint.ListActiveWorkers(shardStatus)
	if err != nil {
		t.Error(err)
	}

	for workerID, shards := range workers {
		assert.Equal(t, 2, len(shards))
		for _, shard := range shards {
			assert.Equal(t, workerID, shard.AssignedTo)
		}
	}
}

func TestListActiveWorkersErrShardNotAssigned(t *testing.T) {
	svc := &mockDynamoDB{tableExist: true, item: map[string]*dynamodb.AttributeValue{}}
	kclConfig := cfg.NewKinesisClientLibConfig("appName", "test", "us-west-2", "abc").
		WithLeaseStealing(true)

	checkpoint := NewDynamoCheckpoint(kclConfig).WithDynamoDB(svc)
	err := checkpoint.Init()
	if err != nil {
		t.Errorf("Checkpoint initialization failed: %+v", err)
	}

	shardStatus := map[string]*par.ShardStatus{
		"0000": {ID: "0000", Mux: &sync.RWMutex{}},
	}

	_, err = checkpoint.ListActiveWorkers(shardStatus)
	if err != ErrShardNotAssigned {
		t.Error("Expected ErrShardNotAssigned when shard is missing AssignedTo value")
	}
}

func TestClaimShard(t *testing.T) {
	svc := &mockDynamoDB{tableExist: true, item: map[string]*dynamodb.AttributeValue{}}
	kclConfig := cfg.NewKinesisClientLibConfig("appName", "test", "us-west-2", "abc").
		WithInitialPositionInStream(cfg.LATEST).
		WithMaxRecords(10).
		WithMaxLeasesForWorker(1).
		WithShardSyncIntervalMillis(5000).
		WithFailoverTimeMillis(300000).
		WithLeaseStealing(true)

	checkpoint := NewDynamoCheckpoint(kclConfig).WithDynamoDB(svc)
	checkpoint.Init()

	marshalledCheckpoint := map[string]*dynamodb.AttributeValue{
		"ShardID": {
			S: aws.String("0001"),
		},
		"AssignedTo": {
			S: aws.String("abcd-efgh"),
		},
		"LeaseTimeout": {
			S: aws.String(time.Now().AddDate(0, -1, 0).UTC().Format(time.RFC3339)),
		},
		"Checkpoint": {
			S: aws.String("deadbeef"),
		},
	}
	input := &dynamodb.PutItemInput{
		TableName: aws.String("TableName"),
		Item:      marshalledCheckpoint,
	}
	checkpoint.svc.PutItem(input)
	shard := &par.ShardStatus{
		ID:         "0001",
		Checkpoint: "deadbeef",
		Mux:        &sync.RWMutex{},
	}

	err := checkpoint.ClaimShard(shard, "ijkl-mnop")
	if err != nil {
		t.Errorf("Shard not claimed %s", err)
	}

	claimRequest, ok := svc.item[ClaimRequestKey]
	if !ok {
		t.Error("Expected claimRequest to be set by ClaimShard")
	} else if *claimRequest.S != "ijkl-mnop" {
		t.Errorf("Expected checkpoint to be ijkl-mnop. Got '%s'", *claimRequest.S)
	}

	status := &par.ShardStatus{
		ID:  shard.ID,
		Mux: &sync.RWMutex{},
	}
	checkpoint.FetchCheckpoint(status)

	// asiggnedTo, checkpointer, and parent shard id should be the same
	assert.Equal(t, shard.AssignedTo, status.AssignedTo)
	assert.Equal(t, shard.Checkpoint, status.Checkpoint)
	assert.Equal(t, shard.ParentShardId, status.ParentShardId)
}
