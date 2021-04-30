/*
 * Copyright (c) 2020 VMware, Inc.
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
package test

import (
	"crypto/md5"
	"fmt"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"
	rec "github.com/awslabs/kinesis-aggregation/go/records"
	"github.com/golang/protobuf/proto"
	"github.com/vmware/vmware-go-kcl/clientlibrary/utils"

	"testing"
)

const specstr = `{"name":"kube-qQyhk","networking":{"containerNetworkCidr":"10.2.0.0/16"},"orgName":"BVT-Org-cLQch","projectName":"project-tDSJd","serviceLevel":"DEVELOPER","size":{"count":1},"version":"1.8.1-4"}`

// NewKinesisClient to create a Kinesis Client.
func NewKinesisClient(t *testing.T, regionName, endpoint string, credentials *credentials.Credentials) *kinesis.Kinesis {
	s, err := session.NewSession(&aws.Config{
		Region:      aws.String(regionName),
		Endpoint:    aws.String(endpoint),
		Credentials: credentials,
	})

	if err != nil {
		// no need to move forward
		t.Fatalf("Failed in getting Kinesis session for creating Worker: %+v", err)
	}
	return kinesis.New(s)
}

// NewDynamoDBClient to create a Kinesis Client.
func NewDynamoDBClient(t *testing.T, regionName, endpoint string, credentials *credentials.Credentials) *dynamodb.DynamoDB {
	s, err := session.NewSession(&aws.Config{
		Region:      aws.String(regionName),
		Endpoint:    aws.String(endpoint),
		Credentials: credentials,
	})

	if err != nil {
		// no need to move forward
		t.Fatalf("Failed in getting DynamoDB session for creating Worker: %+v", err)
	}
	return dynamodb.New(s)
}

func continuouslyPublishSomeData(t *testing.T, kc kinesisiface.KinesisAPI) func() {
	shards := []*kinesis.Shard{}
	var nextToken *string
	for {
		out, err := kc.ListShards(&kinesis.ListShardsInput{
			StreamName: aws.String(streamName),
			NextToken:  nextToken,
		})
		if err != nil {
			t.Errorf("Error in ListShards. %+v", err)
		}

		shards = append(shards, out.Shards...)
		if out.NextToken == nil {
			break
		}
		nextToken = out.NextToken
	}

	done := make(chan int)
	wg := &sync.WaitGroup{}

	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(500 * time.Millisecond)
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				publishToAllShards(t, kc, shards)
				publishSomeData(t, kc)
			}
		}
	}()

	return func() {
		close(done)
		wg.Wait()
	}
}

func publishToAllShards(t *testing.T, kc kinesisiface.KinesisAPI, shards []*kinesis.Shard) {
	// Put records to all shards
	for i := 0; i < 10; i++ {
		for _, shard := range shards {
			publishRecord(t, kc, shard.HashKeyRange.StartingHashKey)
		}
	}
}

// publishSomeData to put some records into Kinesis stream
func publishSomeData(t *testing.T, kc kinesisiface.KinesisAPI) {
	// Put some data into stream.
	t.Log("Putting data into stream using PutRecord API...")
	for i := 0; i < 50; i++ {
		publishRecord(t, kc, nil)
	}
	t.Log("Done putting data into stream using PutRecord API.")

	// Put some data into stream using PutRecords API
	t.Log("Putting data into stream using PutRecords API...")
	for i := 0; i < 10; i++ {
		publishRecords(t, kc)
	}
	t.Log("Done putting data into stream using PutRecords API.")

	// Put some data into stream using KPL Aggregate Record format
	t.Log("Putting data into stream using KPL Aggregate Record ...")
	for i := 0; i < 10; i++ {
		publishAggregateRecord(t, kc)
	}
	t.Log("Done putting data into stream using KPL Aggregate Record.")
}

// publishRecord to put a record into Kinesis stream using PutRecord API.
func publishRecord(t *testing.T, kc kinesisiface.KinesisAPI, hashKey *string) {
	input := &kinesis.PutRecordInput{
		Data:         []byte(specstr),
		StreamName:   aws.String(streamName),
		PartitionKey: aws.String(utils.RandStringBytesMaskImpr(10)),
	}
	if hashKey != nil {
		input.ExplicitHashKey = hashKey
	}
	// Use random string as partition key to ensure even distribution across shards
	_, err := kc.PutRecord(input)

	if err != nil {
		t.Errorf("Error in PutRecord. %+v", err)
	}
}

// publishRecord to put a record into Kinesis stream using PutRecords API.
func publishRecords(t *testing.T, kc kinesisiface.KinesisAPI) {
	// Use random string as partition key to ensure even distribution across shards
	records := make([]*kinesis.PutRecordsRequestEntry, 5)

	for i := 0; i < 5; i++ {
		record := &kinesis.PutRecordsRequestEntry{
			Data:         []byte(specstr),
			PartitionKey: aws.String(utils.RandStringBytesMaskImpr(10)),
		}
		records[i] = record
	}

	_, err := kc.PutRecords(&kinesis.PutRecordsInput{
		Records:    records,
		StreamName: aws.String(streamName),
	})

	if err != nil {
		t.Errorf("Error in PutRecords. %+v", err)
	}
}

// publishRecord to put a record into Kinesis stream using PutRecord API.
func publishAggregateRecord(t *testing.T, kc kinesisiface.KinesisAPI) {
	data := generateAggregateRecord(5, specstr)
	// Use random string as partition key to ensure even distribution across shards
	_, err := kc.PutRecord(&kinesis.PutRecordInput{
		Data:         data,
		StreamName:   aws.String(streamName),
		PartitionKey: aws.String(utils.RandStringBytesMaskImpr(10)),
	})

	if err != nil {
		t.Errorf("Error in PutRecord. %+v", err)
	}
}

// generateAggregateRecord generates an aggregate record in the correct AWS-specified format used by KPL.
// https://github.com/awslabs/amazon-kinesis-producer/blob/master/aggregation-format.md
// copy from: https://github.com/awslabs/kinesis-aggregation/blob/master/go/deaggregator/deaggregator_test.go
func generateAggregateRecord(numRecords int, content string) []byte {
	aggr := &rec.AggregatedRecord{}
	// Start with the magic header
	aggRecord := []byte("\xf3\x89\x9a\xc2")
	partKeyTable := make([]string, 0)

	// Create proto record with numRecords length
	for i := 0; i < numRecords; i++ {
		var partKey uint64
		var hashKey uint64
		partKey = uint64(i)
		hashKey = uint64(i) * uint64(10)
		r := &rec.Record{
			PartitionKeyIndex:    &partKey,
			ExplicitHashKeyIndex: &hashKey,
			Data:                 []byte(content),
			Tags:                 make([]*rec.Tag, 0),
		}

		aggr.Records = append(aggr.Records, r)
		partKeyVal := fmt.Sprint(i)
		partKeyTable = append(partKeyTable, partKeyVal)
	}

	aggr.PartitionKeyTable = partKeyTable
	// Marshal to protobuf record, create md5 sum from proto record
	// and append both to aggRecord with magic header
	data, _ := proto.Marshal(aggr)
	md5Hash := md5.Sum(data)
	aggRecord = append(aggRecord, data...)
	aggRecord = append(aggRecord, md5Hash[:]...)
	return aggRecord
}
