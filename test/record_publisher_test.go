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
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"
	"github.com/vmware/vmware-go-kcl/clientlibrary/utils"
	"testing"
)

const specstr = `{"name":"kube-qQyhk","networking":{"containerNetworkCidr":"10.2.0.0/16"},"orgName":"BVT-Org-cLQch","projectName":"project-tDSJd","serviceLevel":"DEVELOPER","size":{"count":1},"version":"1.8.1-4"}`

// NewKinesisClient to create a Kinesis Client.
func NewKinesisClient(t *testing.T, regionName, endpoint string, credentials *credentials.Credentials) *kinesis.Kinesis{
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

// publishSomeData to put some records into Kinesis stream
func publishSomeData(t *testing.T, kc kinesisiface.KinesisAPI) {
	// Put some data into stream.
	t.Log("Putting data into stream using PutRecord API...")
	for i := 0; i < 50; i++ {
		publishRecord(t, kc)
	}
	t.Log("Done putting data into stream using PutRecord API.")

	// Put some data into stream using PutRecords API
	t.Log("Putting data into stream using PutRecords API...")
	for i := 0; i < 10; i++ {
		publishRecords(t, kc)
	}
	t.Log("Done putting data into stream using PutRecords API.")
}

// publishRecord to put a record into Kinesis stream using PutRecord API.
func publishRecord(t *testing.T, kc kinesisiface.KinesisAPI) {
	// Use random string as partition key to ensure even distribution across shards
	_, err := kc.PutRecord(&kinesis.PutRecordInput{
		Data:         []byte(specstr),
		StreamName:   aws.String(streamName),
		PartitionKey: aws.String(utils.RandStringBytesMaskImpr(10)),
	})

	if err != nil {
		t.Errorf("Error in PutRecord. %+v", err)
	}
}

// publishRecord to put a record into Kinesis stream using PutRecords API.
func publishRecords(t *testing.T, kc kinesisiface.KinesisAPI) {
	// Use random string as partition key to ensure even distribution across shards
	records := make([]*kinesis.PutRecordsRequestEntry, 5)

	for i:= 0; i < 5; i++ {
		records[i] = &kinesis.PutRecordsRequestEntry{
			Data:         []byte(specstr),
			PartitionKey: aws.String(utils.RandStringBytesMaskImpr(10)),
		}
	}

	_, err := kc.PutRecords(&kinesis.PutRecordsInput{
		Records:      records,
		StreamName:   aws.String(streamName),
	})

	if err != nil {
		t.Errorf("Error in PutRecords. %+v", err)
	}
}