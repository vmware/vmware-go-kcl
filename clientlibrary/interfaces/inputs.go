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
// The implementation is derived from https://github.com/awslabs/amazon-kinesis-client
/*
 * Copyright 2014-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://aws.amazon.com/asl/
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package interfaces

import (
	"time"

	"github.com/aws/aws-sdk-go/aws"
	ks "github.com/aws/aws-sdk-go/service/kinesis"
)

const (
	/**
	 * Indicates that the entire application is being shutdown, and if desired the record processor will be given a
	 * final chance to checkpoint. This state will not trigger a direct call to
	 * {@link com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor#shutdown(ShutdownInput)}, but
	 * instead depend on a different interface for backward compatibility.
	 */
	REQUESTED ShutdownReason = iota + 1

	/**
	 * Terminate processing for this RecordProcessor (resharding use case).
	 * Indicates that the shard is closed and all records from the shard have been delivered to the application.
	 * Applications SHOULD checkpoint their progress to indicate that they have successfully processed all records
	 * from this shard and processing of child shards can be started.
	 */
	TERMINATE

	/**
	 * Processing will be moved to a different record processor (fail over, load balancing use cases).
	 * Applications SHOULD NOT checkpoint their progress (as another record processor may have already started
	 * processing data).
	 */
	ZOMBIE
)

// Containers for the parameters to the IRecordProcessor
type (
	/**
	 * Reason the RecordProcessor is being shutdown.
	 * Used to distinguish between a fail-over vs. a termination (shard is closed and all records have been delivered).
	 * In case of a fail over, applications should NOT checkpoint as part of shutdown,
	 * since another record processor may have already started processing records for that shard.
	 * In case of termination (resharding use case), applications SHOULD checkpoint their progress to indicate
	 * that they have successfully processed all the records (processing of child shards can then begin).
	 */
	ShutdownReason int

	InitializationInput struct {
		// The shardId that the record processor is being initialized for.
		ShardId string

		// The last extended sequence number that was successfully checkpointed by the previous record processor.
		ExtendedSequenceNumber *ExtendedSequenceNumber
	}

	ProcessRecordsInput struct {
		// The time that this batch of records was received by the KCL.
		CacheEntryTime *time.Time

		// The time that this batch of records was prepared to be provided to the RecordProcessor.
		CacheExitTime *time.Time

		// The records received from Kinesis. These records may have been de-aggregated if they were published by the KPL.
		Records []*ks.Record

		// A checkpointer that the RecordProcessor can use to checkpoint its progress.
		Checkpointer IRecordProcessorCheckpointer

		// How far behind this batch of records was when received from Kinesis.
		MillisBehindLatest int64
	}

	ShutdownInput struct {
		// ShutdownReason shows why RecordProcessor is going to be shutdown.
		ShutdownReason ShutdownReason

		// Checkpointer is used to record the current progress.
		Checkpointer IRecordProcessorCheckpointer
	}
)

var shutdownReasonMap = map[ShutdownReason]*string{
	REQUESTED: aws.String("REQUESTED"),
	TERMINATE: aws.String("TERMINATE"),
	ZOMBIE:    aws.String("ZOMBIE"),
}

func ShutdownReasonMessage(reason ShutdownReason) *string {
	return shutdownReasonMap[reason]
}
