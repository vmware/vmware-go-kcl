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
	"github.com/stretchr/testify/assert"
	kc "github.com/vmware/vmware-go-kcl/clientlibrary/interfaces"
	"testing"
)

// Record processor factory is used to create RecordProcessor
func recordProcessorFactory(t *testing.T) kc.IRecordProcessorFactory {
	return &dumpRecordProcessorFactory{t: t}
}

// simple record processor and dump everything
type dumpRecordProcessorFactory struct {
	t *testing.T
}

func (d *dumpRecordProcessorFactory) CreateProcessor() kc.IRecordProcessor {
	return &dumpRecordProcessor{
		t: d.t,
	}
}

// Create a dump record processor for printing out all data from record.
type dumpRecordProcessor struct {
	t *testing.T
	count int
}

func (dd *dumpRecordProcessor) Initialize(input *kc.InitializationInput) {
	dd.t.Logf("Processing SharId: %v at checkpoint: %v", input.ShardId, aws.StringValue(input.ExtendedSequenceNumber.SequenceNumber))
	shardID = input.ShardId
	dd.count = 0
}

func (dd *dumpRecordProcessor) ProcessRecords(input *kc.ProcessRecordsInput) {
	dd.t.Log("Processing Records...")

	// don't process empty record
	if len(input.Records) == 0 {
		return
	}

	for _, v := range input.Records {
		dd.t.Logf("Record = %s", v.Data)
		assert.Equal(dd.t, specstr, string(v.Data))
		dd.count++
	}

	// checkpoint it after processing this batch
	lastRecordSequenceNubmer := input.Records[len(input.Records)-1].SequenceNumber
	dd.t.Logf("Checkpoint progress at: %v,  MillisBehindLatest = %v", lastRecordSequenceNubmer, input.MillisBehindLatest)
	input.Checkpointer.Checkpoint(lastRecordSequenceNubmer)
}

func (dd *dumpRecordProcessor) Shutdown(input *kc.ShutdownInput) {
	dd.t.Logf("Shutdown Reason: %v", aws.StringValue(kc.ShutdownReasonMessage(input.ShutdownReason)))
	dd.t.Logf("Processed Record Count = %d", dd.count)

	// When the value of {@link ShutdownInput#getShutdownReason()} is
	// {@link com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason#TERMINATE} it is required that you
	// checkpoint. Failure to do so will result in an IllegalArgumentException, and the KCL no longer making progress.
	if input.ShutdownReason == kc.TERMINATE {
		input.Checkpointer.Checkpoint(nil)
	}

	assert.True(dd.t, dd.count > 0)
}
