package worker

import (
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	log "github.com/sirupsen/logrus"

	cfg "clientlibrary/config"
	kc "clientlibrary/interfaces"
	"clientlibrary/utils"
	"github.com/stretchr/testify/assert"
)

const (
	streamName = "kcl-test"
	regionName = "us-west-2"
	workerID   = "test-worker"
)

const specstr = `{"name":"kube-qQyhk","networking":{"containerNetworkCidr":"10.2.0.0/16"},"orgName":"BVT-Org-cLQch","projectName":"project-tDSJd","serviceLevel":"DEVELOPER","size":{"count":1},"version":"1.8.1-4"}`

func TestWorker(t *testing.T) {
	os.Setenv("AWS_ACCESS_KEY_ID", "your aws access key id")
	os.Setenv("AWS_SECRET_ACCESS_KEY", "your aws secret access key")
	defer os.Unsetenv("AWS_ACCESS_KEY_ID")
	defer os.Unsetenv("AWS_SECRET_ACCESS_KEY")
	kclConfig := cfg.NewKinesisClientLibConfig("appName", streamName, regionName, workerID).
		WithInitialPositionInStream(cfg.LATEST).
		WithMaxRecords(10).
		WithMaxLeasesForWorker(1).
		WithShardSyncIntervalMillis(5000).
		WithFailoverTimeMillis(300000)

	log.SetOutput(os.Stdout)
	log.SetLevel(log.DebugLevel)

	assert.Equal(t, regionName, kclConfig.RegionName)
	assert.Equal(t, streamName, kclConfig.StreamName)

	worker := NewWorker(recordProcessorFactory(t), kclConfig, nil)
	assert.Equal(t, regionName, worker.regionName)
	assert.Equal(t, streamName, worker.streamName)

	err := worker.Start()
	assert.Nil(t, err)

	// Put some data into stream.
	for i := 0; i < 100; i++ {
		// Use random string as partition key to ensure even distribution across shards
		err := worker.Publish(streamName, utils.RandStringBytesMaskImpr(10), []byte(specstr))
		if err != nil {
			t.Errorf("Errorin Publish. %+v", err)
		}
	}

	time.Sleep(10 * time.Second)
	worker.Shutdown()
}

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
}

func (dd *dumpRecordProcessor) Initialize(input *kc.InitializationInput) {
	dd.t.Logf("Processing SharId: %v at checkpoint: %v", input.ShardId, aws.StringValue(input.ExtendedSequenceNumber.SequenceNumber))
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
	}

	// checkpoint it after processing this batch
	lastRecordSequenceNubmer := input.Records[len(input.Records)-1].SequenceNumber
	dd.t.Logf("Checkpoint progress at: %v,  MillisBehindLatest = %v", lastRecordSequenceNubmer, input.MillisBehindLatest)
	input.Checkpointer.Checkpoint(lastRecordSequenceNubmer)
}

func (dd *dumpRecordProcessor) Shutdown(input *kc.ShutdownInput) {
	dd.t.Logf("Shutdown Reason: %v", aws.StringValue(kc.ShutdownReasonMessage(input.ShutdownReason)))
}
