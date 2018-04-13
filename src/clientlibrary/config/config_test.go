package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConfig(t *testing.T) {
	kclConfig := NewKinesisClientLibConfig("appName", "StreamName", "workerId").
		WithFailoverTimeMillis(500).
		WithMaxRecords(100).
		WithInitialPositionInStream(TRIM_HORIZON).
		WithIdleTimeBetweenReadsInMillis(20).
		WithCallProcessRecordsEvenForEmptyRecordList(true).
		WithTaskBackoffTimeMillis(10).
		WithMetricsBufferTimeMillis(500).
		WithMetricsMaxQueueSize(200).
		WithRegionName("us-west-2")

	assert.Equal(t, "appName", kclConfig.ApplicationName)
	assert.Equal(t, "500", kclConfig.FailoverTimeMillis)
}
