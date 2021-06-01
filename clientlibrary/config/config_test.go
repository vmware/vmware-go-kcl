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
package config

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/vmware/vmware-go-kcl/logger"
)

func TestConfig(t *testing.T) {
	kclConfig := NewKinesisClientLibConfig("appName", "StreamName", "us-west-2", "workerId").
		WithFailoverTimeMillis(500).
		WithMaxRecords(100).
		WithInitialPositionInStream(TRIM_HORIZON).
		WithIdleTimeBetweenReadsInMillis(20).
		WithCallProcessRecordsEvenForEmptyRecordList(true).
		WithTaskBackoffTimeMillis(10).
		WithEnhancedFanOutConsumerName("fan-out-consumer")

	assert.Equal(t, "appName", kclConfig.ApplicationName)
	assert.Equal(t, 500, kclConfig.FailoverTimeMillis)
	assert.Equal(t, 10, kclConfig.TaskBackoffTimeMillis)

	assert.True(t, kclConfig.EnableEnhancedFanOutConsumer)
	assert.Equal(t, "fan-out-consumer", kclConfig.EnhancedFanOutConsumerName)

	assert.Equal(t, false, kclConfig.EnableLeaseStealing)
	assert.Equal(t, 5000, kclConfig.LeaseStealingIntervalMillis)

	contextLogger := kclConfig.Logger.WithFields(logger.Fields{"key1": "value1"})
	contextLogger.Debugf("Starting with default logger")
	contextLogger.Infof("Default logger is awesome")
}

func TestConfigLeaseStealing(t *testing.T) {
	kclConfig := NewKinesisClientLibConfig("appName", "StreamName", "us-west-2", "workerId").
		WithFailoverTimeMillis(500).
		WithMaxRecords(100).
		WithInitialPositionInStream(TRIM_HORIZON).
		WithIdleTimeBetweenReadsInMillis(20).
		WithCallProcessRecordsEvenForEmptyRecordList(true).
		WithTaskBackoffTimeMillis(10).
		WithLeaseStealing(true).
		WithLeaseStealingIntervalMillis(10000)

	assert.Equal(t, "appName", kclConfig.ApplicationName)
	assert.Equal(t, 500, kclConfig.FailoverTimeMillis)
	assert.Equal(t, 10, kclConfig.TaskBackoffTimeMillis)
	assert.Equal(t, true, kclConfig.EnableLeaseStealing)
	assert.Equal(t, 10000, kclConfig.LeaseStealingIntervalMillis)

	contextLogger := kclConfig.Logger.WithFields(logger.Fields{"key1": "value1"})
	contextLogger.Debugf("Starting with default logger")
	contextLogger.Infof("Default logger is awesome")
}

func TestConfigDefaultEnhancedFanOutConsumerName(t *testing.T) {
	kclConfig := NewKinesisClientLibConfig("appName", "StreamName", "us-west-2", "workerId")

	assert.Equal(t, "appName", kclConfig.ApplicationName)
	assert.False(t, kclConfig.EnableEnhancedFanOutConsumer)
	assert.Equal(t, "appName", kclConfig.EnhancedFanOutConsumerName)
}

func TestEmptyEnhancedFanOutConsumerName(t *testing.T) {
	assert.PanicsWithValue(t, "Non-empty value expected for EnhancedFanOutConsumerName, actual: ", func() {
		NewKinesisClientLibConfig("app", "stream", "us-west-2", "worker").WithEnhancedFanOutConsumerName("")
	})
}

func TestConfigWithEnhancedFanOutConsumerARN(t *testing.T) {
	kclConfig := NewKinesisClientLibConfig("app", "stream", "us-west-2", "worker").
		WithEnhancedFanOutConsumerARN("consumer:arn")

	assert.True(t, kclConfig.EnableEnhancedFanOutConsumer)
	assert.Equal(t, "consumer:arn", kclConfig.EnhancedFanOutConsumerARN)
}

func TestEmptyEnhancedFanOutConsumerARN(t *testing.T) {
	assert.PanicsWithValue(t, "Non-empty value expected for EnhancedFanOutConsumerARN, actual: ", func() {
		NewKinesisClientLibConfig("app", "stream", "us-west-2", "worker").WithEnhancedFanOutConsumerARN("")
	})
}
