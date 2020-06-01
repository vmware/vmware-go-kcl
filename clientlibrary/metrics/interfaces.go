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
package metrics

type MonitoringService interface {
	Init(appName, streamName, workerID string) error
	Start() error
	IncrRecordsProcessed(string, int)
	IncrBytesProcessed(string, int64)
	MillisBehindLatest(string, float64)
	LeaseGained(string)
	LeaseLost(string)
	LeaseRenewed(string)
	RecordGetRecordsTime(string, float64)
	RecordProcessRecordsTime(string, float64)
	Shutdown()
}

// NoopMonitoringService implements MonitoringService by does nothing.
type NoopMonitoringService struct{}

func (NoopMonitoringService) Init(appName, streamName, workerID string) error { return nil }
func (NoopMonitoringService) Start() error                                    { return nil }
func (NoopMonitoringService) Shutdown()                                       {}

func (NoopMonitoringService) IncrRecordsProcessed(shard string, count int)          {}
func (NoopMonitoringService) IncrBytesProcessed(shard string, count int64)          {}
func (NoopMonitoringService) MillisBehindLatest(shard string, millis float64)       {}
func (NoopMonitoringService) LeaseGained(shard string)                              {}
func (NoopMonitoringService) LeaseLost(shard string)                                {}
func (NoopMonitoringService) LeaseRenewed(shard string)                             {}
func (NoopMonitoringService) RecordGetRecordsTime(shard string, millis float64)     {}
func (NoopMonitoringService) RecordProcessRecordsTime(shard string, millis float64) {}
