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
	Init() error
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

type NoopMonitoringService struct{}

func (n *NoopMonitoringService) Init() error  { return nil }
func (n *NoopMonitoringService) Start() error { return nil }
func (n *NoopMonitoringService) Shutdown()    {}

func (n *NoopMonitoringService) IncrRecordsProcessed(shard string, count int)         {}
func (n *NoopMonitoringService) IncrBytesProcessed(shard string, count int64)         {}
func (n *NoopMonitoringService) MillisBehindLatest(shard string, millSeconds float64) {}
func (n *NoopMonitoringService) LeaseGained(shard string)                             {}
func (n *NoopMonitoringService) LeaseLost(shard string)                               {}
func (n *NoopMonitoringService) LeaseRenewed(shard string)                            {}
func (n *NoopMonitoringService) RecordGetRecordsTime(shard string, time float64)      {}
func (n *NoopMonitoringService) RecordProcessRecordsTime(shard string, time float64)  {}
