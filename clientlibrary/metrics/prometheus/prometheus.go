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
package prometheus

import (
	"net/http"

	prom "github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/vmware/vmware-go-kcl/logger"
)

// MonitoringService publishes kcl metrics to Prometheus.
// It might be trick if the service onboarding with KCL already uses Prometheus.
type MonitoringService struct {
	listenAddress string
	namespace     string
	streamName    string
	workerID      string
	region        string
	logger        logger.Logger

	processedRecords    *prom.CounterVec
	processedBytes      *prom.CounterVec
	behindLatestSeconds *prom.GaugeVec
	leasesHeld          *prom.GaugeVec
	leaseRenewals       *prom.CounterVec
	getRecordsTime      *prom.HistogramVec
	processRecordsTime  *prom.HistogramVec
}

// NewMonitoringService returns a Monitoring service publishing metrics to Prometheus.
func NewMonitoringService(listenAddress, region string, logger logger.Logger) *MonitoringService {
	return &MonitoringService{
		listenAddress: listenAddress,
		region:        region,
		logger:        logger,
	}
}

func (p *MonitoringService) Init(appName, streamName, workerID string) error {
	p.namespace = appName
	p.streamName = streamName
	p.workerID = workerID

	p.processedBytes = prom.NewCounterVec(prom.CounterOpts{
		Name: p.namespace + `_processed_bytes`,
		Help: "Number of bytes processed",
	}, []string{"kinesisStream", "shard"})
	p.processedRecords = prom.NewCounterVec(prom.CounterOpts{
		Name: p.namespace + `_processed_records`,
		Help: "Number of records processed",
	}, []string{"kinesisStream", "shard"})
	p.behindLatestSeconds = prom.NewGaugeVec(prom.GaugeOpts{
		Name: p.namespace + `_behind_latest_seconds`,
		Help: "The number of seconds processing is behind",
	}, []string{"kinesisStream", "shard"})
	p.leasesHeld = prom.NewGaugeVec(prom.GaugeOpts{
		Name: p.namespace + `_leases_held`,
		Help: "The number of leases held by the worker",
	}, []string{"kinesisStream", "shard", "workerID"})
	p.leaseRenewals = prom.NewCounterVec(prom.CounterOpts{
		Name: p.namespace + `_lease_renewals`,
		Help: "The number of successful lease renewals",
	}, []string{"kinesisStream", "shard", "workerID"})
	p.getRecordsTime = prom.NewHistogramVec(prom.HistogramOpts{
		Name: p.namespace + `_get_records_duration_seconds`,
		Help: "The time taken to fetch records and process them",
	}, []string{"kinesisStream", "shard"})
	p.processRecordsTime = prom.NewHistogramVec(prom.HistogramOpts{
		Name: p.namespace + `_process_records_duration_seconds`,
		Help: "The time taken to process records",
	}, []string{"kinesisStream", "shard"})

	metrics := []prom.Collector{
		p.processedBytes,
		p.processedRecords,
		p.behindLatestSeconds,
		p.leasesHeld,
		p.leaseRenewals,
		p.getRecordsTime,
		p.processRecordsTime,
	}
	for _, metric := range metrics {
		err := prom.Register(metric)
		if err != nil {
			return err
		}
	}

	return nil
}

func (p *MonitoringService) Start() error {
	http.Handle("/metrics", promhttp.Handler())
	go func() {
		p.logger.Infof("Starting Prometheus listener on %s", p.listenAddress)
		err := http.ListenAndServe(p.listenAddress, nil)
		if err != nil {
			p.logger.Errorf("Error starting Prometheus metrics endpoint. %+v", err)
		}
		p.logger.Infof("Stopped metrics server")
	}()

	return nil
}

func (p *MonitoringService) Shutdown() {}

func (p *MonitoringService) IncrRecordsProcessed(shard string, count int) {
	p.processedRecords.With(prom.Labels{"shard": shard, "kinesisStream": p.streamName}).Add(float64(count))
}

func (p *MonitoringService) IncrBytesProcessed(shard string, count int64) {
	p.processedBytes.With(prom.Labels{"shard": shard, "kinesisStream": p.streamName}).Add(float64(count))
}

func (p *MonitoringService) MillisBehindLatest(shard string, millis float64) {
	p.behindLatestSeconds.With(prom.Labels{"shard": shard, "kinesisStream": p.streamName}).Set(millis / 1000)
}

func (p *MonitoringService) LeaseGained(shard string) {
	p.leasesHeld.With(prom.Labels{"shard": shard, "kinesisStream": p.streamName, "workerID": p.workerID}).Inc()
}

func (p *MonitoringService) LeaseLost(shard string) {
	p.leasesHeld.With(prom.Labels{"shard": shard, "kinesisStream": p.streamName, "workerID": p.workerID}).Dec()
}

func (p *MonitoringService) LeaseRenewed(shard string) {
	p.leaseRenewals.With(prom.Labels{"shard": shard, "kinesisStream": p.streamName, "workerID": p.workerID}).Inc()
}

func (p *MonitoringService) RecordGetRecordsTime(shard string, millis float64) {
	p.getRecordsTime.With(prom.Labels{"shard": shard, "kinesisStream": p.streamName}).Observe(millis / 1000)
}

func (p *MonitoringService) RecordProcessRecordsTime(shard string, millis float64) {
	p.processRecordsTime.With(prom.Labels{"shard": shard, "kinesisStream": p.streamName}).Observe(millis / 1000)
}
