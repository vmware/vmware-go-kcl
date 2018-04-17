package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	log "github.com/sirupsen/logrus"
)

type PrometheusMonitoringService struct {
	ListenAddress string

	Namespace          string
	KinesisStream      string
	WorkerID           string
	processedRecords   *prometheus.CounterVec
	processedBytes     *prometheus.CounterVec
	behindLatestMillis *prometheus.GaugeVec
	leasesHeld         *prometheus.GaugeVec
	leaseRenewals      *prometheus.CounterVec
	getRecordsTime     *prometheus.HistogramVec
	processRecordsTime *prometheus.HistogramVec
}

func (p *PrometheusMonitoringService) Init() error {
	p.processedBytes = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: p.Namespace + `_processed_bytes`,
		Help: "Number of bytes processed",
	}, []string{"kinesisStream", "shard"})
	p.processedRecords = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: p.Namespace + `_processed_records`,
		Help: "Number of records processed",
	}, []string{"kinesisStream", "shard"})
	p.behindLatestMillis = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: p.Namespace + `_behind_latest_millis`,
		Help: "The amount of milliseconds processing is behind",
	}, []string{"kinesisStream", "shard"})
	p.leasesHeld = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: p.Namespace + `_leases_held`,
		Help: "The number of leases held by the worker",
	}, []string{"kinesisStream", "shard", "workerID"})
	p.leaseRenewals = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: p.Namespace + `_lease_renewals`,
		Help: "The number of successful lease renewals",
	}, []string{"kinesisStream", "shard", "workerID"})
	p.getRecordsTime = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: p.Namespace + `_get_records_duration_milliseconds`,
		Help: "The time taken to fetch records and process them",
	}, []string{"kinesisStream", "shard"})
	p.processRecordsTime = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: p.Namespace + `_process_records_duration_milliseconds`,
		Help: "The time taken to process records",
	}, []string{"kinesisStream", "shard"})

	metrics := []prometheus.Collector{
		p.processedBytes,
		p.processedRecords,
		p.behindLatestMillis,
		p.leasesHeld,
		p.leaseRenewals,
		p.getRecordsTime,
		p.processRecordsTime,
	}
	for _, metric := range metrics {
		err := prometheus.Register(metric)
		if err != nil {
			return err
		}
	}

	http.Handle("/metrics", promhttp.Handler())
	go func() {
		log.Debugf("Starting Prometheus listener on %s", p.ListenAddress)
		err := http.ListenAndServe(p.ListenAddress, nil)
		if err != nil {
			log.Errorln("Error starting Prometheus metrics endpoint", err)
		}
	}()
	return nil
}

func (p *PrometheusMonitoringService) IncrRecordsProcessed(shard string, count int) {
	p.processedRecords.With(prometheus.Labels{"shard": shard, "kinesisStream": p.KinesisStream}).Add(float64(count))
}

func (p *PrometheusMonitoringService) IncrBytesProcessed(shard string, count int64) {
	p.processedBytes.With(prometheus.Labels{"shard": shard, "kinesisStream": p.KinesisStream}).Add(float64(count))
}

func (p *PrometheusMonitoringService) MillisBehindLatest(shard string, millSeconds float64) {
	p.behindLatestMillis.With(prometheus.Labels{"shard": shard, "kinesisStream": p.KinesisStream}).Set(millSeconds)
}

func (p *PrometheusMonitoringService) LeaseGained(shard string) {
	p.leasesHeld.With(prometheus.Labels{"shard": shard, "kinesisStream": p.KinesisStream, "workerID": p.WorkerID}).Inc()
}

func (p *PrometheusMonitoringService) LeaseLost(shard string) {
	p.leasesHeld.With(prometheus.Labels{"shard": shard, "kinesisStream": p.KinesisStream, "workerID": p.WorkerID}).Dec()
}

func (p *PrometheusMonitoringService) LeaseRenewed(shard string) {
	p.leaseRenewals.With(prometheus.Labels{"shard": shard, "kinesisStream": p.KinesisStream, "workerID": p.WorkerID}).Inc()
}

func (p *PrometheusMonitoringService) RecordGetRecordsTime(shard string, time float64) {
	p.getRecordsTime.With(prometheus.Labels{"shard": shard, "kinesisStream": p.KinesisStream}).Observe(time)
}

func (p *PrometheusMonitoringService) RecordProcessRecordsTime(shard string, time float64) {
	p.processRecordsTime.With(prometheus.Labels{"shard": shard, "kinesisStream": p.KinesisStream}).Observe(time)
}
