package metrics

import (
	"fmt"
)

// MonitoringConfiguration allows you to configure how record processing metrics are exposed
type MonitoringConfiguration struct {
	MonitoringService string // Type of monitoring to expose. Supported types are "prometheus"
	Region            string
	Prometheus        PrometheusMonitoringService
	CloudWatch        CloudWatchMonitoringService
	service           MonitoringService
}

type MonitoringService interface {
	Init() error
	IncrRecordsProcessed(string, int)
	IncrBytesProcessed(string, int64)
	MillisBehindLatest(string, float64)
	LeaseGained(string)
	LeaseLost(string)
	LeaseRenewed(string)
	RecordGetRecordsTime(string, float64)
	RecordProcessRecordsTime(string, float64)
	Flush() error
}

func (m *MonitoringConfiguration) Init(nameSpace, streamName string, workerID string) error {
	if m.MonitoringService == "" {
		m.service = &noopMonitoringService{}
		return nil
	}

	switch m.MonitoringService {
	case "prometheus":
		m.Prometheus.Namespace = nameSpace
		m.Prometheus.KinesisStream = streamName
		m.Prometheus.WorkerID = workerID
		m.Prometheus.Region = m.Region
		m.service = &m.Prometheus
	case "cloudwatch":
		m.CloudWatch.Namespace = nameSpace
		m.CloudWatch.KinesisStream = streamName
		m.CloudWatch.WorkerID = workerID
		m.CloudWatch.Region = m.Region
		m.service = &m.CloudWatch
	default:
		return fmt.Errorf("Invalid monitoring service type %s", m.MonitoringService)
	}
	return m.service.Init()
}

func (m *MonitoringConfiguration) GetMonitoringService() MonitoringService {
	return m.service
}

type noopMonitoringService struct{}

func (n *noopMonitoringService) Init() error {
	return nil
}

func (n *noopMonitoringService) IncrRecordsProcessed(shard string, count int)         {}
func (n *noopMonitoringService) IncrBytesProcessed(shard string, count int64)         {}
func (n *noopMonitoringService) MillisBehindLatest(shard string, millSeconds float64) {}
func (n *noopMonitoringService) LeaseGained(shard string)                             {}
func (n *noopMonitoringService) LeaseLost(shard string)                               {}
func (n *noopMonitoringService) LeaseRenewed(shard string)                            {}
func (n *noopMonitoringService) RecordGetRecordsTime(shard string, time float64)      {}
func (n *noopMonitoringService) RecordProcessRecordsTime(shard string, time float64)  {}
func (n *noopMonitoringService) Flush() error                                         { return nil }
