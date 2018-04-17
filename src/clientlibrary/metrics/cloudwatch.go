package metrics

import (
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/cloudwatch"
	"github.com/aws/aws-sdk-go/service/cloudwatch/cloudwatchiface"
	log "github.com/sirupsen/logrus"
)

type CloudWatchMonitoringService struct {
	Namespace     string
	KinesisStream string
	WorkerID      string
	// What granularity we should send metrics to CW at. Note setting this to 1 will cost quite a bit of money
	// At the time of writing (March 2018) about US$200 per month
	ResolutionSec int
	svc           cloudwatchiface.CloudWatchAPI
	shardMetrics  map[string]*cloudWatchMetrics
}

type cloudWatchMetrics struct {
	processedRecords   int64
	processedBytes     int64
	behindLatestMillis []float64
	leasesHeld         int64
	leaseRenewals      int64
	getRecordsTime     []float64
	processRecordsTime []float64
	sync.Mutex
}

func (cw *CloudWatchMonitoringService) Init() error {
	if cw.ResolutionSec == 0 {
		cw.ResolutionSec = 60
	}

	session, err := session.NewSessionWithOptions(
		session.Options{
			SharedConfigState: session.SharedConfigEnable,
		},
	)
	if err != nil {
		return err
	}

	cw.svc = cloudwatch.New(session)
	cw.shardMetrics = make(map[string]*cloudWatchMetrics)
	return nil
}

func (cw *CloudWatchMonitoringService) flushDaemon() {
	previousFlushTime := time.Now()
	resolutionDuration := time.Duration(cw.ResolutionSec) * time.Second
	for {
		time.Sleep(resolutionDuration - time.Now().Sub(previousFlushTime))
		err := cw.flush()
		if err != nil {
			log.Errorln("Error sending metrics to CloudWatch", err)
		}
		previousFlushTime = time.Now()
	}
}

func (cw *CloudWatchMonitoringService) flush() error {
	for shard, metric := range cw.shardMetrics {
		metric.Lock()
		defaultDimensions := []*cloudwatch.Dimension{
			&cloudwatch.Dimension{
				Name:  aws.String("shard"),
				Value: &shard,
			},
			&cloudwatch.Dimension{
				Name:  aws.String("KinesisStreamName"),
				Value: &cw.KinesisStream,
			},
		}
		leaseDimensions := make([]*cloudwatch.Dimension, len(defaultDimensions))
		copy(defaultDimensions, leaseDimensions)
		leaseDimensions = append(leaseDimensions, &cloudwatch.Dimension{
			Name:  aws.String("WorkerID"),
			Value: &cw.WorkerID,
		})
		metricTimestamp := time.Now()
		_, err := cw.svc.PutMetricData(&cloudwatch.PutMetricDataInput{
			Namespace: aws.String(cw.Namespace),
			MetricData: []*cloudwatch.MetricDatum{
				&cloudwatch.MetricDatum{
					Dimensions: defaultDimensions,
					MetricName: aws.String("RecordsProcessed"),
					Unit:       aws.String("Count"),
					Timestamp:  &metricTimestamp,
					Value:      aws.Float64(float64(metric.processedRecords)),
				},
				&cloudwatch.MetricDatum{
					Dimensions: defaultDimensions,
					MetricName: aws.String("DataBytesProcessed"),
					Unit:       aws.String("Byte"),
					Timestamp:  &metricTimestamp,
					Value:      aws.Float64(float64(metric.processedBytes)),
				},
				&cloudwatch.MetricDatum{
					Dimensions: defaultDimensions,
					MetricName: aws.String("MillisBehindLatest"),
					Unit:       aws.String("Milliseconds"),
					Timestamp:  &metricTimestamp,
					StatisticValues: &cloudwatch.StatisticSet{
						SampleCount: aws.Float64(float64(len(metric.behindLatestMillis))),
						Sum:         sumFloat64(metric.behindLatestMillis),
						Maximum:     maxFloat64(metric.behindLatestMillis),
						Minimum:     minFloat64(metric.behindLatestMillis),
					},
				},
				&cloudwatch.MetricDatum{
					Dimensions: defaultDimensions,
					MetricName: aws.String("KinesisDataFetcher.getRecords.Time"),
					Unit:       aws.String("Milliseconds"),
					Timestamp:  &metricTimestamp,
					StatisticValues: &cloudwatch.StatisticSet{
						SampleCount: aws.Float64(float64(len(metric.getRecordsTime))),
						Sum:         sumFloat64(metric.getRecordsTime),
						Maximum:     maxFloat64(metric.getRecordsTime),
						Minimum:     minFloat64(metric.getRecordsTime),
					},
				},
				&cloudwatch.MetricDatum{
					Dimensions: defaultDimensions,
					MetricName: aws.String("RecordProcessor.processRecords.Time"),
					Unit:       aws.String("Milliseconds"),
					Timestamp:  &metricTimestamp,
					StatisticValues: &cloudwatch.StatisticSet{
						SampleCount: aws.Float64(float64(len(metric.processRecordsTime))),
						Sum:         sumFloat64(metric.processRecordsTime),
						Maximum:     maxFloat64(metric.processRecordsTime),
						Minimum:     minFloat64(metric.processRecordsTime),
					},
				},
				&cloudwatch.MetricDatum{
					Dimensions: leaseDimensions,
					MetricName: aws.String("RenewLease.Success"),
					Unit:       aws.String("Count"),
					Timestamp:  &metricTimestamp,
					Value:      aws.Float64(float64(metric.leaseRenewals)),
				},
				&cloudwatch.MetricDatum{
					Dimensions: leaseDimensions,
					MetricName: aws.String("CurrentLeases"),
					Unit:       aws.String("Count"),
					Timestamp:  &metricTimestamp,
					Value:      aws.Float64(float64(metric.leasesHeld)),
				},
			},
		})
		if err == nil {
			metric.processedRecords = 0
			metric.processedBytes = 0
			metric.behindLatestMillis = []float64{}
			metric.leaseRenewals = 0
			metric.getRecordsTime = []float64{}
			metric.processRecordsTime = []float64{}
		}
		metric.Unlock()
		return err
	}
	return nil
}

func (cw *CloudWatchMonitoringService) IncrRecordsProcessed(shard string, count int) {
	if _, ok := cw.shardMetrics[shard]; !ok {
		cw.shardMetrics[shard] = &cloudWatchMetrics{}
	}
	cw.shardMetrics[shard].Lock()
	defer cw.shardMetrics[shard].Unlock()
	cw.shardMetrics[shard].processedRecords += int64(count)
}

func (cw *CloudWatchMonitoringService) IncrBytesProcessed(shard string, count int64) {
	if _, ok := cw.shardMetrics[shard]; !ok {
		cw.shardMetrics[shard] = &cloudWatchMetrics{}
	}
	cw.shardMetrics[shard].Lock()
	defer cw.shardMetrics[shard].Unlock()
	cw.shardMetrics[shard].processedBytes += count
}

func (cw *CloudWatchMonitoringService) MillisBehindLatest(shard string, millSeconds float64) {
	if _, ok := cw.shardMetrics[shard]; !ok {
		cw.shardMetrics[shard] = &cloudWatchMetrics{}
	}
	cw.shardMetrics[shard].Lock()
	defer cw.shardMetrics[shard].Unlock()
	cw.shardMetrics[shard].behindLatestMillis = append(cw.shardMetrics[shard].behindLatestMillis, millSeconds)
}

func (cw *CloudWatchMonitoringService) LeaseGained(shard string) {
	if _, ok := cw.shardMetrics[shard]; !ok {
		cw.shardMetrics[shard] = &cloudWatchMetrics{}
	}
	cw.shardMetrics[shard].Lock()
	defer cw.shardMetrics[shard].Unlock()
	cw.shardMetrics[shard].leasesHeld++
}

func (cw *CloudWatchMonitoringService) LeaseLost(shard string) {
	if _, ok := cw.shardMetrics[shard]; !ok {
		cw.shardMetrics[shard] = &cloudWatchMetrics{}
	}
	cw.shardMetrics[shard].Lock()
	defer cw.shardMetrics[shard].Unlock()
	cw.shardMetrics[shard].leasesHeld--
}

func (cw *CloudWatchMonitoringService) LeaseRenewed(shard string) {
	if _, ok := cw.shardMetrics[shard]; !ok {
		cw.shardMetrics[shard] = &cloudWatchMetrics{}
	}
	cw.shardMetrics[shard].Lock()
	defer cw.shardMetrics[shard].Unlock()
	cw.shardMetrics[shard].leaseRenewals++
}

func (cw *CloudWatchMonitoringService) RecordGetRecordsTime(shard string, time float64) {
	if _, ok := cw.shardMetrics[shard]; !ok {
		cw.shardMetrics[shard] = &cloudWatchMetrics{}
	}
	cw.shardMetrics[shard].Lock()
	defer cw.shardMetrics[shard].Unlock()
	cw.shardMetrics[shard].getRecordsTime = append(cw.shardMetrics[shard].getRecordsTime, time)
}
func (cw *CloudWatchMonitoringService) RecordProcessRecordsTime(shard string, time float64) {
	if _, ok := cw.shardMetrics[shard]; !ok {
		cw.shardMetrics[shard] = &cloudWatchMetrics{}
	}
	cw.shardMetrics[shard].Lock()
	defer cw.shardMetrics[shard].Unlock()
	cw.shardMetrics[shard].processRecordsTime = append(cw.shardMetrics[shard].processRecordsTime, time)
}

func sumFloat64(slice []float64) *float64 {
	sum := float64(0)
	for _, num := range slice {
		sum += num
	}
	return &sum
}

func maxFloat64(slice []float64) *float64 {
	if len(slice) < 1 {
		return aws.Float64(0)
	}
	max := slice[0]
	for _, num := range slice {
		if num > max {
			max = num
		}
	}
	return &max
}

func minFloat64(slice []float64) *float64 {
	if len(slice) < 1 {
		return aws.Float64(0)
	}
	min := slice[0]
	for _, num := range slice {
		if num < min {
			min = num
		}
	}
	return &min
}
