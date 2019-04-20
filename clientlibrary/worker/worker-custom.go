/*
 * Copyright (c) 2019 VMware, Inc.
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
package worker

import (
	log "github.com/sirupsen/logrus"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	chk "github.com/vmware/vmware-go-kcl/clientlibrary/checkpoint"
	"github.com/vmware/vmware-go-kcl/clientlibrary/config"
	kcl "github.com/vmware/vmware-go-kcl/clientlibrary/interfaces"
	"github.com/vmware/vmware-go-kcl/clientlibrary/metrics"
)

// NewCustomWorker constructs a Worker instance for processing Kinesis stream data by directly inject custom cjheckpointer.
func NewCustomWorker(factory kcl.IRecordProcessorFactory, kclConfig *config.KinesisClientLibConfiguration,
	checkpointer chk.Checkpointer, metricsConfig *metrics.MonitoringConfiguration) *Worker {
	w := &Worker{
		streamName:       kclConfig.StreamName,
		regionName:       kclConfig.RegionName,
		workerID:         kclConfig.WorkerID,
		processorFactory: factory,
		kclConfig:        kclConfig,
		checkpointer:     checkpointer,
		metricsConfig:    metricsConfig,
	}

	// create session for Kinesis
	log.Info("Creating Kinesis session")

	s, err := session.NewSession(&aws.Config{
		Region:      aws.String(w.regionName),
		Endpoint:    &kclConfig.KinesisEndpoint,
		Credentials: kclConfig.KinesisCredentials,
	})

	if err != nil {
		// no need to move forward
		log.Fatalf("Failed in getting Kinesis session for creating Worker: %+v", err)
	}
	w.kc = kinesis.New(s)

	if w.metricsConfig == nil {
		// "" means noop monitor service. i.e. not emitting any metrics.
		w.metricsConfig = &metrics.MonitoringConfiguration{MonitoringService: ""}
	}
	return w
}
