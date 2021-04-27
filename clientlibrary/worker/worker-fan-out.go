/*
 * Copyright (c) 2021 VMware, Inc.
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
	"fmt"
	"math"
	"time"

	"github.com/aws/aws-sdk-go/service/kinesis"

	"github.com/vmware/vmware-go-kcl/clientlibrary/utils"
)

// fetchConsumerARNWithRetry tries to fetch consumer ARN. Retries 10 times with exponential backoff in case of an error
func (w *Worker) fetchConsumerARNWithRetry() (string, error) {
	for retry := 0; ; retry++ {
		consumerARN, err := w.fetchConsumerARN()
		if err == nil {
			return consumerARN, nil
		}
		if retry < 10 {
			sleepDuration := time.Duration(math.Exp2(float64(retry))*100) * time.Millisecond
			w.kclConfig.Logger.Errorf("Could not get consumer ARN: %v, retrying after: %s", err, sleepDuration)
			time.Sleep(sleepDuration)
			continue
		}
		return consumerARN, err
	}
}

// fetchConsumerARN gets enhanced fan-out consumerARN.
// Registers enhanced fan-out consumer if the consumer is not found
func (w *Worker) fetchConsumerARN() (string, error) {
	log := w.kclConfig.Logger
	log.Debugf("Fetching stream consumer ARN")
	streamDescription, err := w.kc.DescribeStream(&kinesis.DescribeStreamInput{
		StreamName: &w.kclConfig.StreamName,
	})
	if err != nil {
		log.Errorf("Could not describe stream: %v", err)
		return "", err
	}
	streamConsumerDescription, err := w.kc.DescribeStreamConsumer(&kinesis.DescribeStreamConsumerInput{
		ConsumerName: &w.kclConfig.EnhancedFanOutConsumerName,
		StreamARN:    streamDescription.StreamDescription.StreamARN,
	})
	if err == nil {
		log.Infof("Enhanced fan-out consumer found, consumer status: %s", *streamConsumerDescription.ConsumerDescription.ConsumerStatus)
		if *streamConsumerDescription.ConsumerDescription.ConsumerStatus != kinesis.ConsumerStatusActive {
			return "", fmt.Errorf("consumer is not in active status yet, current status: %s", *streamConsumerDescription.ConsumerDescription.ConsumerStatus)
		}
		return *streamConsumerDescription.ConsumerDescription.ConsumerARN, nil
	}
	if utils.AWSErrCode(err) == kinesis.ErrCodeResourceNotFoundException {
		log.Infof("Enhanced fan-out consumer not found, registering new consumer with name: %s", w.kclConfig.EnhancedFanOutConsumerName)
		out, err := w.kc.RegisterStreamConsumer(&kinesis.RegisterStreamConsumerInput{
			ConsumerName: &w.kclConfig.EnhancedFanOutConsumerName,
			StreamARN:    streamDescription.StreamDescription.StreamARN,
		})
		if err != nil {
			log.Errorf("Could not register enhanced fan-out consumer: %v", err)
			return "", err
		}
		if *out.Consumer.ConsumerStatus != kinesis.ConsumerStatusActive {
			return "", fmt.Errorf("consumer is not in active status yet, current status: %s", *out.Consumer.ConsumerStatus)
		}
		return *out.Consumer.ConsumerARN, nil
	}
	log.Errorf("Could not describe stream consumer: %v", err)
	return "", err
}
