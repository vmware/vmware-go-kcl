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
// Note: The implementation comes from https://www.mountedthoughts.com/golang-logger-interface/

package test

import (
	"github.com/stretchr/testify/assert"

	"github.com/sirupsen/logrus"
	"go.uber.org/zap"
	"testing"

	"github.com/vmware/vmware-go-kcl/logger"
	zaplogger "github.com/vmware/vmware-go-kcl/logger/zap"
)

func TestZapLoggerWithConfig(t *testing.T) {
	config := logger.Configuration{
		EnableConsole:     true,
		ConsoleLevel:      logger.Debug,
		ConsoleJSONFormat: true,
		EnableFile:        true,
		FileLevel:         logger.Info,
		FileJSONFormat:    true,
		Filename:          "log.log",
	}

	log := zaplogger.NewZapLoggerWithConfig(config)

	contextLogger := log.WithFields(logger.Fields{"key1": "value1"})
	contextLogger.Debugf("Starting with zap")
	contextLogger.Infof("Zap is awesome")
}

func TestZapLogger(t *testing.T) {
	zapLogger, err := zap.NewProduction()
	assert.Nil(t, err)

	log := zaplogger.NewZapLogger(zapLogger.Sugar())

	contextLogger := log.WithFields(logger.Fields{"key1": "value1"})
	contextLogger.Debugf("Starting with zap")
	contextLogger.Infof("Zap is awesome")
}

func TestLogrusLoggerWithConfig(t *testing.T) {
	config := logger.Configuration{
		EnableConsole:     true,
		ConsoleLevel:      logger.Debug,
		ConsoleJSONFormat: false,
		EnableFile:        true,
		FileLevel:         logger.Info,
		FileJSONFormat:    true,
		Filename:          "log.log",
	}

	log := logger.NewLogrusLoggerWithConfig(config)

	contextLogger := log.WithFields(logger.Fields{"key1": "value1"})
	contextLogger.Debugf("Starting with logrus")
	contextLogger.Infof("Logrus is awesome")
}

func TestLogrusLogger(t *testing.T) {
	// adapts to Logger interface from *logrus.Logger
	log := logger.NewLogrusLogger(logrus.StandardLogger())

	contextLogger := log.WithFields(logger.Fields{"key1": "value1"})
	contextLogger.Debugf("Starting with logrus")
	contextLogger.Infof("Logrus is awesome")
}

func TestLogrusLoggerWithFieldsAtInit(t *testing.T) {
	// adapts to Logger interface from *logrus.Entry
	fieldLogger := logrus.StandardLogger().WithField("key0", "value0")
	log := logger.NewLogrusLogger(fieldLogger)

	contextLogger := log.WithFields(logger.Fields{"key1": "value1"})
	contextLogger.Debugf("Starting with logrus")
	contextLogger.Infof("Structured logging is awesome")
}
