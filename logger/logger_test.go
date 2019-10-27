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

package logger

import (
	"github.com/stretchr/testify/assert"

	"github.com/sirupsen/logrus"
	"go.uber.org/zap"
	"testing"
)

func TestZapLoggerWithConfig(t *testing.T) {
	config := Configuration{
		EnableConsole:     true,
		ConsoleLevel:      Debug,
		ConsoleJSONFormat: true,
		EnableFile:        false,
		FileLevel:         Info,
		FileJSONFormat:    true,
		FileLocation:      "log.log",
	}

	log := NewZapLoggerWithConfig(config)

	contextLogger := log.WithFields(Fields{"key1": "value1"})
	contextLogger.Debugf("Starting with zap")
	contextLogger.Infof("Zap is awesome")
}

func TestZapLogger(t *testing.T) {
	zapLogger, err := zap.NewProduction()
	assert.Nil(t, err)

	log := NewZapLogger(zapLogger.Sugar())

	contextLogger := log.WithFields(Fields{"key1": "value1"})
	contextLogger.Debugf("Starting with zap")
	contextLogger.Infof("Zap is awesome")
}

func TestLogrusLoggerWithConfig(t *testing.T) {
	config := Configuration{
		EnableConsole:     true,
		ConsoleLevel:      Debug,
		ConsoleJSONFormat: false,
		EnableFile:        false,
		FileLevel:         Info,
		FileJSONFormat:    true,
		FileLocation:      "log.log",
	}

	log := NewLogrusLoggerWithConfig(config)

	contextLogger := log.WithFields(Fields{"key1": "value1"})
	contextLogger.Debugf("Starting with logrus")
	contextLogger.Infof("Logrus is awesome")
}

func TestLogrusLogger(t *testing.T) {
	// adapts to Logger interface
	log := NewLogrusLogger(logrus.StandardLogger())

	contextLogger := log.WithFields(Fields{"key1": "value1"})
	contextLogger.Debugf("Starting with logrus")
	contextLogger.Infof("Logrus is awesome")
}
