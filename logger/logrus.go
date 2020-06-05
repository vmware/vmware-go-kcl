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
// https://github.com/amitrai48/logger

package logger

import (
	"io"
	"os"

	"github.com/sirupsen/logrus"
	lumberjack "gopkg.in/natefinch/lumberjack.v2"
)

type LogrusLogEntry struct {
	entry *logrus.Entry
}

type LogrusLogger struct {
	logger logrus.FieldLogger
}

// NewLogrusLogger adapts existing logrus logger to Logger interface.
// The call is responsible for configuring logrus logger appropriately.
func NewLogrusLogger(lLogger logrus.FieldLogger) Logger {
	return &LogrusLogger{
		logger: lLogger,
	}
}

// NewLogrusLoggerWithConfig creates and configs Logger instance backed by
// logrus logger.
func NewLogrusLoggerWithConfig(config Configuration) Logger {
	logLevel := config.ConsoleLevel
	if logLevel == "" {
		logLevel = config.FileLevel
	}

	level, err := logrus.ParseLevel(logLevel)
	if err != nil {
		// fallback to InfoLevel
		level = logrus.InfoLevel
	}

	normalizeConfig(&config)

	stdOutHandler := os.Stdout
	fileHandler := &lumberjack.Logger{
		Filename:   config.Filename,
		MaxSize:    config.MaxSizeMB,
		Compress:   true,
		MaxAge:     config.MaxAgeDays,
		MaxBackups: config.MaxBackups,
		LocalTime:  config.LocalTime,
	}
	lLogger := &logrus.Logger{
		Out:       stdOutHandler,
		Formatter: getFormatter(config.ConsoleJSONFormat),
		Hooks:     make(logrus.LevelHooks),
		Level:     level,
	}

	if config.EnableConsole && config.EnableFile {
		lLogger.SetOutput(io.MultiWriter(stdOutHandler, fileHandler))
	} else {
		if config.EnableFile {
			lLogger.SetOutput(fileHandler)
			lLogger.SetFormatter(getFormatter(config.FileJSONFormat))
		}
	}

	return &LogrusLogger{
		logger: lLogger,
	}
}

func (l *LogrusLogger) Debugf(format string, args ...interface{}) {
	l.logger.Debugf(format, args...)
}

func (l *LogrusLogger) Infof(format string, args ...interface{}) {
	l.logger.Infof(format, args...)
}

func (l *LogrusLogger) Warnf(format string, args ...interface{}) {
	l.logger.Warnf(format, args...)
}

func (l *LogrusLogger) Errorf(format string, args ...interface{}) {
	l.logger.Errorf(format, args...)
}

func (l *LogrusLogger) Fatalf(format string, args ...interface{}) {
	l.logger.Fatalf(format, args...)
}

func (l *LogrusLogger) Panicf(format string, args ...interface{}) {
	l.logger.Fatalf(format, args...)
}

func (l *LogrusLogger) WithFields(fields Fields) Logger {
	return &LogrusLogEntry{
		entry: l.logger.WithFields(convertToLogrusFields(fields)),
	}
}

func (l *LogrusLogEntry) Debugf(format string, args ...interface{}) {
	l.entry.Debugf(format, args...)
}

func (l *LogrusLogEntry) Infof(format string, args ...interface{}) {
	l.entry.Infof(format, args...)
}

func (l *LogrusLogEntry) Warnf(format string, args ...interface{}) {
	l.entry.Warnf(format, args...)
}

func (l *LogrusLogEntry) Errorf(format string, args ...interface{}) {
	l.entry.Errorf(format, args...)
}

func (l *LogrusLogEntry) Fatalf(format string, args ...interface{}) {
	l.entry.Fatalf(format, args...)
}

func (l *LogrusLogEntry) Panicf(format string, args ...interface{}) {
	l.entry.Fatalf(format, args...)
}

func (l *LogrusLogEntry) WithFields(fields Fields) Logger {
	return &LogrusLogEntry{
		entry: l.entry.WithFields(convertToLogrusFields(fields)),
	}
}

func getFormatter(isJSON bool) logrus.Formatter {
	if isJSON {
		return &logrus.JSONFormatter{}
	}
	return &logrus.TextFormatter{
		FullTimestamp:          true,
		DisableLevelTruncation: true,
	}
}

func convertToLogrusFields(fields Fields) logrus.Fields {
	logrusFields := logrus.Fields{}
	for index, val := range fields {
		logrusFields[index] = val
	}
	return logrusFields
}
