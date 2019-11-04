package zap_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vmware/vmware-go-kcl/logger"
	"github.com/vmware/vmware-go-kcl/logger/zap"
	uzap "go.uber.org/zap"
)

func TestZapLoggerWithConfig(t *testing.T) {
	config := logger.Configuration{
		EnableConsole:     true,
		ConsoleLevel:      logger.Debug,
		ConsoleJSONFormat: true,
		EnableFile:        false,
		FileLevel:         logger.Info,
		FileJSONFormat:    true,
		Filename:          "log.log",
	}

	log := zap.NewZapLoggerWithConfig(config)

	contextLogger := log.WithFields(logger.Fields{"key1": "value1"})
	contextLogger.Debugf("Starting with zap")
	contextLogger.Infof("Zap is awesome")
}

func TestZapLogger(t *testing.T) {
	zapLogger, err := uzap.NewProduction()
	assert.Nil(t, err)

	log := zap.NewZapLogger(zapLogger.Sugar())

	contextLogger := log.WithFields(logger.Fields{"key1": "value1"})
	contextLogger.Debugf("Starting with zap")
	contextLogger.Infof("Zap is awesome")
}
