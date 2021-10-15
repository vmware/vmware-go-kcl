package zerolog

import (
	"github.com/vmware/vmware-go-kcl/logger"
	"testing"
)

func TestZeroLogLoggerWithConfig(t *testing.T) {
	config := logger.Configuration{
		EnableConsole:     true,
		ConsoleLevel:      logger.Debug,
		ConsoleJSONFormat: true,
		EnableFile:        true,
		FileLevel:         logger.Info,
		FileJSONFormat:    false,
		Filename:          "/tmp/kcl-zerolog-log.log",
	}

	log := NewZerologLoggerWithConfig(config)

	contextLogger := log.WithFields(logger.Fields{"key1": "value1"})
	contextLogger.Debugf("Starting with rs zerolog")
	contextLogger.Infof("Rs zerolog is awesome")
}

func TestZeroLogLogger(t *testing.T) {
	log := NewZerologLogger()

	contextLogger := log.WithFields(logger.Fields{"key1": "value1"})
	contextLogger.Debugf("Starting with zerolog")
	contextLogger.Infof("Zerolog is awesome")
}
