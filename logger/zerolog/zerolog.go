package zerolog

import (
	"github.com/rs/zerolog"
	"github.com/vmware/vmware-go-kcl/logger"
	"gopkg.in/natefinch/lumberjack.v2"
	"os"
)

type zeroLogger struct {
	log zerolog.Logger
}

func NewZerologLogger() logger.Logger {
	return NewZerologLoggerWithConfig(logger.Configuration{
		EnableConsole:     true,
		ConsoleJSONFormat: true,
		ConsoleLevel:      logger.Info,
		EnableFile:        false,
		FileJSONFormat:    false,
		FileLevel:         logger.Info,
		Filename:          "",
		MaxSizeMB:         0,
		MaxAgeDays:        0,
		MaxBackups:        0,
		LocalTime:         true,
	})
}

func NewZerologLoggerWithConfig(config logger.Configuration) logger.Logger {
	var consoleHandler *zerolog.ConsoleWriter
	var fileHandler *lumberjack.Logger
	var finalLogger zerolog.Logger

	normalizeConfig(&config)

	if config.EnableConsole {
		consoleHandler = &zerolog.ConsoleWriter{Out: os.Stdout}
	}

	if config.EnableFile {
		fileHandler = &lumberjack.Logger{
			Filename:   config.Filename,
			MaxSize:    config.MaxSizeMB,
			Compress:   true,
			MaxAge:     config.MaxAgeDays,
			MaxBackups: config.MaxBackups,
			LocalTime:  config.LocalTime,
		}
	}

	if config.EnableConsole && config.EnableFile {
		multi := zerolog.MultiLevelWriter(consoleHandler, fileHandler)
		finalLogger = zerolog.New(multi).Level(getZeroLogLevel(config.ConsoleLevel)).With().Timestamp().Logger()
	} else if config.EnableFile {
		finalLogger = zerolog.New(fileHandler).Level(getZeroLogLevel(config.FileLevel)).With().Timestamp().Logger()
	} else {
		finalLogger = zerolog.New(consoleHandler).Level(getZeroLogLevel(config.ConsoleLevel)).With().Timestamp().Logger()
	}

	return &zeroLogger{log: finalLogger}
}

func (z zeroLogger) Debugf(format string, args ...interface{}) {
	z.log.Debug().Msgf(format, args...)
}

func (z zeroLogger) Infof(format string, args ...interface{}) {
	z.log.Info().Msgf(format, args...)
}

func (z zeroLogger) Warnf(format string, args ...interface{}) {
	z.log.Warn().Msgf(format, args...)
}

func (z zeroLogger) Errorf(format string, args ...interface{}) {
	z.log.Error().Msgf(format, args...)
}

func (z zeroLogger) Fatalf(format string, args ...interface{}) {
	z.log.Fatal().Msgf(format, args...)
}

func (z zeroLogger) Panicf(format string, args ...interface{}) {
	z.log.Panic().Msgf(format, args...)
}

func (z zeroLogger) WithFields(keyValues logger.Fields) logger.Logger {
	newLogger := z.log.With()
	for k, v := range keyValues {
		newLogger.Interface(k, v)
	}

	return &zeroLogger{
		log: newLogger.Logger(),
	}
}

func getZeroLogLevel(level string) zerolog.Level {
	switch level {
	case logger.Info:
		return zerolog.InfoLevel
	case logger.Warn:
		return zerolog.WarnLevel
	case logger.Debug:
		return zerolog.DebugLevel
	case logger.Error:
		return zerolog.ErrorLevel
	case logger.Fatal:
		return zerolog.FatalLevel
	default:
		return zerolog.InfoLevel
	}
}

func normalizeConfig(config *logger.Configuration) {
	if config.MaxSizeMB <= 0 {
		config.MaxSizeMB = 100
	}

	if config.MaxAgeDays <= 0 {
		config.MaxAgeDays = 7
	}

	if config.MaxBackups < 0 {
		config.MaxBackups = 0
	}
}
