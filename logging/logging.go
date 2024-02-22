package logging

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Logger abstracts the zap logger, useful for restricting what types of log levels can be logged (warn, critical etc) and reducing zap imports
type Logger struct {
	zap *zap.Logger
	cfg *zap.Config
}

// NewLogger creates a new logger based on the environment
func NewLogger(level ...string) (*Logger, error) {
	var cfgLogLevel zapcore.Level = zapcore.InfoLevel
	if len(level) > 0 {
		// Use the provided custom level
		switch level[0] {
		case "Debug":
			cfgLogLevel = zapcore.DebugLevel
		case "Info":
			cfgLogLevel = zapcore.InfoLevel
		case "Warn":
			cfgLogLevel = zapcore.WarnLevel
		case "Error":
			cfgLogLevel = zapcore.ErrorLevel
		default:
			return nil, errors.New(fmt.Sprintf("unrecognized level provided:%s", level[0]))
		}
	}

	hostname, err := os.Hostname()
	if err != nil {
		fmt.Printf("error getting hostname info, %v", err)
	}

	var lConfig zap.Config

	lConfig = zap.NewDevelopmentConfig()
	lConfig.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
	lConfig.Level.SetLevel(cfgLogLevel)

	lConfig.EncoderConfig.EncodeTime = zapcore.RFC3339NanoTimeEncoder
	lConfig.EncoderConfig.FunctionKey = "func"
	lConfig.OutputPaths = []string{"stdout"}
	lConfig.ErrorOutputPaths = []string{"stdout"}

	logger, err := lConfig.Build(zap.AddCallerSkip(1))
	if err != nil {
		fmt.Printf("error configuring default global logger, %v", err)
		return nil, err
	}

	LoggerObj := Logger{
		zap: logger,
		cfg: &lConfig,
	}

	return &LoggerObj, err
}

// format msg to remove newline character
func (l Logger) sanitize(msg string) string {
	sanitizedMsg := strings.ReplaceAll(msg, "\n", "")
	sanitizedMsg = strings.ReplaceAll(sanitizedMsg, "\r", "")
	return sanitizedMsg
}

// Debug wraps Sugar Debugf
func (l Logger) Debugf(msg string, args ...interface{}) {
	sanitized := l.sanitize(msg)
	l.writer().Sugar().Debugf(sanitized, args)
}

// Info wraps Sugar Infof
func (l Logger) Infof(msg string, args ...interface{}) {
	sanitized := l.sanitize(msg)
	l.writer().Sugar().Infof(sanitized, args...)
}

// Warn wraps Sugar Warnf
func (l Logger) Warnf(msg string, args ...interface{}) {
	sanitized := l.sanitize(msg)
	l.writer().Sugar().Warnf(sanitized, args)
}

// Error wraps Sugar Errorf
func (l Logger) Errorf(msg string, args ...interface{}) {
	sanitized := l.sanitize(msg)
	l.writer().Sugar().Errorf(sanitized, args)
}

// Debug wraps Sugar Debug
func (l Logger) Debug(msg string) {
	sanitized := l.sanitize(msg)
	l.writer().Sugar().Debug(sanitized)
}

// Info wraps Sugar Info
func (l Logger) Info(msg string) {
	sanitized := l.sanitize(msg)
	l.writer().Sugar().Info(sanitized)
}

// Warn wraps Sugar Warn
func (l Logger) Warn(msg string) {
	sanitized := l.sanitize(msg)
	l.writer().Sugar().Warn(sanitized)
}

// Error wraps Sugar Error
func (l Logger) Error(msg string) {
	sanitized := l.sanitize(msg)
	l.writer().Sugar().Error(sanitized)
}

// SetLogLevel changes the log level of the logger
func (l *Logger) SetLogLevel(level zapcore.Level) {
	l.cfg.Level.SetLevel(level)
}

// Create a no-op logger for tests
var noOpLogger = zap.NewNop()

// writer gets the existing zap logger or returns the noOpLogger for testing
func (l Logger) writer() *zap.Logger {
	if l.zap == nil {
		return noOpLogger
	}
	return l.zap
}
