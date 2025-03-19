package logger

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/mattn/go-isatty"
	zp "go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/library/go/core/log/zap"
)

var Log log.Logger

type Factory func(context.Context) log.Logger

func AdditionalComponentCallerEncoder(caller zapcore.EntryCaller, enc zapcore.PrimitiveArrayEncoder) {
	path := caller.String()
	lastIndex := len(path) - 1
	for i := 0; i < 3; i++ {
		lastIndex = strings.LastIndex(path[0:lastIndex], "/")
		if lastIndex == -1 {
			break
		}
	}
	if lastIndex > 0 {
		path = path[lastIndex+1:]
	}
	enc.AppendString(path)
}

type levels struct {
	Zap zapcore.Level
	Log log.Level
}

func getEnvLogLevels() levels {
	if level, ok := os.LookupEnv("LOG_LEVEL"); ok {
		return parseLevel(level)
	}
	return levels{zapcore.InfoLevel, log.InfoLevel}
}

func parseLevel(level string) levels {
	zpLvl := zapcore.InfoLevel
	lvl := log.InfoLevel
	if level != "" {
		fmt.Printf("overriden YT log level to: %v\n", level)
		var l zapcore.Level
		if err := l.UnmarshalText([]byte(level)); err == nil {
			zpLvl = l
		}
		var gl log.Level
		if err := gl.UnmarshalText([]byte(level)); err == nil {
			lvl = gl
		}
	}
	return levels{zpLvl, lvl}
}

func DefaultLoggerConfig(level zapcore.Level) zp.Config {
	encoder := zapcore.CapitalColorLevelEncoder
	if !isatty.IsTerminal(os.Stdout.Fd()) || !isatty.IsTerminal(os.Stderr.Fd()) {
		encoder = zapcore.CapitalLevelEncoder
	}

	return zp.Config{
		Level:            zp.NewAtomicLevelAt(level),
		Encoding:         "console",
		OutputPaths:      []string{"stdout"},
		ErrorOutputPaths: []string{"stderr"},
		EncoderConfig: zapcore.EncoderConfig{
			MessageKey:     "msg",
			LevelKey:       "level",
			TimeKey:        "ts",
			CallerKey:      "caller",
			EncodeLevel:    encoder,
			EncodeTime:     zapcore.ISO8601TimeEncoder,
			EncodeDuration: zapcore.StringDurationEncoder,
			EncodeCaller:   AdditionalComponentCallerEncoder,
		},
	}
}

func init() {
	level := getEnvLogLevels()
	Log = zap.Must(DefaultLoggerConfig(level.Zap))
}
