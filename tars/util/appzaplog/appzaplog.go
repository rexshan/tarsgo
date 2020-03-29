// @author kordenlu
// @创建时间 2017/05/27 10:37
// 功能描述: zaplog的封装层

// Package appzaplog 对zaplog的封装,加入了一些常用日志信息的输出。
// 默认测试环境是DebugLevel，生产为InfoLevel。
// 可以通过http://127.0.0.1:port/log 来查询和修改日志级别
//
// DebugLevel用于详细的问题追踪
// InfoLevel 关键路径追踪
// WarnLevel 告警路径
// ErrLevel 错误路径

package appzaplog

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/rexshan/tarsgo/tars/util/appzaplog/zap"
	"github.com/rexshan/tarsgo/tars/util/appzaplog/zap/zapcore"
	"net/http"
	"strings"
)

var (
	appInnerLog *zap.Logger
)

// InitAppLog根据options的设置,初始化日志系统。
// 注意默认是测试环境模式,需要设置线上模式的需要设置TestEnv(false)
func InitAppLog(options ...appZapOption) error {
	var (
		err   error
		Level zap.AtomicLevel
	)
	config := defaultLogOptions
	for _, option := range options {
		option.apply(&config)
	}

	if Level, appInnerLog, err = zapLogInit(&config); err != nil {
		fmt.Printf("ZapLogInit err:%v", err)
		return err
	}

	appInnerLog = appInnerLog.WithOptions(zap.AddCallerSkip(1))
	logLevelHttpServer(&config, Level)
	return nil
}

// Debug logs a message at DebugLevel. The message includes any fields passed
// at the log site, as well as any fields accumulated on the logger.
func Debug(msg string, fields ...zapcore.Field) {
	appInnerLog.Debug(msg, fields...)
}

// Info logs a message at InfoLevel. The message includes any fields passed
// at the log site, as well as any fields accumulated on the logger.
func Info(msg string, fields ...zapcore.Field) {
	appInnerLog.Info(msg, fields...)
}

// Warn logs a message at WarnLevel. The message includes any fields passed
// at the log site, as well as any fields accumulated on the logger.
func Warn(msg string, fields ...zapcore.Field) {
	appInnerLog.Warn(msg, fields...)
}

// Error logs a message at ErrorLevel. The message includes any fields passed
// at the log site, as well as any fields accumulated on the logger.
func Error(msg string, fields ...zapcore.Field) {
	appInnerLog.Error(msg, fields...)
}

// DPanic logs a message at DPanicLevel. The message includes any fields passed
// at the log site, as well as any fields accumulated on the logger.
//
// If the logger is in development mode, it then panics (DPanic means
// "development panic"). This is useful for catching errors that are
// recoverable, but shouldn't ever happen.
func DPanic(msg string, fields ...zapcore.Field) {
	appInnerLog.DPanic(msg, fields...)
}

// Panic logs a message at PanicLevel. The message includes any fields passed
// at the log site, as well as any fields accumulated on the logger.
//
// The logger then panics, even if logging at PanicLevel is disabled.
func Panic(msg string, fields ...zapcore.Field) {
	appInnerLog.Panic(msg, fields...)
}

// Fatal logs a message at FatalLevel. The message includes any fields passed
// at the log site, as well as any fields accumulated on the logger.
//
// The logger then calls os.Exit(1), even if logging at FatalLevel is disabled.
func Fatal(msg string, fields ...zapcore.Field) {
	appInnerLog.Fatal(msg, fields...)
}

func Sync() error {
	return appInnerLog.Sync()
}

func SetLogLevel(level string) error {
	switch strings.ToLower(level) {
	case "debug", "info", "warn", "error", "fatal":
		level = strings.ToLower(level)
	case "all":
		level = "debug"
	case "off","none":
		level = "fatal"
	default:
		return errors.New("not support level")
	}
	client := http.Client{}

	type payload struct {
		Level string `json:"level"`
	}
	mypayload := payload{
		Level: level,
	}
	bin, err := json.Marshal(mypayload)
	if err != nil {
		return err
	}
	req, err := http.NewRequest("PUT", setlevelpath, bytes.NewReader(bin))
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return nil
}
