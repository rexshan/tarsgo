// @author kordenlu
// @创建时间 2017/07/27 16:40
// 功能描述:

package appzaplog

import (
	"github.com/rexshan/tarsgo/tars/util/appzaplog/zap"
	"github.com/rexshan/tarsgo/tars/util/appzaplog/zap/zapcore"
	"fmt"
	//"log/syslog"
	"gopkg.in/natefinch/lumberjack.v2"
	"os"
	"runtime"
	"time"
)

//func RFC3339TimeEncoder(t time.Time, enc zapcore.PrimitiveArrayEncoder) {
//	enc.AppendString(t.Format(time.RFC3339))
//}

type zapLogIniter interface {
	loginit(*appZapLogConf) (zap.AtomicLevel, *zap.Logger, error)
}

type macZapLogInit struct {
}
type winZapLogInit struct {
}

func (self *macZapLogInit) loginit(config *appZapLogConf) (zap.AtomicLevel, *zap.Logger, error) {
	var (
		zapconfig zap.Config
		llevel    zap.AtomicLevel
		lzaplog   *zap.Logger
		err       error
	)
	//if config.testenv {
	//	zapconfig = zap.NewDevelopmentConfig()
	//} else {
	zapconfig = zap.NewProductionConfig()
	//}

	zapconfig.DisableStacktrace = true
	zapconfig.EncoderConfig.TimeKey = "timestamp"                   //"@timestamp"
	zapconfig.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder //epochSecondTimeEncoder //RFC3339TimeEncoder
	lzaplog, err = zapconfig.Build()
	llevel = zapconfig.Level
	return llevel, lzaplog, err
}
func (self *winZapLogInit) loginit(config *appZapLogConf) (zap.AtomicLevel, *zap.Logger, error) {
	var (
		zapconfig zap.Config
		llevel    zap.AtomicLevel
		lzaplog   *zap.Logger
		err       error
	)
	//if config.testenv {
	//	zapconfig = zap.NewDevelopmentConfig()
	//} else {
	zapconfig = zap.NewProductionConfig()
	//}

	zapconfig.DisableStacktrace = true
	zapconfig.EncoderConfig.TimeKey = "timestamp"                   //"@timestamp"
	zapconfig.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder //epochSecondTimeEncoder //RFC3339TimeEncoder
	lzaplog, err = zapconfig.Build()
	llevel = zapconfig.Level
	return llevel, lzaplog, err
}

type unixLikeZapLogInit struct {
}

func epochMillisTimeEncoder(t time.Time, enc zapcore.PrimitiveArrayEncoder) {
	nanos := t.UnixNano()
	millis := nanos / int64(time.Millisecond)
	enc.AppendInt64(millis)
}

func epochSecondTimeEncoder(t time.Time, enc zapcore.PrimitiveArrayEncoder) {
	enc.AppendInt64(t.Unix())
}

func epochFullTimeEncoder(t time.Time, enc zapcore.PrimitiveArrayEncoder) {
	enc.AppendString(t.Format("2006-01-02 15:04:05.000"))
}

func (self *unixLikeZapLogInit) loginit(config *appZapLogConf) (zap.AtomicLevel, *zap.Logger, error) {
	var (
		llevel  zap.AtomicLevel
		lzaplog *zap.Logger
	)
	/*
		writer, err := syslog.New(syslog.LOG_ERR|syslog.LOG_LOCAL0, config.processName)
		if err != nil {
			return llevel, lzaplog, err
		}
		// Initialize Zap.
		encconf := zap.NewProductionEncoderConfig()
		encconf.TimeKey = "timestamp"               //"@timestamp"
		encconf.EncodeTime = epochFullTimeEncoder//epochSecondTimeEncoder //RFC3339TimeEncoder
		encoder := zapcore.NewJSONEncoder(encconf)
		if config.testenv {
			llevel = zap.NewAtomicLevelAt(zap.DebugLevel)
		} else {
			llevel = zap.NewAtomicLevelAt(zap.InfoLevel)
		}
		core := newCore(llevel, encoder, writer)

		lzaplog = zap.New(core, zap.AddCaller(), zap.AddStacktrace(zapcore.DPanicLevel))
	*/

	//writers := []zapcore.WriteSyncer{os.Stdout, os.Stderr}
	writers := []zapcore.WriteSyncer{os.Stderr}
	output := zapcore.NewMultiWriteSyncer(writers...)
	if len(config.logPath) != 0 {
		output = zapcore.AddSync(&lumberjack.Logger{
			Filename: config.logPath,
			MaxSize:  500, // megabytes
			MaxAge:   5,  // days
		})
	}

	encconf := zap.NewProductionEncoderConfig()
	encconf.TimeKey = "timestamp"             //"@timestamp"
	encconf.EncodeTime = epochFullTimeEncoder //epochSecondTimeEncoder //RFC3339TimeEncoder
	encoder := zapcore.NewJSONEncoder(encconf)
	if config.testenv {
		llevel = zap.NewAtomicLevelAt(zap.DebugLevel)
	} else {
		llevel = zap.NewAtomicLevelAt(zap.InfoLevel)
	}

	lzaplog = zap.New(zapcore.NewCore(encoder, output, llevel), zap.AddCaller(), zap.AddStacktrace(zapcore.DPanicLevel))
	return llevel, lzaplog, nil
}

func zapLogInit(config *appZapLogConf) (zap.AtomicLevel, *zap.Logger, error) {
	var (
		zapinit zapLogIniter
		level   zap.AtomicLevel
		llog    *zap.Logger
		err     error
	)

	if runtime.GOOS == "darwin" {
		zapinit = &macZapLogInit{}
	} else if runtime.GOOS == "windows" {
		zapinit = &winZapLogInit{}
	} else {
		zapinit = &unixLikeZapLogInit{}
	}

	if level, llog, err = zapinit.loginit(config); err != nil {
		fmt.Printf("loginit err:%v", err)
		return level, llog, err
	}

	if config.withPid {
		llog = llog.With(zap.Int("pid", os.Getpid()))
	}

	if config.HostName != "" {
		llog = llog.With(zap.String("hostname", config.HostName))
	}

	if config.ElkTemplateName != "" {
		llog = llog.With(zap.String("service", config.ElkTemplateName))
	}
	return level, llog, nil
}
