// @author kordenlu
// @创建时间 2017/06/01 14:06
// 功能描述:

package appzaplog

import (
	"github.com/pkg/errors"
	"github.com/rexshan/tarsgo/tars/util/appzaplog/zap/zapcore"
	"log/syslog"
)

//
// Core
//

type core struct {
	zapcore.LevelEnabler

	encoder zapcore.Encoder
	writer  *syslog.Writer

	fields []zapcore.Field
}

func newCore(enab zapcore.LevelEnabler, encoder zapcore.Encoder, writer *syslog.Writer) *core {
	return &core{
		LevelEnabler: enab,
		encoder:      encoder,
		writer:       writer,
	}
}

func (core *core) With(fields []zapcore.Field) zapcore.Core {
	// Clone core.
	clone := *core

	// Clone encoder.
	clone.encoder = core.encoder.Clone()

	// append fields.
	for i := range fields {
		fields[i].AddTo(clone.encoder)
	}
	// Done.
	return &clone
}

func (core *core) Check(entry zapcore.Entry, checked *zapcore.CheckedEntry) *zapcore.CheckedEntry {
	if core.Enabled(entry.Level) {
		return checked.AddCore(entry, core)
	}
	return checked
}

func (core *core) Write(entry zapcore.Entry, fields []zapcore.Field) error {
	// Generate the message.
	buffer, err := core.encoder.EncodeEntry(entry, fields)
	if err != nil {
		return errors.Wrap(err, "failed to encode log entry")
	}

	message := buffer.String()

	// Write the message.
	switch entry.Level {
	case zapcore.DebugLevel:
		return core.writer.Debug(message)

	case zapcore.InfoLevel:
		return core.writer.Info(message)

	case zapcore.WarnLevel:
		return core.writer.Warning(message)

	case zapcore.ErrorLevel:
		return core.writer.Err(message)

	case zapcore.DPanicLevel:
		return core.writer.Crit(message)

	case zapcore.PanicLevel:
		return core.writer.Crit(message)

	case zapcore.FatalLevel:
		return core.writer.Crit(message)

	default:
		return errors.Errorf("unknown log level: %v", entry.Level)
	}
}

func (core *core) Sync() error {
	return nil
}
