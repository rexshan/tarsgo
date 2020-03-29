package appzaplog

import (
	"github.com/rexshan/tarsgo/tars/util/appzaplog/zap/zapcore"
	"github.com/rexshan/tarsgo/tars/util/appzaplog/zap"
)

func UID(uid uint64) zapcore.Field {
	return zap.Uint64("uid",uid)
}

func ROOMID(roomid uint64) zapcore.Field {
	return zap.Uint64("roomid",roomid)
}

