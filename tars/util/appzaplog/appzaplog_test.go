// @author kordenlu
// @创建时间 2017/06/02 10:05
// 功能描述:

package appzaplog

import (
	"encoding/json"
	"fmt"
	"github.com/rexshan/tarsgo/tars/util/appzaplog/zap"
	"github.com/rexshan/tarsgo/tars/util/appzaplog/zap/zapcore"
	"testing"
)

type payload struct {
	Level *zapcore.Level `json:"level"`
}

func Example() {
	if err := InitAppLog(TestEnv(false)); err != nil {
		fmt.Errorf("InitAppLog err:%v", err)
	}
	defer Sync()
	Info("hello world", zap.String("author", "lbq"))
}

func TestJsonPayLoad(t *testing.T) {
	level := zapcore.Level(-1)
	pl := payload{
		Level: &level,
	}
	binarypl, err := json.Marshal(pl)
	if err != nil {
		t.Errorf("failed err:%v", err)
	}
	t.Logf("json str:%v", string(binarypl))
}
