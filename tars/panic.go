package tars

import (
	"fmt"
	"github.com/rexshan/tarsgo/tars/util/appzaplog"
	"github.com/rexshan/tarsgo/tars/util/appzaplog/zap"
	"os"
	"path/filepath"
	"runtime/debug"
	"time"
)

func checkPanic() {
	if r := recover(); r != nil {
		path, _ := filepath.Abs(filepath.Dir(os.Args[0]))
		os.Chdir(path)
		file, _ := os.Create(fmt.Sprintf("panic.%s", time.Now().Format("20060102-150405")))
		file.WriteString(string(debug.Stack()))
		file.Close()
		os.Exit(-1)
	}
}

func CheckGoPanic() {
	if r := recover(); r != nil {
		appzaplog.Error("painc==============:",zap.String("stack",string(debug.Stack())))
	}
}
