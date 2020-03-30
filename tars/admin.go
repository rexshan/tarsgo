package tars

import (
	"fmt"
	"github.com/rexshan/tarsgo/tars/util/appzaplog"
	"strings"

	debugutil "github.com/rexshan/tarsgo/tars/util/debug"
	logger "github.com/rexshan/tarsgo/tars/util/rogger"
)

//Admin struct
type Admin struct {
}

//Shutdown shutdown all servant by admin
func (a *Admin) Shutdown() error {
	for obj, s := range goSvrs {
		TLOG.Debug("shutdown", obj)
		//TODO
		go s.Shutdown()
	}
	shutdown <- true
	return nil
}

//Notify handler for cmds from admin
func (a *Admin) Notify(command string) (string, error) {
	cmd := strings.Split(command, " ")
	go reportNotifyInfo("AdminServant::notify:" + cmd[0])
	switch cmd[0] {
	case "tars.viewversion":
		return GetServerConfig().Version, nil
	case "tars.setloglevel":
		switch cmd[1] {
		case "INFO":
			logger.SetLevel(logger.INFO)
			_ = appzaplog.SetLogLevel("info")
		case "WARN":
			logger.SetLevel(logger.WARN)
			_ = appzaplog.SetLogLevel("warn")
		case "ERROR":
			logger.SetLevel(logger.ERROR)
			_ = appzaplog.SetLogLevel("error")
		case "DEBUG":
			_ = appzaplog.SetLogLevel("debug")
			logger.SetLevel(logger.DEBUG)
		case "NONE":
			_ = appzaplog.SetLogLevel("none")
			logger.SetLevel(logger.OFF)
		}
		return fmt.Sprintf("%s succ", command), nil
	case "tars.dumpstack":
		debugutil.DumpStack(true, "stackinfo")
		return fmt.Sprintf("%s succ", command), nil
	case "tars.loadconfig":
		cfg := GetServerConfig()
		remoteConf := NewRConf(cfg.App, cfg.Server, cfg.BasePath)
		c, err := remoteConf.GetConfig(cmd[1])
		if err != nil {
			return fmt.Sprintf("Getconfig Error!: %s", cmd[1]), err
		}
		noticeLoadConfig(cmd[1],c)
		return fmt.Sprintf("Getconfig Success!: %s", cmd[1]), nil

	case "tars.connection":
		return fmt.Sprintf("%s not support now!", command), nil
	default:
		if fn, ok := adminMethods[cmd[0]]; ok {
			return fn(command)
		}
		return fmt.Sprintf("%s not support now!", command), nil
	}
}

//RegisterAdmin register admin functions
func RegisterAdmin(name string, fn adminFn) {
	adminMethods[name] = fn
}


