// @author kordenlu
// @创建时间 2017/07/27 15:57
// 功能描述:

package appzaplog

import (
	"os"
	"path"
)

const logapipath = "/log"

type appZapLogConf struct {
	testenv         bool
	processName     string
	withPid         bool
	logapipath      string
	listenAddr      string
	HostName        string
	ElkTemplateName string //区分不同业务
	logPath         string
}

var defaultLogOptions appZapLogConf = appZapLogConf{
	testenv:         true,
	processName:     path.Base(os.Args[0]),
	withPid:         true,
	logapipath:      logapipath,
	listenAddr:      "127.0.0.1:0",
	ElkTemplateName: path.Base(os.Args[0]),
}

type appZapOption interface {
	apply(*appZapLogConf)
}

type appZapOptionFunc func(*appZapLogConf)

func (self appZapOptionFunc) apply(option *appZapLogConf) {
	self(option)
}

// ListenAddr设置logserver的http端口,用来管理日志级别。
// 默认监听127.0.0.1下的随机端口
func ListenAddr(addr string) appZapOption {
	return appZapOptionFunc(func(option *appZapLogConf) {
		option.listenAddr = addr
	})
}

// LogApiPath设置logserver的api名字。
// 默认为 /log。
func LogApiPath(apipath string) appZapOption {
	return appZapOptionFunc(func(option *appZapLogConf) {
		option.logapipath = apipath
	})
}

// 设置日志路径
func LogPath(path string) appZapOption {
	return appZapOptionFunc(func(option *appZapLogConf) {
		option.logPath = path
	})
}

// WithPid设置日志输出中是否加入pid的项。
// 默认为true。
func WithPid(yes bool) appZapOption {
	return appZapOptionFunc(func(option *appZapLogConf) {
		option.withPid = yes
	})
}

// ProcessName设置输出的进程名字。
// 默认去当前执行文件的名字。
func ProcessName(pname string) appZapOption {
	return appZapOptionFunc(func(option *appZapLogConf) {
		option.processName = pname
	})
}

// TestEnv设置是否测试环境。
// 默认为true,测试环境。
func TestEnv(yes bool) appZapOption {
	return appZapOptionFunc(func(option *appZapLogConf) {
		option.testenv = yes
	})
}

// HostName设置日志机器的ip地址,方便定位。
// 默认不输出。
func HostName(hostname string) appZapOption {
	return appZapOptionFunc(func(option *appZapLogConf) {
		option.HostName = hostname
	})
}

// ElkTmeplateName用来设置输出到elk中的唯一名字。
// 默认不输出。
func ElkTmeplateName(name string) appZapOption {
	return appZapOptionFunc(func(option *appZapLogConf) {
		option.ElkTemplateName = name
	})
}
