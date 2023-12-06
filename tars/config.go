package tars

import (
	"errors"
	"github.com/rexshan/tarsgo/tars/util/endpoint"
	"strings"
)

var svrCfg *serverConfig
var cltCfg *clientConfig
var subCfgChan chan *CfgItem

type ConfigListener func(string, string)

type CfgItem struct {
	FileName string
	Content  string
}

// GetServerConfig : Get server config
func GetServerConfig() *serverConfig {
	Init()
	return svrCfg
}

// GetClientConfig : Get client config
func GetClientConfig() *clientConfig {
	Init()
	return cltCfg
}

type adapterConfig struct {
	Endpoint endpoint.Endpoint
	Protocol string
	Obj      string
	Threads  int
}

type serverConfig struct {
	Node      string
	App       string
	Server    string
	LogPath   string
	LogSize   uint64
	LogNum    uint64
	LogLevel  string
	Version   string
	LocalIP   string
	BasePath  string
	DataPath  string
	config    string
	notify    string
	log       string
	netThread int
	Adapters  map[string]adapterConfig

	Container   string
	Isdocker    bool
	Enableset   bool
	Setdivision string
}

type clientConfig struct {
	Locator                 string
	stat                    string
	property                string
	netconnectionnum        int
	modulename              string
	refreshEndpointInterval int
	reportInterval          int
	AsyncInvokeTimeout      int
	KeepAliveInterval       int
}

func fullObjName(obj string) (string, error) {
	var fullObjName string
	pos := strings.Index(obj, ".")
	if pos > 0 {
		fullObjName = obj
	} else {
		switch {
		case GetServerConfig() == nil:
			return fullObjName, errors.New("nil server config")
		case GetServerConfig().App == "" || GetServerConfig().Server == "":
			return fullObjName, errors.New("empty app or server name")
		}
		fullObjName = strings.Join([]string{
			GetServerConfig().App,
			GetServerConfig().Server,
			obj,
		}, ".")
	}
	return fullObjName, nil
}

func SubTarsConfig(listener ConfigListener, fileName ...string) {
	go func(w ConfigListener, fl ...string) {
		CheckGoPanic()
		for {
			select {
			case item, ok := <-subCfgChan:
				if ok {
					for _, f := range fl {
						if item.FileName == f {
							w(f, item.Content)
						}
					}

				}
			}
		}
	}(listener, fileName...)
}

func noticeLoadConfig(fileName string, content string) {
	select {
	case subCfgChan <- &CfgItem{FileName: fileName, Content: content}:
	default:
		TLOG.Warnf("not consumer config drop it :%s", fileName)
	}
}
