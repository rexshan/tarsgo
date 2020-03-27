//服务端启动初始化，解析命令行参数，解析配置

package tars

import (
	"flag"
	"fmt"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/rexshan/tarsgo/tars/protocol/res/adminf"
	"github.com/rexshan/tarsgo/tars/transport"
	"github.com/rexshan/tarsgo/tars/util/conf"
	"github.com/rexshan/tarsgo/tars/util/endpoint"
	"github.com/rexshan/tarsgo/tars/util/rogger"
	"github.com/rexshan/tarsgo/tars/util/tools"
)

var tarsConfig map[string]*transport.TarsServerConf
var goSvrs map[string]*transport.TarsServer
var httpSvrs map[string]*http.Server
var shutdown chan bool
var serList []string
var objRunList []string

//TLOG is the logger for tars framework.
var TLOG = rogger.GetLogger("TLOG")
var initOnce sync.Once

type adminFn func(string) (string, error)

var (
	adminMethods map[string]adminFn
	ConfPath *string
)

func init() {
	tarsConfig = make(map[string]*transport.TarsServerConf)
	goSvrs = make(map[string]*transport.TarsServer)
	httpSvrs = make(map[string]*http.Server)
	shutdown = make(chan bool, 1)
	adminMethods = make(map[string]adminFn)
	rogger.SetLevel(rogger.ERROR)
	Init()
}

//Init should run before GetServerConfig & GetClientConfig , or before run
// and Init should be only run once
func Init() {
	initOnce.Do(initConfig)
}

func initConfig() {
	ConfPath = flag.String("config", "", "init config path")
	flag.Parse()
	if len(*ConfPath) == 0 {
		return
	}
	c, err := conf.NewConf(*ConfPath)
	if err != nil {
		TLOG.Error("open app config fail")
	}
	//Config.go
	//Server
	svrCfg = new(serverConfig)
	if c.GetString("/tars/application<enableset>") == "Y" {
		svrCfg.Enableset = true
		svrCfg.Setdivision = c.GetString("/tars/application<setdivision>")
	}
	sMap := c.GetMap("/tars/application/server")
	svrCfg.Node = sMap["node"]
	svrCfg.App = sMap["app"]
	svrCfg.Server = sMap["server"]
	svrCfg.LocalIP = sMap["localip"]
	//svrCfg.Container = c.GetString("/tars/application<container>")
	//init log
	svrCfg.LogPath = sMap["logpath"]
	svrCfg.LogSize = tools.ParseLogSizeMb(sMap["logsize"])
	svrCfg.LogNum = tools.ParseLogNum(sMap["lognum"])
	svrCfg.LogLevel = sMap["logLevel"]
	svrCfg.config = sMap["config"]
	svrCfg.notify = sMap["notify"]
	svrCfg.BasePath = sMap["basepath"]
	svrCfg.DataPath = sMap["datapath"]
	//svrCfg.netThread = sMap["netthread"]
	svrCfg.netThread = c.GetInt("/tars/application/server<netthread>")

	svrCfg.log = sMap["log"]
	//add version info
	svrCfg.Version = TarsVersion
	//add adapters config
	svrCfg.Adapters = make(map[string]adapterConfig)

	rogger.SetLevel(rogger.StringToLevel(svrCfg.LogLevel))
	TLOG.SetFileRoller(svrCfg.LogPath+"/"+svrCfg.App+"/"+svrCfg.Server, 10, 100)

	//client
	cltCfg = new(clientConfig)
	cMap := c.GetMap("/tars/application/client")
	cltCfg.Locator = cMap["locator"]
	cltCfg.stat = cMap["stat"]
	cltCfg.property = cMap["property"]
	cltCfg.AsyncInvokeTimeout = c.GetInt("/tars/application/client<async-invoke-timeout>")
	cltCfg.refreshEndpointInterval = c.GetInt("/tars/application/client<refresh-endpoint-interval>")
	serList = c.GetDomain("/tars/application/server")

	for _, adapter := range serList {
		endString := c.GetString("/tars/application/server/" + adapter + "<endpoint>")
		end := endpoint.Parse(endString)
		svrObj := c.GetString("/tars/application/server/" + adapter + "<servant>")
		protocol := c.GetString("/tars/application/server/" + adapter + "<protocol>")
		threads := c.GetInt("/tars/application/server/" + adapter + "<threads>")
		svrCfg.Adapters[adapter] = adapterConfig{end, protocol, svrObj, threads}
		host := end.Host
		if end.Bind != "" {
			host = end.Bind
		}
		conf := &transport.TarsServerConf{
			Proto:         end.Proto,
			Address:       fmt.Sprintf("%s:%d", host, end.Port),
			MaxInvoke:     int32(MaxInvoke),
			AcceptTimeout: AcceptTimeout,
			ReadTimeout:   ReadTimeout,
			WriteTimeout:  WriteTimeout,
			HandleTimeout: HandleTimeout,
			IdleTimeout:   IdleTimeout,

			TCPNoDelay:     TCPNoDelay,
			TCPReadBuffer:  TCPReadBuffer,
			TCPWriteBuffer: TCPWriteBuffer,
		}

		tarsConfig[svrObj] = conf
	}
	TLOG.Debug("config add ", tarsConfig)
	localString := c.GetString("/tars/application/server<local>")
	localpoint := endpoint.Parse(localString)

	adminCfg := &transport.TarsServerConf{
		Proto:          "tcp",
		Address:        fmt.Sprintf("%s:%d", localpoint.Host, localpoint.Port),
		MaxInvoke:      int32(MaxInvoke),
		AcceptTimeout:  AcceptTimeout,
		ReadTimeout:    ReadTimeout,
		WriteTimeout:   WriteTimeout,
		HandleTimeout:  HandleTimeout,
		IdleTimeout:    IdleTimeout,
		QueueCap:       QueueCap,
		TCPNoDelay:     TCPNoDelay,
		TCPReadBuffer:  TCPReadBuffer,
		TCPWriteBuffer: TCPWriteBuffer,
	}

	tarsConfig["AdminObj"] = adminCfg
	svrCfg.Adapters["AdminAdapter"] = adapterConfig{localpoint, "tcp", "AdminObj", 1}
}

func initFrameWorkClient(c *Communicator) {
	if cc := GetClientConfig(); cc != nil {
		go initReport(c, cc.stat)
	}
	return
}

//Run the application
func Run() {
	Init()
	TLOG.Debug("Run the application ")
	comm := startFrameWorkComm()
	initFrameWorkClient(comm)
	TLOG.Debug("config add111999999999 ")
	<-statInited
	TLOG.Debug("config add111 ")
	// add adminF
	adf := new(adminf.AdminF)
	ad := new(Admin)
	addServantCommon(adf, ad, "AdminObj",false)

	for _, obj := range objRunList {
		if s, ok := httpSvrs[obj]; ok {
			go func(obj string) {
				TLOG.Info(obj, "http server start")
				err := s.ListenAndServe()
				if err != nil {
					TLOG.Info(obj, "server start failed", err)
					os.Exit(1)
				}
			}(obj)
			continue
		}

		s := goSvrs[obj]
		if s == nil {
			TLOG.Debug("Obj not found", obj)
			break
		}
		TLOG.Debug("Run", obj, s.GetConfig())
		go func(obj string) {
			err := s.Serve()
			if err != nil {
				fmt.Println(obj, "server start failed", err)
				os.Exit(1)
			}
		}(obj)
	}
	go reportNotifyInfo("restart")
	mainloop()
}

func mainloop() {
	ha := new(NodeFHelper)
	node := GetServerConfig().Node
	app := GetServerConfig().App
	server := GetServerConfig().Server
	ha.SetNodeInfo(startFrameWorkComm(), node, app, server)

	go ha.ReportVersion(GetServerConfig().Version)
	go ha.KeepAlive("") //first start
	loop := time.NewTicker(MainLoopTicker)
	for {
		select {
		case <-shutdown:
			reportNotifyInfo("stop")
			return
		case <-loop.C:
			for name, adapter := range svrCfg.Adapters {
				if adapter.Protocol == "not_tars" {
					//TODO not_tars support
					ha.KeepAlive(name)
					continue
				}
				if s, ok := goSvrs[adapter.Obj]; ok {
					if !s.IsZombie(ZombileTimeout) {
						ha.KeepAlive(name)
					}
				}

				if _, ok := httpSvrs[adapter.Obj]; ok {
					ha.KeepAlive(name)
					continue
				}
			}

		}
	}
}
