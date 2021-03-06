package tars

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/rexshan/tarsgo/tars/transport"
)

//AddServant add dispatch and interface for object.
func AddServant(v dispatch, f interface{}, obj string) {
	fullobjname,err := fullObjName(obj)
	if err != nil {
		TLOG.Debugf("not found fullname: %s %+v", obj,err)
		return
	}
	addServantCommon(v, f, fullobjname, true)
}

//AddServantWithContext add dispatch and interface for object, which have ctx,context
func AddServantWithContext(v dispatch, f interface{}, obj string) {
	fullobjname,err := fullObjName(obj)
	if err != nil {
		TLOG.Debugf("not found fullname: %s %+v", obj,err)
		return
	}
	addServantCommon(v, fullobjname, obj, true)
}

func addServantCommon(v dispatch, f interface{}, obj string, withContext bool) {
	objRunList = append(objRunList, obj)
	cfg, ok := tarsConfig[obj]
	if !ok {
		TLOG.Debug("servant obj name not found ", obj)
		return
	}
	TLOG.Debug("add:", cfg)
	jp := NewTarsProtocol(v, f, withContext)
	s := transport.NewTarsServer(jp, cfg)
	goSvrs[obj] = s
}

//AddHttpServant add http servant handler for obj.
func AddHttpServant(mux *TarsHttpMux, obj string) {
	fullobjname,err := fullObjName(obj)
	if err != nil {
		TLOG.Errorf("找不到对象服务即将关闭")
		return
	}
	cfg, ok := tarsConfig[fullobjname]
	if !ok {
		TLOG.Errorf("servant obj name not found ", obj)
		return
	}
	TLOG.Debug("add http server:", cfg)
	objRunList = append(objRunList, fullobjname)
	appConf := GetServerConfig()
	addrInfo := strings.SplitN(cfg.Address, ":", 2)
	var port int
	if len(addrInfo) == 2 {
		port, _ = strconv.Atoi(addrInfo[1])
	}
	httpConf := &TarsHttpConf{
		Container: appConf.Container,
		AppName:   fmt.Sprintf("%s.%s", appConf.App, appConf.Server),
		Version:   appConf.Version,
		IP:        addrInfo[0],
		Port:      int32(port),
		SetId:     appConf.Setdivision,
	}
	mux.SetConfig(httpConf)
	s := &http.Server{Addr: cfg.Address, Handler: mux}
	httpSvrs[fullobjname] = s
}
