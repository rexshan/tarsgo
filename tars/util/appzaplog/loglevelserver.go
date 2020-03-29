// @author kordenlu
// @创建时间 2017/07/27 17:01
// 功能描述:

package appzaplog

import (
	"github.com/rexshan/tarsgo/tars/util/appzaplog/zap"
	"net"
	"net/http"
)

var setlevelpath string

func logLevelHttpServer(config *appZapLogConf, level zap.AtomicLevel) {

	logservermux := http.NewServeMux()
	// set log level
	logservermux.Handle(config.logapipath, level)
	listener, err := net.Listen("tcp", config.listenAddr)
	if err != nil {
		Fatal("failed Listen: ",
			zap.String("ipport", config.listenAddr),
			zap.Error(err),
		)
	} else {
		setlevelpath = "http://" + listener.Addr().String() + config.logapipath
		Info("open log service success", zap.String("url", setlevelpath))
	}
	go func() {
		err = http.Serve(listener, logservermux)
		if err != nil {
			Fatal("failed ListenAndServe: ",
				zap.String("ipport", net.JoinHostPort("127.0.0.1", "18001")),
				zap.Error(err),
			)
		}
	}()

}
