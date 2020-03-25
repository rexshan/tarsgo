package tools

import (
	"github.com/rexshan/tarsgo/tars"
	"net/http"
	"net/http/pprof"
)

func InitPref(pprofBind []string) {
	pprofServeMux := http.NewServeMux()
	pprofServeMux.HandleFunc("/debug/pprof/", pprof.Index)
	pprofServeMux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	pprofServeMux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	pprofServeMux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	for _, addr := range pprofBind {
		go func() {
			if err := http.ListenAndServe(addr, pprofServeMux); err != nil {
				tars.TLOG.Error("http.ListenAndServe(\"%s\", pprofServeMux) error(%v)", addr, err)
				panic(err)
			}
		}()
	}
}