// @author kordenlu
// @创建时间 2017/07/27 17:17
// 功能描述:

package appzaplog

import "testing"

func TestDefaultConfig(t *testing.T) {
	switch {
	case defaultLogOptions.listenAddr != "127.0.0.1:0":
		t.Error("listenAddr not equal 127.0.0.1:0", defaultLogOptions.listenAddr)
	case defaultLogOptions.logapipath != logapipath:
		t.Error("logapipath not equal ", defaultLogOptions.logapipath)
	case defaultLogOptions.testenv != true:
		t.Error("testenv not equal ", defaultLogOptions.testenv)
	}
}
