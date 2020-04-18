package tars

import (
	"context"
	"errors"
	"github.com/rexshan/tarsgo/tars/util/current"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rexshan/tarsgo/tars/protocol/res/basef"
	"github.com/rexshan/tarsgo/tars/protocol/res/requestf"
	"github.com/rexshan/tarsgo/tars/util/tools"
)

//ServantProxy is the struct for proxy servants.
type ServantProxy struct {
	sid      int32
	name     string
	comm     *Communicator
	obj      *ObjectProxy
	timeout  int
	hashcode string
	isHash   bool
}

const (
	STATUSERRSTR = "errstring"
)

func NewServantProxy(comm *Communicator, objName string) *ServantProxy {
	s := &ServantProxy{
		comm: comm,
	}
	pos := strings.Index(objName, "@")
	if pos > 0 {
		s.name = objName[0:pos]
	} else {
		s.name = objName
	}
	if s.comm.Client.AsyncInvokeTimeout > 0 {
		s.timeout = s.comm.Client.AsyncInvokeTimeout
	}else {
		s.timeout = 3000  //默认给3000的超时控制
	}
	TLOG.Info("-------------------- Servant time out set %d",s.timeout)
	s.obj = NewObjectProxy(comm, objName)
	return s
}

//TarsSetTimeout sets the timeout for client calling the server , which is in ms.
func (s *ServantProxy) TarsSetTimeout(t int) {
	s.timeout = t
}

//TarsSetHashCode sets the hash code for client calling the server , which is for Message hash code.
func (s *ServantProxy) TarsSetHashCode(code string) {
	if len(code) > 0 {
		s.hashcode = code
		s.isHash = true
	}
}

//Tars_invoke is use for client inoking server.
func (s *ServantProxy) Tars_invoke(ctx context.Context, ctype byte,
	sFuncName string,
	buf []byte,status map[string]string,
	reqContext map[string]string,
	Resp *requestf.ResponsePacket) error {
	defer checkPanic()
	var reqCtxMap map[string]string
	//TODO 重置sid，防止溢出
	atomic.CompareAndSwapInt32(&s.sid, 1<<31-1, 1)

	if reqContext != nil {
		reqCtxMap = reqContext
	}else {
		if v,ok := current.GetRequestContext(ctx);ok {
			reqCtxMap = v
		}else {
			reqCtxMap = make(map[string]string)
		}
	}
	req := requestf.RequestPacket{
		IVersion:     1,
		CPacketType:  0,
		IRequestId:   atomic.AddInt32(&s.sid, 1),
		SServantName: s.name,
		SFuncName:    sFuncName,
		SBuffer:      tools.ByteToInt8(buf),
		ITimeout:     ReqDefaultTimeout,
		Context:      reqCtxMap,
		Status:       status,
	}
	msg := &Message{Req: &req, Ser: s, Obj: s.obj}
	msg.Init()
	if s.isHash {
		TLOG.Debugf("--------------- HashCode %s",s.hashcode)
		msg.setConsistHashCode(s.hashcode)
	}
	var err error
	if allFilters.cf != nil {
		err = allFilters.cf(ctx, msg, s.obj.Invoke, time.Duration(s.timeout)*time.Millisecond)
	} else {
		err = s.obj.Invokes.hashcode(ctx, msg, time.Duration(s.timeout)*time.Millisecond)
	}

	if err != nil {
		TLOG.Errorf("Invoke Obj:%s,fun:%s,error:%s timeout: %d", s.name, sFuncName, err.Error(),s.timeout)
		if msg.Resp == nil {
			ReportStat(msg, 0, 0, 1)
		} else if msg.Status == basef.TARSINVOKETIMEOUT {
			ReportStat(msg, 0, 1, 0)
		} else {
			ReportStat(msg, 0, 0, 1)
		}
		return err
	}
	msg.End()
	*Resp = *msg.Resp
	if errStr,ok := msg.Resp.Status[STATUSERRSTR];ok {
		return errors.New(errStr)
	}
	//report
	ReportStat(msg, 1, 0, 0)
	return err
}


func (s *ServantProxy)GetProxyEndPoints() []string{
	ipPorts := make([]string,0)
	adpMap := s.obj.GetAvailableProxys()
	for ipPort, _ := range adpMap {
		ipPorts = append(ipPorts, ipPort)
	}
	return ipPorts
}

func (s *ServantProxy)ProxyInvoke(ctx context.Context, cType byte, sFuncName string, buf []byte, ipPort string,Resp *requestf.ResponsePacket) error {
	if ipPort == "" {
		return errors.New("not set ip port")
	}
	adpMap := s.obj.GetAvailableProxys()
	adp := adpMap[ipPort]
	atomic.CompareAndSwapInt32(&s.sid, 1<<31-1, 1)
	ctxMap,ok := current.GetRequestContext(ctx)
	if !ok {
		ctxMap = make(map[string]string)
	}
	req := requestf.RequestPacket{
		IVersion:     1,
		CPacketType:  int8(cType),
		IRequestId:   atomic.AddInt32(&s.sid, 1),
		SServantName: s.name,
		SFuncName:    sFuncName,
		SBuffer:      tools.ByteToInt8(buf),
		ITimeout:     ReqDefaultTimeout,
		Context:      ctxMap,
	//	Status:       status,
	}
	msg := &Message{Req: &req, Ser: s, Obj: s.obj,Adp:adp}
	msg.Init()

	var err error
	if allFilters.cf != nil {
		err = allFilters.cf(ctx, msg, s.obj.Invoke, time.Duration(s.timeout)*time.Millisecond)
	} else {
		err = s.obj.Invoke(ctx, msg, time.Duration(s.timeout)*time.Millisecond)
	}
	msg.End()
	if err != nil {
		TLOG.Errorf("Invoke Obj:%s,fun:%s,error:%s", s.name, sFuncName, err.Error())
		if msg.Resp == nil {
			ReportStat(msg, 0, 0, 1)
		} else if msg.Status == basef.TARSINVOKETIMEOUT {
			ReportStat(msg, 0, 1, 0)
		} else {
			ReportStat(msg, 0, 0, 1)
		}
		return err
	}
	*Resp = *msg.Resp
	if errStr,ok := msg.Resp.Status[STATUSERRSTR];ok {
		return errors.New(errStr)
	}
	ReportStat(msg, 1, 0, 0)
	return nil
}

//ServantProxyFactory is ServantProxy' factory struct.
type ServantProxyFactory struct {
	objs map[string]*ServantProxy
	comm *Communicator
	fm   *sync.RWMutex
}

func NewServantProxyFactory(comm *Communicator) *ServantProxyFactory {
	return &ServantProxyFactory{
		comm: comm,
		fm:new(sync.RWMutex),
		objs: make(map[string]*ServantProxy),
	}
}


//GetServantProxy gets the ServanrProxy for the object.
func (o *ServantProxyFactory) GetServantProxy(objName string) *ServantProxy {
	proxy := o.getProxy(objName)
	if proxy != nil {
		return proxy
	}
	return o.createProxy(objName)
}

func (o *ServantProxyFactory)getProxy(objName string)*ServantProxy {
	o.fm.RLock()
	defer o.fm.RUnlock()
	if obj, ok := o.objs[objName]; ok {
		return obj
	}
	return nil
}

func (o *ServantProxyFactory) createProxy(objName string) *ServantProxy {
	o.fm.Lock()
	defer o.fm.Unlock()
	if obj, ok := o.objs[objName]; ok {
		return obj
	}
	obj := NewServantProxy(o.comm, objName)
	o.objs[objName] = obj
	return obj
}
