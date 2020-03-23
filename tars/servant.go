package tars

import (
	"context"
	"errors"
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
	hashcode int64
	isHash   bool
}

//Init init the ServantProxy struct.
func (s *ServantProxy) Init(comm *Communicator, objName string) {
	pos := strings.Index(objName, "@")
	if pos > 0 {
		s.name = objName[0:pos]
	} else {
		s.name = objName
	}
	s.comm = comm
	of := new(ObjectProxyFactory)
	of.Init(comm)
	s.timeout = s.comm.Client.AsyncInvokeTimeout
	s.obj = of.GetObjectProxy(objName)
}

//TarsSetTimeout sets the timeout for client calling the server , which is in ms.
func (s *ServantProxy) TarsSetTimeout(t int) {
	s.timeout = t
}

//TarsSetHashCode sets the hash code for client calling the server , which is for Message hash code.
func (s *ServantProxy) TarsSetHashCode(code int64) {
	s.hashcode = code
	s.isHash = true
}

//Tars_invoke is use for client inoking server.
func (s *ServantProxy) Tars_invoke(ctx context.Context, ctype byte,
	sFuncName string,
	buf []byte,status map[string]string,
	reqContext map[string]string,
	Resp *requestf.ResponsePacket) error {
	var sndCtx map[string]string
	defer checkPanic()
	//TODO 重置sid，防止溢出
	atomic.CompareAndSwapInt32(&s.sid, 1<<31-1, 1)
	if ctxMap,ok := FromOutgoingContext(ctx);ok {
		sndCtx = ctxMap
	}else {
		sndCtx = reqContext
	}
	req := requestf.RequestPacket{
		IVersion:     1,
		CPacketType:  0,
		IRequestId:   atomic.AddInt32(&s.sid, 1),
		SServantName: s.name,
		SFuncName:    sFuncName,
		SBuffer:      tools.ByteToInt8(buf),
		ITimeout:     ReqDefaultTimeout,
		Context:      sndCtx,
		Status:       status,
	}
	msg := &Message{Req: &req, Ser: s, Obj: s.obj}
	msg.Init()
	if s.isHash {
		msg.SetHashCode(s.hashcode)
	}
	var err error
	if allFilters.cf != nil {
		err = allFilters.cf(ctx, msg, s.obj.Invoke, time.Duration(s.timeout)*time.Millisecond)
	} else {
		err = s.obj.Invoke(ctx, msg, time.Duration(s.timeout)*time.Millisecond)
	}
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
	msg.End()
	*Resp = *msg.Resp
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
	ctxMap,_ := FromOutgoingContext(ctx)

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
	f := &ServantProxyFactory{}
	f.Init(comm)
	return f
}

//Init init the  ServantProxyFactory.
func (o *ServantProxyFactory) Init(comm *Communicator) {
	o.fm = new(sync.RWMutex)
	o.comm = comm
	o.objs = make(map[string]*ServantProxy)
}

//GetServantProxy gets the ServanrProxy for the object.
func (o *ServantProxyFactory) GetServantProxy(objName string) *ServantProxy {
	o.fm.Lock()
	if obj, ok := o.objs[objName]; ok {
		o.fm.Unlock()
		return obj
	}
	o.fm.Unlock()
	obj := new(ServantProxy)
	obj.Init(o.comm, objName)
	o.fm.Lock()
	o.objs[objName] = obj
	o.fm.Unlock()
	return obj
}
