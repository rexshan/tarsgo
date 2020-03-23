package tars

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"gitee.com/bee-circle/tarsgo/tars/protocol/res/basef"
	"gitee.com/bee-circle/tarsgo/tars/protocol/res/requestf"
	"gitee.com/bee-circle/tarsgo/tars/util/rtimer"
)

// ObjectProxy is struct contains proxy information
type ObjectProxy struct {
	manager  *EndpointManager
	comm     *Communicator
	queueLen int32
}

// Init proxy
func (obj *ObjectProxy) Init(comm *Communicator, objName string) {
	obj.comm = comm
	obj.manager = new(EndpointManager)
	obj.manager.Init(objName, obj.comm)
}

// Invoke get proxy information
func (obj *ObjectProxy) Invoke(ctx context.Context, msg *Message, timeout time.Duration) error {
	var adp *AdapterProxy
	if msg.Adp != nil {
		adp = msg.Adp
	}else {
		adp = obj.manager.SelectAdapterProxy(msg)
	}

	if adp == nil {
		return errors.New("no adapter Proxy selected:" + msg.Req.SServantName)
	}
	if obj.queueLen > ObjQueueMax {
		return errors.New("invoke queue is full:" + msg.Req.SServantName)
	}

	if err := adp.breaker.Allow(); err != nil {
		//熔断
		return err
	}

	msg.Adp = adp
	atomic.AddInt32(&obj.queueLen, 1)
	readCh := make(chan *requestf.ResponsePacket, 1)
	adp.resp.Store(msg.Req.IRequestId, readCh)
	defer func() {
		checkPanic()
		atomic.AddInt32(&obj.queueLen, -1)
		adp.resp.Delete(msg.Req.IRequestId)
		close(readCh)
	}()

	err := adp.Send(msg.Req)
	adp.onBreaker(&err)
	if err != nil {
		return err
	}

	select {
	case <-rtimer.After(timeout):
		msg.Status = basef.TARSINVOKETIMEOUT
		adp.failAdd()
		return fmt.Errorf("%s|%s|%d", "request timeout", msg.Req.SServantName, msg.Req.IRequestId)
	case msg.Resp = <-readCh:
		if msg.Resp.IRet != basef.TARSSERVERSUCCESS {
			if msg.Resp.SResultDesc == "" {
				return fmt.Errorf("basef error code %d", msg.Resp.IRet)
			}
			return errors.New(msg.Resp.SResultDesc)
		}
		TLOG.Debug("recv msg succ ", msg.Req.IRequestId)
	}
	return nil
}

func (obj *ObjectProxy)GetAvailableProxys() map[string]*AdapterProxy{
	return obj.manager.GetAvailableProxys()
}

// ObjectProxyFactory is a struct contains proxy information(add)
type ObjectProxyFactory struct {
	objs map[string]*ObjectProxy
	comm *Communicator
	om   *sync.Mutex
}

// Init ObjectProxyFactory
func (o *ObjectProxyFactory) Init(comm *Communicator) {
	o.om = new(sync.Mutex)
	o.comm = comm
	o.objs = make(map[string]*ObjectProxy)
}

// GetObjectProxy get objectproxy
func (o *ObjectProxyFactory) GetObjectProxy(objName string) *ObjectProxy {
	o.om.Lock()
	defer o.om.Unlock()
	if obj, ok := o.objs[objName]; ok {
		return obj
	}
	obj := new(ObjectProxy)
	obj.Init(o.comm, objName)
	o.objs[objName] = obj
	return obj
}
