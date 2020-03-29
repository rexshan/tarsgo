package tars

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/rexshan/tarsgo/tars/protocol/res/basef"
	"github.com/rexshan/tarsgo/tars/protocol/res/requestf"
	"github.com/rexshan/tarsgo/tars/util/rtimer"
)

// ObjectProxy is struct contains proxy information
type ObjectProxy struct {
	manager  *EndpointManager
	comm     *Communicator
	queueLen int32
}

func NewObjectProxy(comm *Communicator, objName string) *ObjectProxy {
	return &ObjectProxy{
		comm:    comm,
		manager: NewEndpointManager(objName, comm),
	}
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


