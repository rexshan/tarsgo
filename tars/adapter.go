package tars

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/rexshan/tarsgo/tars/protocol/res/basef"
	"github.com/rexshan/tarsgo/tars/util/tools"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rexshan/tarsgo/tars/protocol/codec"
	"github.com/rexshan/tarsgo/tars/protocol/res/endpointf"
	"github.com/rexshan/tarsgo/tars/protocol/res/requestf"
	"github.com/rexshan/tarsgo/tars/transport"
)

// AdapterProxy : Adapter proxy
type AdapterProxy struct {
	resp              sync.Map
	point             *endpointf.EndpointF
	tarsClient        *transport.TarsClient
	comm              *Communicator
	servantProxy      *ServantProxy
	failCount         int32
	lastFailCount     int32
	sendCount         int32
	successCount      int32
	lastSuccessTime   int64
	status            bool
	closed            bool
	breaker           tools.Breaker
	lastKeepAliveTime int64
}

func NewAdapterProxy(point *endpointf.EndpointF, comm *Communicator) *AdapterProxy {
	strPort := strconv.FormatInt(int64(point.Port), 10)
	brkGroup := tools.NewGroup(nil)
	brk := brkGroup.Get(net.JoinHostPort(point.Host, strPort))
	c := &AdapterProxy{
		comm:    comm,
		breaker: brk,
		point:   point,
	}
	proto := "tcp"
	if point.Istcp == 0 {
		proto = "udp"
	}
	netThread, _ := c.comm.GetPropertyInt("netthread")

	conf := &transport.TarsClientConf{
		Proto:        proto,
		NumConnect:   netThread,
		QueueLen:     ClientQueueLen,
		IdleTimeout:  ClientIdleTimeout,
		ReadTimeout:  ClientReadTimeout,
		WriteTimeout: ClientWriteTimeout,
	}
	c.tarsClient = transport.NewTarsClient(fmt.Sprintf("%s:%d", point.Host, point.Port), c, conf)
	c.status = true
	go c.checkActive()
	return c
}

// ParsePackage : Parse packet from bytes
func (c *AdapterProxy) ParsePackage(buff []byte) (int, int) {
	return TarsRequest(buff)
}

// Recv : Recover read channel when closed for timeout
func (c *AdapterProxy) Recv(pkg []byte) {
	defer func() {
		if err := recover(); err != nil {
			TLOG.Error("recv pkg painc:", err)
		}
	}()
	packet := requestf.ResponsePacket{}
	err := packet.ReadFrom(codec.NewReader(pkg))
	if err != nil {
		TLOG.Error("decode packet error", err.Error())
		return
	}
	chIF, ok := c.resp.Load(packet.IRequestId)
	if ok {
		ch := chIF.(chan *requestf.ResponsePacket)
		TLOG.Debug("IN:", packet)
		ch <- &packet
	} else {
		TLOG.Error("timeout resp,drop it:", packet.IRequestId)
	}
}

// Send : Send packet
func (c *AdapterProxy) Send(req *requestf.RequestPacket) error {
	TLOG.Debug("send req:", req.IRequestId)
	c.sendAdd()
	sbuf := bytes.NewBuffer(nil)
	sbuf.Write(make([]byte, 4))
	os := codec.NewBuffer()
	req.WriteTo(os)
	bs := os.ToBytes()
	sbuf.Write(bs)
	len := sbuf.Len()
	binary.BigEndian.PutUint32(sbuf.Bytes(), uint32(len))
	return c.tarsClient.Send(sbuf.Bytes())
}

func (c *AdapterProxy) onBreaker(err *error) {
	if err != nil && *err != nil {
		c.breaker.MarkFailed()
	} else {
		c.breaker.MarkSuccess()
	}
}

func (c *AdapterProxy) Available() bool {
	return c.breaker.Allow() == nil
}

// GetPoint : Get an endpoint
func (c *AdapterProxy) GetPoint() *endpointf.EndpointF {
	return c.point
}

// Close : Close the client
func (c *AdapterProxy) Close() {
	c.tarsClient.Close()
	c.closed = true
}

func (c *AdapterProxy) sendAdd() {
	atomic.AddInt32(&c.sendCount, 1)
}

func (c *AdapterProxy) successAdd() {
	now := time.Now().Unix()
	atomic.SwapInt64(&c.lastSuccessTime, now)
	atomic.AddInt32(&c.successCount, 1)
	atomic.SwapInt32(&c.lastFailCount, 0)
}

func (c *AdapterProxy) failAdd() {
	atomic.AddInt32(&c.lastFailCount, 1)
	atomic.AddInt32(&c.failCount, 1)
}

func (c *AdapterProxy) reset() {
	now := time.Now().Unix()
	atomic.SwapInt32(&c.sendCount, 0)
	atomic.SwapInt32(&c.failCount, 0)
	atomic.SwapInt64(&c.lastKeepAliveTime, now)
}

func (c *AdapterProxy) checkActive() {
	for range time.NewTicker(AdapterProxyTicker).C {
		if c.closed {
			return
		}
		c.doKeepAlive()
	}
}

func (c *AdapterProxy) doKeepAlive() {
	if c.closed {
		return
	}
	now := time.Now().Unix()
	if now-c.lastKeepAliveTime < int64(c.comm.Client.KeepAliveInterval/1000) {
		return
	}
	c.lastKeepAliveTime = now

	req := requestf.RequestPacket{
		IVersion:     1,
		CPacketType:  basef.TARSONEWAY,
		IRequestId:   c.servantProxy.genRequestID(),
		SServantName: c.servantProxy.name,
		SFuncName:    "tars_ping",
		ITimeout:     int32(c.servantProxy.timeout),
	}
	msg := &Message{Req: &req, Ser: c.servantProxy}
	msg.Init()
	msg.Adp = c
	if err := c.Send(msg.Req); err != nil {
		c.failAdd()
		return
	}
	c.successAdd()
}
