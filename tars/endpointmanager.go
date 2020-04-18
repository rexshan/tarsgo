package tars

import (
	"github.com/rexshan/tarsgo/tars/sd"
	"github.com/rexshan/tarsgo/tars/util/appzaplog"
	"github.com/rexshan/tarsgo/tars/util/appzaplog/zap"
	"github.com/rexshan/tarsgo/tars/util/hash"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/rexshan/tarsgo/tars/protocol/res/endpointf"
	"github.com/rexshan/tarsgo/tars/util/endpoint"
	"github.com/rexshan/tarsgo/tars/util/set"
)

// EndpointManager is a struct which contains endpoint information.
type EndpointManager struct {
	objName         string
	directproxy     bool
	adapters        map[string]*AdapterProxy
	index           []interface{} //cache the set
	pointsSet       *set.Set
	comm            *Communicator
	mlock           *sync.Mutex
	refreshInterval int
	pos             int32
	depth           int32
	consistadapters *hash.HashRing
}



func (e *EndpointManager) setObjName(objName string) {
	if objName == "" {
		return
	}
	pos := strings.Index(objName, "@")
	if pos > 0 {
		//[direct]
		e.objName = objName[0:pos]
		endpoints := objName[pos+1:]
		e.directproxy = true
		for _, end := range strings.Split(endpoints, ":") {
			e.pointsSet.Add(endpoint.Parse(end))
		}
		e.index = e.pointsSet.Slice()

		for _, v := range e.index {
			ep := v.(endpoint.Endpoint)
			e.consistadapters = e.consistadapters.AddNode(ep.IPPort)
		}
	} else {
		//[proxy] TODO singleton
		TLOG.Debug("proxy mode:", objName)
		e.objName = objName
		e.findAndSetObj(startFrameWorkComm().sd)
		go func() {
			loop := time.NewTicker(time.Duration(e.refreshInterval) * time.Millisecond)
			for range loop.C {
				//TODO exit
				e.findAndSetObj(startFrameWorkComm().sd)
			}
		}()
	}
}

func NewEndpointManager(objName string, comm *Communicator) *EndpointManager {
	e := &EndpointManager{
		comm:            comm,
		mlock:           new(sync.Mutex),
		adapters:        make(map[string]*AdapterProxy),
		refreshInterval: comm.Client.refreshEndpointInterval,
		pointsSet:set.NewSet(),
		directproxy:false,
		pos:0,
		depth:0,
		consistadapters: hash.New([]string{}),
	}
	e.setObjName(objName)
	return e
}

// Init endpoint struct.
func (e *EndpointManager) Init(objName string, comm *Communicator) error {
	e.comm = comm
	e.mlock = new(sync.Mutex)
	e.adapters = make(map[string]*AdapterProxy)
	e.pointsSet = set.NewSet()
	e.directproxy = false
	e.refreshInterval = comm.Client.refreshEndpointInterval
	e.pos = 0
	e.depth = 0
	//ObjName要放到最后初始化
	e.setObjName(objName)
	return nil
}

// GetNextValidProxy returns polling adapter information.
func (e *EndpointManager) GetNextValidProxy() *AdapterProxy {
	e.mlock.Lock()
	ep := e.GetNextEndpoint()
	if ep == nil {
		e.mlock.Unlock()
		return nil
	}
	if adp, ok := e.adapters[ep.IPPort]; ok {
		// returns nil if recursively all nodes have not found an available node.
		if adp.status {
			e.mlock.Unlock()
			return adp
		} else if e.depth > e.pointsSet.Len() {
			e.mlock.Unlock()
			return nil
		} else {
			e.depth++
			e.mlock.Unlock()
			return e.GetNextValidProxy()
		}
	}
	adp,err := e.createProxy(ep.IPPort)
	e.mlock.Unlock()
	if err != nil {
		return nil
	}
	return adp
}

// GetNextEndpoint returns the endpoint basic information.
func (e *EndpointManager) GetNextEndpoint() *endpoint.Endpoint {
	length := len(e.index)
	if length <= 0 {
		return nil
	}
	var ep endpoint.Endpoint
	e.pos = (e.pos + 1) % int32(length)
	ep = e.index[e.pos].(endpoint.Endpoint)
	return &ep
}

// GetAllEndpoint returns all endpoint information as a array(support not tars service).
func (e *EndpointManager) GetAllEndpoint() []*endpoint.Endpoint {
	es := make([]*endpoint.Endpoint, len(e.index))
	e.mlock.Lock()
	defer e.mlock.Unlock()
	for i, v := range e.index {
		e := v.(endpoint.Endpoint)
		es[i] = &e
	}
	return es
}
/*
func (e *EndpointManager) createProxy(ep endpoint.Endpoint) {
	TLOG.Debug("create adapter:", ep)
	end := endpoint.Endpoint2tars(ep)
	e.adapters[ep.IPPort] = NewAdapterProxy(&end,e.comm)
}
*/

func (e *EndpointManager) GetHashProxy(hashcode string) *AdapterProxy {
	intHashCode,err := strconv.ParseInt(hashcode,10,64)
	if err != nil {
		return nil
	}
	e.mlock.Lock()
	ep := e.GetHashEndpoint(intHashCode)
	if ep == nil {
		e.mlock.Unlock()
		return nil
	}
	if adp, ok := e.adapters[ep.IPPort]; ok {
		e.mlock.Unlock()
		return adp
	}
	adp,err := e.createProxy(ep.IPPort)
	e.mlock.Unlock()
	if err != nil {
		return nil
	}
	return adp
}


func (e *EndpointManager)GetConsistHashProxy(hashcode string) *AdapterProxy {
	var (
		localadapter *AdapterProxy
		ipport string
		ok bool
		err error
	)
	e.mlock.Lock()
	ipport,ok = e.consistadapters.GetNode(hashcode)
	if !ok {
		e.mlock.Unlock()
		TLOG.Warnf("GetConsistHashProxy not found %s",hashcode)
		return localadapter
	}

	if localadapter, ok = e.adapters[ipport]; ok {
		e.mlock.Unlock()
		if localadapter.Available(){
			return localadapter
		}
		TLOG.Warnf("GetConsistHashProxy not found %s %s",hashcode,ipport)
		return nil
	}

	localadapter,err = e.createProxy(ipport)
	e.mlock.Unlock()
	if err != nil {
		TLOG.Warnf("GetConsistHashProxy createProxy %s %s",hashcode,ipport)
		return nil
	}
	return localadapter
}

func (e *EndpointManager)createProxy(ipport string)(*AdapterProxy,error) {
	host,port,err := net.SplitHostPort(ipport)
	if err != nil {
		TLOG.Warnf("SplitHostPort not found %s ",ipport)
		return nil,err
	}
	intPort,err := strconv.ParseInt(port,10,64)
	if err != nil {
		TLOG.Warnf("ParseInt not found %s",ipport)
		return nil,err
	}
	ed := endpointf.EndpointF{
		Host:host,
		Port:int32(intPort),
		Istcp:1,
	}
	e.adapters[ipport] = NewAdapterProxy(&ed, e.comm)
	return e.adapters[ipport],nil
}

// GetHashEndpoint returns hash endpoint information.
func (e *EndpointManager) GetHashEndpoint(hashcode int64) *endpoint.Endpoint {
	length := len(e.index)
	if length <= 0 {
		return nil
	}
	pos := hashcode % int64(length)
	ep := e.index[pos].(endpoint.Endpoint)
	return &ep
}

// SelectAdapterProxy returns selected adapter.
func (e *EndpointManager) SelectAdapterProxy(msg *Message) *AdapterProxy {
	switch  {
	case msg.isConsistHash:
		return e.GetConsistHashProxy(msg.hashCode)
	case msg.isHash:
		return e.GetHashProxy(msg.hashCode)
	default:
		return e.GetNextValidProxy()
	}
}

func (e *EndpointManager) findAndSetObj(sdhelper sd.SDHelper) {
	if sdhelper == nil {
		return
	}
	activeEp := new([]endpointf.EndpointF)
	inactiveEp := new([]endpointf.EndpointF)
	var setable, ok bool
	var setID string
	var ret int32
	var err error
	if setable, ok = e.comm.GetPropertyBool("enableset"); ok {
		setID, _ = e.comm.GetProperty("setdivision")
	}
	if setable {
		ret, err = sdhelper.FindObjectByIdInSameSet(e.objName, setID, activeEp, inactiveEp)
	} else {
		ret, err = sdhelper.FindObjectByIdInSameGroup(e.objName, activeEp, inactiveEp)
	}
	if err != nil {
		TLOG.Errorf("find obj end fail: %s %v", e.objName,err.Error())
		return
	}
	TLOG.Debug("find obj endpoint:", e.objName, ret, *activeEp, *inactiveEp)

	e.mlock.Lock()
	defer e.mlock.Unlock()
	if (len(*inactiveEp)) > 0 {
		for _, ep := range *inactiveEp {
			end := endpoint.Tars2endpoint(ep)
			e.pointsSet.Remove(end)
			if a, ok := e.adapters[end.IPPort]; ok {
				delete(e.adapters, end.IPPort)
				appzaplog.Info("----------------remote ep", zap.String("endpoint", end.IPPort))
				e.consistadapters = e.consistadapters.RemoveNode(end.IPPort)
				a.Close()
			}
		}
	}
	if (len(*activeEp)) > 0 {
		e.pointsSet.Clear()
		for _, ep := range *activeEp {
			end := endpoint.Tars2endpoint(ep)
			e.pointsSet.Add(end)
			appzaplog.Info("----------------add ep", zap.String("endpoint", end.IPPort))
			e.consistadapters = e.consistadapters.AddNode(end.IPPort)
		}
		e.index = e.pointsSet.Slice()
	}
}


func (e *EndpointManager)GetAvailableProxys() (list map[string]*AdapterProxy) {
	enlist := e.GetAllEndpoint()
	list = make(map[string]*AdapterProxy)
	for _,ed := range enlist {
		if e.adapters[ed.IPPort].status {
			list[ed.IPPort] = e.adapters[ed.IPPort]
		}
	}
	return
}