package tars

import (
	"github.com/rexshan/tarsgo/tars/sd"
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
	e.createProxy(*ep)
	a := e.adapters[ep.IPPort]
	e.mlock.Unlock()
	return a
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

func (e *EndpointManager) createProxy(ep endpoint.Endpoint) {
	TLOG.Debug("create adapter:", ep)
	end := endpoint.Endpoint2tars(ep)
	e.adapters[ep.IPPort] = NewAdapterProxy(&end,e.comm)
}

// GetHashProxy returns hash adapter information.
func (e *EndpointManager) GetHashProxy(hashcode int64) *AdapterProxy {
	// very unsafe.
	ep := e.GetHashEndpoint(hashcode)
	if ep == nil {
		return nil
	}
	if adp, ok := e.adapters[ep.IPPort]; ok {
		return adp
	}
	e.createProxy(*ep)
	return e.adapters[ep.IPPort]
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
	if msg.isHash {
		return e.GetHashProxy(msg.hashCode)
	}
	return e.GetNextValidProxy()
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
	if (len(*inactiveEp)) > 0 {
		for _, ep := range *inactiveEp {
			end := endpoint.Tars2endpoint(ep)
			e.pointsSet.Remove(end)
			if a, ok := e.adapters[end.IPPort]; ok {
				delete(e.adapters, end.IPPort)
				a.Close()
			}
		}
	}
	if (len(*activeEp)) > 0 {
		// clean it first,then add back .this action must lead to add lock,
		// but if don't clean may lead to leakage.it's better to use remove.
		e.pointsSet.Clear()
		for _, ep := range *activeEp {
			end := endpoint.Tars2endpoint(ep)
			e.pointsSet.Add(end)
		}
		e.index = e.pointsSet.Slice()
	}
	for k,end := range e.adapters {
		// clean up dirty data
		if !e.pointsSet.Has(end) {
			if a, ok := e.adapters[k]; ok {
				delete(e.adapters, k)
				a.Close()
			}
		}
	}
	e.mlock.Unlock()
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