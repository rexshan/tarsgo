package tcpToRpc

import (
	"context"
	"encoding/json"
	pb "github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
	"github.com/rexshan/tarsgo/tars"
	"github.com/rexshan/tarsgo/tars/protocol/res/requestf"
	"github.com/rexshan/tarsgo/tars/util/tools"
	"time"
	"unicode"
)

type (
	RPCInvoker interface {
		Decode(body json.RawMessage) (input RPCInput, err error)
		Invoke(input RPCInput) (output RPCOutput, err error)
		Encode(output RPCOutput) (body json.RawMessage, err error)
	}

	ToInvoker interface {
		Decode(body json.RawMessage) (input MSGInput, err error)
		Invoke(input MSGInput,Srv,Func string)(output MSGOutput,err error)
		Encode(output MSGOutput) (body json.RawMessage, err error)
	}


	TCPToRPC struct {
		comm *tars.Communicator
	}

	WebsocketToRPC struct {
		comm *tars.Communicator
	}


	MSGToRPC struct {
		comm *tars.Communicator
	}
)

var (
	NotFoundRouterError = errors.New("not found router")
)


func invoke(comm *tars.Communicator, input RPCInput) (out RPCOutput, err error) {
	var (
		rpcStub *tars.ServantProxy
		_status map[string]string
	)
	rpcStub = comm.GetServantProxy(input.Obj)
	input.Func = LcFirst(input.Func)
	_resp := new(requestf.ResponsePacket)
	err = rpcStub.Tars_invoke(context.Background(), 0, input.Func, input.Req, _status, input.Opt, _resp)
	if err != nil {
		tars.TLOG.Errorf("rpc invoke err :%s  %s %+v ctx %v", input.Obj, input.Func, err, input.Opt)
		nErr := Parse(err.Error())
		out.Ret = nErr.Code
		if nErr.Code == Failure {
			out.Desc = "网络繁忙，请稍后再试"
		} else {
			out.Desc = nErr.Detail
		}
		out.Obj = input.Obj
		out.Func = input.Func
		return
	}
	out.Ret = _resp.IRet
	out.Desc = _resp.SResultDesc
	out.Rsp = tools.Int8ToByte(_resp.SBuffer)
	out.Opt = _resp.Context
	out.Obj = input.Obj
	out.Func = input.Func
	return
}

func NewTCPToRPC(c *tars.Communicator) RPCInvoker {
	return &TCPToRPC{comm: c}
}

func (t *TCPToRPC) Decode(body json.RawMessage) (input RPCInput, err error) {
	if err = pb.Unmarshal(body, &input); err != nil {
		tars.TLOG.Errorf("decode err :%+v", err)
		return
	}
	tars.TLOG.Debugf("this decode %s %s %+v", input.Obj, input.Func, input.Opt)
	return
}

func (t *TCPToRPC) Invoke(input RPCInput) (output RPCOutput, err error) {
	start := time.Now().UnixNano() / int64(time.Millisecond)
	out, err := invoke(t.comm, input)
	end := time.Now().UnixNano() / int64(time.Millisecond)
	tars.TLOG.Infof("exec time :%d %s %s %+v", int64(end-start), input.Obj, input.Func, input.Opt)
	return out, err
}

func (t *TCPToRPC) Encode(output RPCOutput) (body json.RawMessage, err error) {
	tars.TLOG.Debugf("encode:%d %s %s %+v", output.Ret, output.Desc, output.Opt)
	if body, err = pb.Marshal(&output); err != nil {
		tars.TLOG.Errorf("encode error :%+v", err)
		return
	}
	return
}

func NewWebsocketToRPC(c *tars.Communicator) RPCInvoker {
	return &WebsocketToRPC{comm: c}
}

func (w *WebsocketToRPC) Decode(body json.RawMessage) (input RPCInput, err error) {
	return input, nil
}

func (w *WebsocketToRPC) Invoke(input RPCInput) (output RPCOutput, err error) {
	return output, nil
}

func (w *WebsocketToRPC) Encode(output RPCOutput) (body json.RawMessage, err error) {
	return nil, nil
}

/*----------------------------MSGToRPC-----------------------------------------*/
func NewMsgToRPC(c *tars.Communicator)ToInvoker {
	return &MSGToRPC{comm:c}
}

func (m *MSGToRPC)Decode(body json.RawMessage)(input MSGInput,err error) {
	if err = pb.Unmarshal(body, &input); err != nil {
		tars.TLOG.Errorf("MSGInput decode err :%+v", err)
		return
	}
	tars.TLOG.Debugf("MSGInput decode %s %s %+v", input.Alias, input.Opt)
	return
}

func (m *MSGToRPC)Invoke(input MSGInput,Srv,Func string)(output MSGOutput,err error) {
	if Srv != "" && Func != "" {
		start := time.Now().UnixNano() / int64(time.Millisecond)
		out, err := m.invokeWidth(m.comm, input,Srv,Func)
		end := time.Now().UnixNano() / int64(time.Millisecond)
		tars.TLOG.Infof("MSGInput exec time :%d %s %s %+v", int64(end-start), Srv,Func, input.Opt)
		return out, err
	}
	err = NotFoundRouterError
	return
}

func (m *MSGToRPC)Encode(output MSGOutput)(body json.RawMessage,err error) {
	tars.TLOG.Debugf("MSGOutput encode:%d %s %s ", output.Ret, output.Desc, output.Opt)
	if body, err = pb.Marshal(&output); err != nil {
		tars.TLOG.Errorf("MSGOutput encode error :%+v", err)
		return
	}
	return
}

func (m *MSGToRPC)invokeWidth(comm *tars.Communicator,input MSGInput,Obj,Func string)(out MSGOutput,err error) {
	var (
		rpcStub *tars.ServantProxy
		_status map[string]string
	)
	rpcStub = comm.GetServantProxy(Obj)
	_resp := new(requestf.ResponsePacket)
	err = rpcStub.Tars_invoke(context.Background(), 0, LcFirst(Func), input.Req, _status, input.Opt, _resp)
	if err != nil {
		tars.TLOG.Errorf("rpc invoke err :%s  %s %+v  %+v", Obj, Func, input.Opt,err)
		nErr := Parse(err.Error())
		out.Ret = nErr.Code
		if nErr.Code == Failure {
			out.Desc = "网络繁忙，请稍后再试"
		} else {
			out.Desc = nErr.Detail
		}
		return
	}
	out.Ret = _resp.IRet
	out.Desc = _resp.SResultDesc
	out.Rsp = tools.Int8ToByte(_resp.SBuffer)
	out.Opt = _resp.Context
	return
}

func LcFirst(str string) string {
	for i, v := range str {
		return string(unicode.ToLower(v)) + str[i+1:]
	}
	return ""
}
