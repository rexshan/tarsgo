//Package statf comment
// This file war generated by tars2go 1.1
// Generated from StatF.tars
package statf

import (
	"fmt"
	"github.com/rexshan/tarsgo/tars/protocol/codec"
)

//ProxyInfo strcut implement
type ProxyInfo struct {
	BFromClient bool `json:"bFromClient"`
}

func (st *ProxyInfo) resetDefault() {
}

//ReadFrom reads  from _is and put into struct.
func (st *ProxyInfo) ReadFrom(_is *codec.Reader) error {
	var err error
	var length int32
	var have bool
	var ty byte
	st.resetDefault()

	err = _is.Read_bool(&st.BFromClient, 0, true)
	if err != nil {
		return err
	}

	_ = length
	_ = have
	_ = ty
	return nil
}

//ReadBlock reads struct from the given tag , require or optional.
func (st *ProxyInfo) ReadBlock(_is *codec.Reader, tag byte, require bool) error {
	var err error
	var have bool
	st.resetDefault()

	err, have = _is.SkipTo(codec.STRUCT_BEGIN, tag, require)
	if err != nil {
		return err
	}
	if !have {
		if require {
			return fmt.Errorf("require ProxyInfo, but not exist. tag %d", tag)
		}
		return nil

	}

	st.ReadFrom(_is)

	err = _is.SkipToStructEnd()
	if err != nil {
		return err
	}
	_ = have
	return nil
}

//WriteTo encode struct to buffer
func (st *ProxyInfo) WriteTo(_os *codec.Buffer) error {
	var err error

	err = _os.Write_bool(st.BFromClient, 0)
	if err != nil {
		return err
	}

	return nil
}

//WriteBlock encode struct
func (st *ProxyInfo) WriteBlock(_os *codec.Buffer, tag byte) error {
	var err error
	err = _os.WriteHead(codec.STRUCT_BEGIN, tag)
	if err != nil {
		return err
	}

	st.WriteTo(_os)

	err = _os.WriteHead(codec.STRUCT_END, 0)
	if err != nil {
		return err
	}
	return nil
}
