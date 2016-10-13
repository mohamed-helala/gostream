package loopy

import (
	"time"
)

type FuncInfo struct {
	FuncIdx    int
	FuncParams Params
}

type TimeInfo struct {
	InTime  time.Time
	OutTime time.Time
}

type MHeader struct {
	FuncInfo map[string]FuncInfo
	TmInfo   map[string]TimeInfo
	Attribs  map[string]T
}

type M struct {
	*MHeader
	Value T
}

type cM struct {
	start, end string
	WRStatus   int
	ERStatus   int
	value      T
}

func (m *M) Clone() T {
	// copy time info and share OpInfo
	if m == nil {
		return nil
	}
	tinfo := map[string]TimeInfo{}
	for k, v := range m.TmInfo {
		tinfo[k] = v
	}

	return &M{&MHeader{FuncInfo: m.FuncInfo, TmInfo: tinfo, Attribs: map[string]T{}}, DeepClone(m.Value)}
}

func DeepClone(m T) T {
	if m == nil {
		return nil
	}
	switch t := m.(type) {
	case Cloneable:
		return t.Clone()
	default:
		// To do: implement deep clone
		return m
	}
}

func DeepDispose(m T) {
	if m == nil {
		return
	}
	switch t := m.(type) {
	case Disposable:
		t.Dispose()
	default:
		// To do: implement deep dispose
	}
}

func (m *M) Dispose() {
	if m != nil && m.Value != nil {
		(m.Value.(Disposable)).Dispose()
	}
}

func (m *M) Exists() bool {
	if m == nil {
		return false
	}
	if m.Value == nil {
		return false
	}
	return true
}

func (m *MHeader) AddTimeInfo(tinfo map[string]TimeInfo) {
	if tinfo == nil {
		return
	}
	for k, v := range tinfo {
		if _, ok := m.TmInfo[k]; !ok {
			m.TmInfo[k] = v
		}
	}
}

func NewMessage(v T) *M {
	return &M{&MHeader{FuncInfo: map[string]FuncInfo{},
		TmInfo: map[string]TimeInfo{}, Attribs: map[string]T{}}, v}
}

func MessageV(x T) T {
	if x == nil {
		return nil
	}
	switch t := x.(type) {
	case *M:
		return t.Value
	default:
		return x
	}
}

func MessageH(x T) *MHeader {
	if x == nil {
		return nil
	}
	switch t := x.(type) {
	case *M:
		return t.MHeader
	default:
		return nil
	}
}

func Message(x T) *M {
	if x == nil {
		return nil
	}
	switch t := x.(type) {
	case *M:
		return t
	default:
		return nil
	}
}

func ReadNewSettings(name string, x T) (int, Params) {
	switch t := x.(type) {
	case *M:
		if t != nil {
			if f, ok := t.FuncInfo[name]; ok {
				return f.FuncIdx, f.FuncParams
			}
		}
	}
	return -1, Params{}
}
