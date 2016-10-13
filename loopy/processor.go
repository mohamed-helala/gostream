package loopy

import (
	"sync"
	"time"

	"github.com/satori/go.uuid"
)

//#################################################################
//                   Basic Types
//#################################################################

type T interface{}

type Spout interface {
	Read() T
}

type Cloneable interface {
	Clone() T
}

type Disposable interface {
	Dispose()
}

//#################################################################
//                   Types for Parameters and Functions
//#################################################################

type Parameter struct {
	Value float64
	Low   float64 // lower bound
	High  float64 // upper bound
}

type Function struct {
	FuncName   string
	FuncParams Params
	State      T
	Mapper     func(T, Params) T
	Reducer    func(T, T, Params) (T, T)
}

type Functions []*Function
type Params map[string]Parameter

//#################################################################
//                   Processor Info
//#################################################################

type ProcessorInfo struct {
	Name    string
	Id      uint64
	_type   int
	Funcs   Functions
	FuncIdx int //Current active Function
}

func (p *ProcessorInfo) AddTimeInfo(t int, x T) {

	ct := time.Now()
	switch y := x.(type) {
	case []T:
		p.AddTimeInfo1(t, ct, y)
	case T:
		p.AddTimeInfo1(t, ct, y)
	}
}

func (proc *ProcessorInfo) AddTimeInfo1(t int, ct time.Time, Z ...T) {
	name := proc.Name
	// record time
	for _, x := range Z {
		if x == nil {
			continue
		}
		switch c := x.(type) {
		case *M:
			var (
				tinfo TimeInfo
				ok    bool
			)
			if c == nil {
				continue
			}
			if tinfo, ok = c.TmInfo[name]; !ok {
				tinfo = TimeInfo{}
			}
			if t == PROC_ENTER_TIME || t == PROC_BOTH_TIME {
				tinfo.InTime = ct
			}
			if t == PROC_LEAVE_TIME || t == PROC_BOTH_TIME {
				tinfo.OutTime = ct
			}
			c.TmInfo[name] = tinfo
		}
	}
}

func (proc *ProcessorInfo) UpdateSettings(x T) {
	name := proc.Name

	f_idx, params := ReadNewSettings(name, x)
	if f_idx != -1 {
		proc.FuncIdx = f_idx
		proc.Funcs[proc.FuncIdx].FuncParams = params
	}

}

//#################################################################
//                   Processor Stack
//#################################################################

type Element struct {
	value *ProcessorInfo
	next  *Element
}

type ProcessorStack struct {
	top   *Element
	size  int
	mutex *sync.Mutex
}

// Return the stack's length
func (s *ProcessorStack) Len() int {
	return s.size
}

// Push a new element onto the stack
func (s *ProcessorStack) Push(v *ProcessorInfo) {
	s.top = &Element{value: v, next: s.top}
	s.size++
}

// Remove the top element from the stack and return it's value
// If the stack is empty, return nil
func (s *ProcessorStack) Pop() (value *ProcessorInfo) {
	if s.size > 0 {
		value, s.top = s.top.value, s.top.next
		s.size--
		return
	}
	return nil
}

func (s *ProcessorStack) ExecStack(x T) T {
	if x == nil {
		return nil
	}
	y := x
	for e := s.top; e != nil; e = e.next {
		pi := e.value
		pi.AddTimeInfo(PROC_ENTER_TIME, y)
		if e.value._type == OP_REDUCE {
			s.mutex.Lock()
			e.value.Funcs[e.value.FuncIdx].State, y =
				e.value.Funcs[e.value.FuncIdx].Reducer(e.value.Funcs[e.value.FuncIdx].State, y,
					e.value.Funcs[e.value.FuncIdx].FuncParams)
			s.mutex.Unlock()
		} else {
			y = e.value.Funcs[e.value.FuncIdx].Mapper(y,
				e.value.Funcs[e.value.FuncIdx].FuncParams)
		}
		pi.AddTimeInfo(PROC_ENTER_TIME, y)
	}
	return y
}

//#################################################################
//                   Processor
//#################################################################

type Composite struct {
	InProcs  []*Processor
	OutProcs []*Processor
}

type Processor struct {
	*ProcessorInfo
	WRStatus       int
	ERStatus       int
	InStack        *ProcessorStack
	OutStack       *ProcessorStack
	Inputs         []chan T
	Outputs        []chan T
	F              func(inputs ...chan T) []chan T
	G              *OGraph
	C              *sync.Cond
	Composite      *Composite
	IsComposite    bool
	IsGraphRemoved bool
}

func NewProcessor(g *OGraph, inchans []chan T, outchans []chan T, _type int) *Processor {
	return &Processor{ProcessorInfo: &ProcessorInfo{FuncIdx: -1, _type: _type, Name: uuid.NewV4().String(), Id: g.seq.Read()},
		Inputs: inchans, Outputs: outchans,
		InStack:        &ProcessorStack{size: 0, mutex: &sync.Mutex{}},
		OutStack:       &ProcessorStack{size: 0, mutex: &sync.Mutex{}},
		C:              sync.NewCond(&sync.Mutex{}),
		ERStatus:       ST_RUN,
		G:              g,
		WRStatus:       ST_RESUME,
		Composite:      nil,
		IsComposite:    false,
		IsGraphRemoved: false}
}

func (p *Processor) Wait() bool {
	if p.WRStatus == ST_REQWAIT {
		p.C.L.Lock()
		p.WRStatus = ST_WAIT
		p.C.Wait()
		p.WRStatus = ST_RESUME
		p.C.L.Unlock()
		if p.ERStatus == ST_EXIT {
			return false
		}
	}
	return true
}

func (p *Processor) Resume(newERState int) {
	if p.WRStatus == ST_WAIT {
		p.C.L.Lock()
		p.ERStatus = newERState
		p.C.Broadcast()
		p.C.L.Unlock()
	}
}

func (p *Processor) WaitMessage(x T, chans ...chan T) (bool, bool) {
	if x == nil {
		return false, true
	}
	switch t := x.(type) {
	case *cM:
		if t.end == "" || t.end != p.Name {
			for _, c := range chans {
				c <- x
			}
		}
		p.ERStatus = t.ERStatus
		p.WRStatus = t.WRStatus
		return true, p.Wait()
	default:
	}
	return false, true
}

func (p *Processor) ParseAttrib(attribs []T) *Processor {
	var pproc *Processor = nil
	st := 0
	if len(attribs) > 0 {
		switch t := attribs[0].(type) {
		case string:
			p.Name = t
			st = 1
		}
	}
	for i := st; i < len(attribs); i += 2 {
		switch attribs[i].(int) {
		case OP_ATTRIB_NAME:
			p.Name = attribs[i+1].(string)
		case OP_ATTRIB_FUNC_IDX:
			p.FuncIdx = attribs[i+1].(int)
		case OP_ATTRIB_ER_STATUS:
			p.ERStatus = attribs[i+1].(int)
		case OP_ATTRIB_WR_STATUS:
			p.WRStatus = attribs[i+1].(int)
		case OP_ATTRIB_PREV_PROC:
			pproc = attribs[i+1].(*Processor)
		case OP_ATTRIB_GRAPH_REMOVED:
			p.IsGraphRemoved = attribs[i+1].(bool)
		}
	}
	return pproc
}
