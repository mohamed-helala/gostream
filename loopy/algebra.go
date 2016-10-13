package loopy

import "time"

//#############################################################
// 1. Data processing operators
//##############################################################

// Source
// It joins `group` and returns a Source processor
// which generates a data stream using the given
// spout.
func (g *OGraph) Source(s Spout, attribs ...T) *aGraph {
	proc := g.NewProcessor(nil, make([]chan T, 1), OP_SOURCE)
	g.Register(proc, proc.ParseAttrib(attribs))
	proc.F = func(inputs ...chan T) []chan T {
		g.group.Add(1)
		proc.Inputs = inputs
		go func() {
			defer g.group.Done()
			defer close(proc.Outputs[0])
			var x T
			for {
				t := time.Now()
				x = s.Read()
				if x == nil {
					break
				}
				proc.AddTimeInfo1(PROC_ENTER_TIME, t, x)
				proc.AddTimeInfo(PROC_LEAVE_TIME, x)
				proc.Outputs[0] <- proc.OutStack.ExecStack(x)
				if !proc.Wait() {
					break
				}
			}
		}()
		return proc.Outputs
	}
	return &aGraph{g, proc}
}

// Grounding sink
// It joins `group` and returns a sink processor
// which discards *all* of the inputs from its upstream.
// A sink processor is one that accepts an incoming stream, but
// has no output stream.
func (g *OGraph) Ground(attribs ...T) *aGraph {
	proc := g.NewProcessor(nil, []chan T{}, OP_GROUND)
	g.Register(proc, proc.ParseAttrib(attribs))
	proc.F = func(inputs ...chan T) []chan T {
		g.group.Add(1)
		proc.Inputs = inputs
		go func() {
			defer g.group.Done()
			ct := time.Now()
			for x := range proc.Inputs[0] {
				// need to implement a disposing function.
				comm, state := proc.WaitMessage(x, proc.Outputs...)
				if !state {
					break
				}
				if x != nil && !comm {
					x = proc.InStack.ExecStack(x)
					proc.AddTimeInfo(PROC_ENTER_TIME, x)
					DeepDispose(x)
					ut := time.Now()
					proc.AddTimeInfo1(PROC_LEAVE_TIME, ut, x)
					dt := ut.Sub(ct).Seconds()
					if dt >= proc.G.DecayInt && proc.G.Active {
						AccumulateStats(proc.G.GndBranches[proc.Name], proc.G.Alpha, dt, x)
						ct = ut
					} else {
						AccumulateStats(proc.G.GndBranches[proc.Name], 0, 0, x)
					}
				}
			}
		}()
		return proc.Outputs
	}
	return &aGraph{g, proc}
}

// Map processor:
// It joins `group` and uses a function `f`. The returned
// processor applies the function `f` to each input reading `x`
// from the upstream, and writes `f(x)` to the downstream.
func (g *OGraph) Map(funcs Functions, attribs ...T) *aGraph {
	proc := g.NewProcessor(nil, make([]chan T, 1), OP_MAP)
	proc.Funcs, proc.FuncIdx = funcs, 0
	g.Register(proc, proc.ParseAttrib(attribs))
	proc.F = func(inputs ...chan T) []chan T {
		g.group.Add(1)
		proc.Inputs = inputs
		go func() {
			defer g.group.Done()
			defer close(proc.Outputs[0])
			for {
				x, ok := <-proc.Inputs[0]
				if !ok {
					break
				}
				comm, state := proc.WaitMessage(x, proc.Outputs...)
				if !state {
					break
				}
				if !comm {
					x = proc.InStack.ExecStack(x)
					proc.AddTimeInfo(PROC_ENTER_TIME, x)
					proc.ProcessorInfo.UpdateSettings(x)
					y := proc.Funcs[proc.FuncIdx].Mapper(x, proc.Funcs[proc.FuncIdx].FuncParams)
					proc.AddTimeInfo(PROC_LEAVE_TIME, y)
					proc.Outputs[0] <- proc.OutStack.ExecStack(y)
				}
			}
		}()
		return proc.Outputs
	}
	return &aGraph{g, proc}
}

// Reduce processor:
// It maintains an internal state u which is initialized
// by `u0`. For each reading `x` from the incoming stream,
// reduce updates the state `u` using the `g` function and
// generates an output `y` for the outgoing stream.
func (g *OGraph) Reduce(u0 T, funcs Functions, attribs ...T) *aGraph {
	proc := g.NewProcessor(nil, make([]chan T, 1), OP_REDUCE)
	proc.Funcs, proc.FuncIdx = funcs, 0
	g.Register(proc, proc.ParseAttrib(attribs))
	proc.F = func(inputs ...chan T) []chan T {
		g.group.Add(1)
		u := u0
		proc.Funcs[proc.FuncIdx].State = u0
		proc.Inputs = inputs
		go func() {
			defer g.group.Done()
			defer close(proc.Outputs[0])
			defer DeepDispose(u) //(u.(Disposable)).Dispose()
			var y T
			for {
				x, ok := <-proc.Inputs[0]
				if !ok {
					break
				}
				comm, state := proc.WaitMessage(x, proc.Outputs...)
				if !state {
					break
				}
				if !comm {
					if x != nil {
						x = proc.InStack.ExecStack(x)
						proc.AddTimeInfo(PROC_ENTER_TIME, x)
						proc.ProcessorInfo.UpdateSettings(x)
						u, y = proc.Funcs[proc.FuncIdx].Reducer(u, x, proc.Funcs[proc.FuncIdx].FuncParams)
						proc.Funcs[proc.FuncIdx].State = u
						proc.AddTimeInfo(PROC_LEAVE_TIME, y)
						proc.Outputs[0] <- proc.OutStack.ExecStack(y)
					} else {
						proc.Outputs[0] <- x
					}
				}
			}
		}()
		return proc.Outputs
	}
	return &aGraph{g, proc}
}

// Filter processor:
// It forwards certain readings from the incoming
// stream that meets the predicate `p` to the first
// output channel `c1` and send the remaining readings
// to `c2`.
func (g *OGraph) Filter(funcs Functions, attribs ...T) *aGraph {
	proc := g.NewProcessor(nil, make([]chan T, 2), OP_FILTER)
	proc.Funcs, proc.FuncIdx = funcs, 0
	g.Register(proc, proc.ParseAttrib(attribs))
	proc.F = func(inputs ...chan T) []chan T {
		proc.Inputs = inputs
		g.group.Add(1)
		go func() {
			defer g.group.Done()
			defer close(proc.Outputs[0])
			defer close(proc.Outputs[1])
			for x := range proc.Inputs[0] {
				comm, state := proc.WaitMessage(x, proc.Outputs...)
				if !state {
					break
				}
				if !comm {
					x = proc.InStack.ExecStack(x)
					proc.AddTimeInfo(PROC_ENTER_TIME, x)
					proc.ProcessorInfo.UpdateSettings(x)
					dec := proc.Funcs[proc.FuncIdx].Mapper(x, proc.Funcs[proc.FuncIdx].FuncParams).(bool)
					proc.AddTimeInfo(PROC_LEAVE_TIME, x)
					if dec {
						proc.Outputs[0] <- x
					} else {
						proc.Outputs[1] <- x
					}
				}
			}
		}()
		return proc.Outputs
	}
	return &aGraph{g, proc}
}

// //##############################################################
// // 2. Flow Control
// //##############################################################

// Copy processor:
// It makes duplicates o the incoming stream. It is
// important to observe that Copy writes its output
// synchronously on all duplicated outgoing streams.
func (g *OGraph) Copy(n int, attribs ...T) *aGraph {
	proc := g.NewProcessor(nil, make([]chan T, n), OP_COPYN)
	g.Register(proc, proc.ParseAttrib(attribs))
	proc.F = func(inputs ...chan T) []chan T {
		proc.Inputs = inputs
		g.group.Add(1)
		go func() {
			defer g.group.Done()
			defer func() {
				for i := 0; i < n; i++ {
					close(proc.Outputs[i])
				}
			}()
			for x := range proc.Inputs[0] {
				comm, state := proc.WaitMessage(x, proc.Outputs...)
				if !state {
					break
				}
				if !comm {
					x = proc.InStack.ExecStack(x)
					proc.AddTimeInfo(PROC_ENTER_TIME, x)
					if x != nil {
						for i := 1; i < n; i++ {
							y := DeepClone(x)
							proc.AddTimeInfo(PROC_LEAVE_TIME, y)
							proc.Outputs[i] <- x
						}
						proc.AddTimeInfo(PROC_LEAVE_TIME, x)
						proc.Outputs[0] <- x
					} else {
						for i := 0; i < n; i++ {
							proc.Outputs[i] <- x
						}
					}
				}

			}
		}()
		return proc.Outputs
	}
	return &aGraph{g, proc}
}

func (g *OGraph) Split(n int, attribs ...T) *aGraph {
	proc := g.NewProcessor(nil, make([]chan T, n), OP_SPLIT)
	g.Register(proc, proc.ParseAttrib(attribs))
	proc.F = func(inputs ...chan T) []chan T {
		proc.Inputs = inputs
		closeall := func() {
			for _, c := range proc.Outputs {
				close(c)
			}
		}
		g.group.Add(1)
		go func() {
			defer g.group.Done()
			defer closeall()
			k := 0
			for x := range proc.Inputs[0] {
				comm, state := proc.WaitMessage(x, proc.Outputs...)
				if !state {
					break
				}
				if !comm {
					proc.AddTimeInfo(PROC_BOTH_TIME, x)
					proc.Outputs[k] <- x
					k = (k + 1) % len(proc.Outputs)
				}
			}
		}()
		return proc.Outputs
	}
	return &aGraph{g, proc}
}

// Latch processor:
// It allows the incoming and outgoing channels to be
// asynchronous (namely transmitting at different rates).
// it returns two channels, the original input channel `c1` and
// the output channel `c2`.
func (g *OGraph) Latch(attribs ...T) *aGraph {
	proc := g.NewProcessor(nil, make([]chan T, 2), OP_LATCH)
	g.Register(proc, proc.ParseAttrib(attribs))
	proc.F = func(inputs ...chan T) []chan T {
		var (
			u       T
			proceed bool = true
			clone   bool = true
		)
		proc.Inputs = inputs
		g.group.Add(1)
		go func() {
			defer g.group.Done()
			defer close(proc.Outputs[1])

			for x := range inputs[0] {
				comm, state := proc.WaitMessage(x, proc.Outputs...)
				if !state {
					break
				}
				if !comm {
					x = proc.InStack.ExecStack(x)
					proc.AddTimeInfo(PROC_ENTER_TIME, x)
					if x != nil && clone {
						if u != nil {
							DeepDispose(u)
						}
						u = DeepClone(x)
					}
					proc.AddTimeInfo(PROC_LEAVE_TIME, x)
					proc.Outputs[1] <- x
				}
			}
			proceed = false
		}()
		g.group.Add(1)
		go func() {
			defer g.group.Done()
			defer close(proc.Outputs[0])
			var y T
			for proceed {
				y = u
				clone = false
				proc.AddTimeInfo(PROC_ENTER_TIME, y)
				if y != nil {
					y = DeepClone(u) //(u.(Cloneable)).Clone()
				}
				clone = true
				proc.AddTimeInfo(PROC_LEAVE_TIME, y)
				proc.Outputs[0] <- y
			}
		}()
		return proc.Outputs
	}
	return &aGraph{g, proc}
}

// Cut processor:
// It allows the incoming and outgoing channles to be
// asynchronous (namely transmitting at different rates).
// it returns two channel, the original input channel `c1` and
// the output channel `c2`. The operator guarantees that every
// incoming reading is written once to the outgoing channel.
// A nil value is used for the extra write operations.
func (g *OGraph) Cut(attribs ...T) *aGraph {
	proc := g.NewProcessor(nil, make([]chan T, 2), OP_CUT)
	g.Register(proc, proc.ParseAttrib(attribs))
	proc.F = func(inputs ...chan T) []chan T {
		g.group.Add(1)
		var (
			u       T
			proceed bool = true
			clone   bool = true
		)
		proc.Inputs = inputs
		go func() {
			defer g.group.Done()
			defer close(proc.Outputs[1])
			for x := range proc.Inputs[0] {
				comm, state := proc.WaitMessage(x, proc.Outputs...)
				if !state {
					break
				}
				if !comm {
					x = proc.InStack.ExecStack(x)
					proc.AddTimeInfo(PROC_ENTER_TIME, x)
					if x != nil && u == nil && clone {
						u = DeepClone(x)
						//u = (x.(Cloneable)).Clone()
					}
					proc.AddTimeInfo(PROC_LEAVE_TIME, x)
					proc.Outputs[1] <- x
				}
			}
			proceed = false
		}()
		g.group.Add(1)
		go func() {
			defer g.group.Done()
			defer close(proc.Outputs[0])
			for proceed {
				y := u
				proc.AddTimeInfo(PROC_ENTER_TIME, y)
				clone = false
				if y != nil {
					y = u
					u = nil
				}
				clone = true
				proc.AddTimeInfo(PROC_LEAVE_TIME, y)
				proc.Outputs[0] <- y
			}
		}()
		return proc.Outputs
	}
	return &aGraph{g, proc}
}

// LeftMultiply processor:
// It reads from two incoming channels inputs[0] and
// inputs[1] and outputs pairs (x1; x2) to the outgoing
// channel `c1`. Unlike Map, multiply synchronizes the
// writes with inputs[0] and latches with inputs[2]. It
// also forwards inputs[2] to `c2`.
func (g *OGraph) LeftMultiply(attribs ...T) *aGraph {
	proc := g.NewProcessor(nil, make([]chan T, 2), OP_LEFT_MULTIPLY)
	var f func([]T) T = nil
	if len(attribs) > 0 {
		switch t := attribs[0].(type) {
		case func([]T) T:
			f = t
			attribs = attribs[1:]
		}
	}
	g.Register(proc, proc.ParseAttrib(attribs))
	proc.F = func(inputs ...chan T) []chan T {
		var clatch chan T
		// outs := Latch1(group).F(inputs[1])
		proc.Inputs = inputs
		latch1 := g.Latch(OP_ATTRIB_GRAPH_REMOVED, true).Proc
		latch1.Outputs[0] = make(chan T)
		latch1.Outputs[1] = proc.Outputs[1]
		outs := latch1.F(inputs[1])
		clatch, proc.Outputs[1] = outs[0], outs[1]
		g.group.Add(1)
		go func() {
			defer g.group.Done()
			defer close(proc.Outputs[0])
			for x := range inputs[0] {
				proc.AddTimeInfo(PROC_ENTER_TIME, x)
				if y, ok := <-clatch; ok {
					proc.AddTimeInfo(PROC_ENTER_TIME, y)
					yy := []T{x, y}
					proc.AddTimeInfo(PROC_LEAVE_TIME, x)
					proc.AddTimeInfo(PROC_LEAVE_TIME, y)
					if f == nil {
						proc.Outputs[0] <- yy
					} else {
						proc.Outputs[0] <- f(yy)
					}

				}
			}
		}()
		return proc.Outputs
	}
	return &aGraph{g, proc}
}

// Multiply processor:
// It reads from multiple incoming channels and writes to one
// outgoing channel. The operator reads one value at
// a time from each incoming stream, forms a vector
// (x_1,...,x_k), and synchronously writes this vector
// to the outgoing stream.
func (g *OGraph) Multiply(attribs ...T) *aGraph {
	proc := g.NewProcessor(nil, make([]chan T, 1), OP_MULTIPLY)
	// check first attrib
	var f func([]T) T = nil
	if len(attribs) > 0 {
		switch t := attribs[0].(type) {
		case func([]T) T:
			f = t
			attribs = attribs[1:]
		}
	}
	g.Register(proc, proc.ParseAttrib(attribs))
	proc.F = func(inputs ...chan T) []chan T {
		k := 0
		ok := false
		proc.Inputs = inputs
		g.group.Add(1)
		go func() {
			defer g.group.Done()
			defer close(proc.Outputs[0])
			for {
				k = 0
				y := make([]T, len(proc.Inputs))
				for _, x := range proc.Inputs {
					y[k], ok = <-x
					if ok {
						proc.AddTimeInfo(PROC_ENTER_TIME, y[k])
						k++
					}
				}
				if k == 0 {
					break
				}
				proc.AddTimeInfo1(PROC_LEAVE_TIME, time.Now(), y...)
				if f == nil {
					proc.Outputs[0] <- proc.OutStack.ExecStack(y[0:k])
				} else {
					proc.Outputs[0] <- proc.OutStack.ExecStack(f(y[0:k]))
				}
			}
		}()
		return proc.Outputs
	}
	return &aGraph{g, proc}
}

// Add processor:
// It merges multiple incoming channels in a greedy fashion.
// It performs best effort reads on the incoming collection
// of channels asynchronously, and writes to one outgoing
// channel.
func (g *OGraph) Add(attribs ...T) *aGraph {
	proc := g.NewProcessor(nil, make([]chan T, 1), OP_ADD)
	g.Register(proc, proc.ParseAttrib(attribs))
	proc.F = func(inputs ...chan T) []chan T {
		k := len(inputs)
		proc.Inputs = inputs
		for i, cin := range inputs {
			g.group.Add(1)
			go func(i int, cin chan T) {
				defer g.group.Done()
				for x := range cin {
					proc.AddTimeInfo(PROC_ENTER_TIME, x)
					proc.AddTimeInfo(PROC_LEAVE_TIME, x)
					proc.Outputs[0] <- proc.OutStack.ExecStack(x)
				}
				k--
				if k == 0 {
					close(proc.Outputs[0])
				}
			}(i, cin)
		}
		return proc.Outputs
	}
	return &aGraph{g, proc}
}

//##############################################################
//##############################################################
// Higher order operators
//##############################################################

// Scatter processor:
// It reads from an incoming channel, but generates a list of
// outgoing channels. The list of outgoing channels can be
// arbitrary size controlled by the fan out parameter `fout`.
// It is parameterized by the generator function `f` and a
// partition function `p`. `f` computes for each incoming value,
// a vector of emitted values to the output channels. `p` maps
// each emitted value to one output channel, and it has the
// signature `p(emitted_element, vector_index, fout)`.
func (g *OGraph) Scatter(n int, f func(T) []T, p func(T, int, int) int, attribs ...T) *aGraph {
	proc := g.NewProcessor(nil, make([]chan T, n), OP_SCATTER)
	g.Register(proc, proc.ParseAttrib(attribs))
	proc.F = func(inputs ...chan T) []chan T {
		proc.Inputs = inputs
		closeall := func() {
			for _, c := range proc.Outputs {
				close(c)
			}
		}
		g.group.Add(1)
		go func() {
			defer g.group.Done()
			defer closeall()
			for x := range proc.Inputs[0] {
				comm, state := proc.WaitMessage(x, proc.Outputs...)
				if !state {
					break
				}
				if !comm {
					x = proc.InStack.ExecStack(x)
					proc.AddTimeInfo(PROC_ENTER_TIME, x)
					v := f(x)
					if v != nil {
						proc.AddTimeInfo1(PROC_LEAVE_TIME, time.Now(), v...)
						for i, y := range v {
							idx := p(y, i, n)
							if idx >= 0 {
								proc.Outputs[idx] <- y
							}
						}
					}
				}
			}
		}()
		return proc.Outputs
	}
	return &aGraph{g, proc}
}

// Merge processor:
// It merges a collection of incoming channels back into a
// single outgoing channel. A function `f` continuously receives
// a buffer of the same size as the number of input channels.
// Every element in this buffer contains either an input element
// or nil. `f` must perform a merge or selection operation on the
// buffer and returns the resulted element. 'f' also returns the
// element index in case of a selection operation or -1 in case
// of a merge operation. Note that input channels should be from
// decoupled sources. In case of Scatter, only the merge
// operation is supported.
func (g *OGraph) Merge(p func([]T) (int, T), attribs ...T) *aGraph {
	proc := g.NewProcessor(nil, make([]chan T, 1), OP_MERGE)
	g.Register(proc, proc.ParseAttrib(attribs))
	proc.F = func(inputs ...chan T) []chan T {
		k := len(inputs)
		proc.Inputs = inputs
		buf, ok := make([]T, k), make([]bool, k)
		for i := 0; i < k; i++ {
			ok[i] = true
		}
		g.group.Add(1)
		go func() {
			defer close(proc.Outputs[0])
			defer g.group.Done()
			for k > 0 {
				for i := 0; i < len(proc.Inputs); i++ {
					if buf[i] == nil && ok[i] {
						buf[i], ok[i] = <-proc.Inputs[i]
						if !ok[i] {
							k--
						} else {
							proc.AddTimeInfo(PROC_ENTER_TIME, buf[i])
						}
					}
				}
				i, y := p(buf)
				if y != nil {
					proc.AddTimeInfo(PROC_LEAVE_TIME, y)
				}
				proc.Outputs[0] <- y
				if i < 0 {
					buf = make([]T, len(inputs))
				} else {
					buf[i] = nil
				}
			}
		}()
		return proc.Outputs
	}
	return &aGraph{g, proc}
}

// ListMap processor:
// It has input and output as collections of channels. It is
// parameterized by a Map processor and applies the Map on each
// incoming channel to generate a corresponding output channel.
func (g *OGraph) List(n int, f func(*OGraph, int) (*Processor, *Processor), attribs ...T) *aGraph {
	proc := g.NewProcessor(nil, nil, OP_COMPOSITE)
	proc.IsComposite = true
	proc.Composite = &Composite{make([]*Processor, n), make([]*Processor, n)}
	for i := 0; i < n; i++ {
		proc.Composite.InProcs[i], proc.Composite.OutProcs[i] = f(g, i)
	}
	g.Register(proc, proc.ParseAttrib(attribs))
	return &aGraph{g, proc}
}

func (g *OGraph) Group(n_inputs, n_outputs int, f func(T) []T, p func(T, int, int) int, attribs ...T) *aGraph {

	// search for prev processor in attribs
	var pproc *Processor = nil
	for i := 0; i < len(attribs); i += 2 {
		if attribs[i].(int) == OP_ATTRIB_PREV_PROC {
			pproc = attribs[i+1].(*Processor)
		}
	}
	h1 := func(g *OGraph, i int) (*Processor, *Processor) {
		p := g.Scatter(n_outputs, f, p).Proc
		return p, p
	}
	h2 := func(g *OGraph, i int) (*Processor, *Processor) {
		p := g.Add().Proc
		return p, p
	}
	scattComp := g.List(n_inputs, h1).Proc
	addComp := g.List(n_outputs, h2).Proc

	if pproc != nil {
		g.LinkOut(pproc.Name, scattComp.Name)
	}
	ops := make([]string, n_outputs)
	for i := 0; i < n_outputs; i++ {
		ops[i] = addComp.Composite.InProcs[i].Name
	}
	for i := 0; i < n_inputs; i++ {
		g.LinkOut(scattComp.Composite.OutProcs[i].Name, ops...)
	}

	return &aGraph{g, addComp}
}
