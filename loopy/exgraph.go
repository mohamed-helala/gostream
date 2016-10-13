package loopy

import (
	"fmt"
	"gem"
	"math"
	"runtime"
	"sync"
	"time"

	"github.com/twmb/algoimpl/go/graph"
)

//#################################################################
//                   Operators Graph
//#################################################################

type ChanInfo struct {
	In_idxs  []int //indecies of input channels
	Out_idxs []int //indecies of output channels
}
type EdgeInfo struct {
	Chans     map[string]*ChanInfo // channels used for a graph edge
	NInchans  int                  // number of input channels
	NOutchans int                  // number of output channels
	br        *Branch              // branch for this edge
}

type OGraph struct {
	*graph.Graph
	Nodes_map    map[string]graph.Node // a map for storing graph nodes
	split_nodes  map[string]graph.Node // a map for storing split nodes
	gnd_nodes    map[string]graph.Node // a map for storing ground nodes
	Edges_info   map[string]*EdgeInfo  // a map for storing input and output edges (channels)
	inChan_mask  map[string][]string
	outChan_mask map[string][]string
	ProcIdToName map[uint64]string
	Branches     []*Branch            // a list of all branches in the graph
	GndBranches  map[string][]*Branch // a list of all branches that ends with a given ground
	Alpha        float64
	DecayInt     float64    // Decay interval
	SchInt       float64    // Scheduling interval
	Active       bool       // Not used now
	NumCpu       int        // number of cpus for scheduling
	monProc      *Processor // Monitor processor
	TL, TP       float64    // Thresholds for Period and Latency
	group        *sync.WaitGroup
	seq          *Sequence
	// updates    map[string]*ProcInfoList
	// views      []ParamsViewer
}

type aGraph struct {
	*OGraph
	Proc *Processor
}

func NewOGraph() *OGraph {
	return &OGraph{Graph: graph.New(graph.Directed),
		Nodes_map:    make(map[string]graph.Node),
		split_nodes:  make(map[string]graph.Node),
		gnd_nodes:    make(map[string]graph.Node),
		Edges_info:   make(map[string]*EdgeInfo),
		inChan_mask:  make(map[string][]string),
		outChan_mask: make(map[string][]string),
		Branches:     make([]*Branch, 0, 10),
		GndBranches:  make(map[string][]*Branch),
		ProcIdToName: make(map[uint64]string),
		Alpha:        0.2, DecayInt: 5000, SchInt: 10000,
		Active: true, NumCpu: runtime.NumCPU(), monProc: nil,
		TL: 100, TP: 60, group: &sync.WaitGroup{},
		seq: NewSequence(0)}
}

func (g *OGraph) NewProcessor(inchans []chan T, outchans []chan T, _type int) *Processor {
	return NewProcessor(g, inchans, outchans, _type)
}

func (g *OGraph) AddProc(proc *Processor) {
	n := g.MakeNode()
	g.Nodes_map[proc.Name] = n
	g.Edges_info[proc.Name] = &EdgeInfo{map[string]*ChanInfo{}, 0, 0, nil}
	g.outChan_mask[proc.Name] = make([]string, 0)
	g.inChan_mask[proc.Name] = make([]string, 0)
	proc.G = g
	*g.Nodes_map[proc.Name].Value = proc
	if proc._type != OP_MAP && proc._type != OP_REDUCE && proc._type != OP_GROUND {
		g.split_nodes[proc.Name] = n
	}
	if proc._type == OP_GROUND {
		g.gnd_nodes[proc.Name] = n
	}
}

func (g *OGraph) Get(name string) *Processor {
	if n, ok := g.Nodes_map[name]; ok {
		return (*n.Value).(*Processor)
	} else {
		panic(fmt.Sprintf("Couldn't find name %s in Get method", name))
	}
}

// func (g *OGraph) GetUniqueName(name string) (string, bool) {
// 	var un_name string
// 	var ok bool
// 	if un_name, ok = g.user_names[name]; !ok{
// 		un_name =  name
// 	}
// 	// check if unique name exist
// 	if _, ok = g.Nodes_map[un_name]; ok{
// 		return un_name, true
// 	}
// 	return un_name, false
// }

// func (g *OGraph) GetUserName(name string) (string, bool) {
// 	for k, v := range g.user_names{
// 		if v == name{
// 			return k, true
// 		}
// 	}
// 	return name, false
// }

func (g *OGraph) Dispose() {}

func (g *OGraph) Connect(name1 string, name2 string, out_idxs, in_idxs []int) {
	g.MakeEdge(g.Nodes_map[name1], g.Nodes_map[name2])
	for i := 0; i < len(out_idxs); i++ {
		if out_idxs[i] < len(g.outChan_mask[name1]) {
			panic(fmt.Sprintf("Outpur channel %d of operator %s is already occupied by %s, failed to reset it to %s",
				out_idxs[i], name1, g.outChan_mask[name1][out_idxs[i]], name2))
		} else {
			g.outChan_mask[name1] = append(g.outChan_mask[name1], name2)
		}
	}
	for i := 0; i < len(in_idxs); i++ {
		if in_idxs[i] < len(g.inChan_mask[name2]) {
			panic(fmt.Sprintf("Input channel %d of operator %s is already occupied by %s, failed to reset it to %s",
				in_idxs[i], name2, g.inChan_mask[name2][in_idxs[i]], name1))
		} else {
			g.inChan_mask[name2] = append(g.inChan_mask[name2], name1)
		}
	}
	if cinfo, ok := g.Edges_info[name2].Chans[name1]; ok {
		cinfo.In_idxs = append(cinfo.In_idxs, in_idxs...)
		cinfo.Out_idxs = append(cinfo.Out_idxs, out_idxs...)
	} else {
		g.Edges_info[name2].Chans[name1] = &ChanInfo{in_idxs, out_idxs}
	}

	g.Edges_info[name2].NInchans = g.Edges_info[name2].NInchans + len(in_idxs)
	g.Edges_info[name1].NOutchans = g.Edges_info[name1].NOutchans + len(out_idxs)
}

func (g *OGraph) _linkOut(fork string, ops ...string) {
	// for free output channgels from the fork operator
	k := len(ops)
	if _, ok := g.Nodes_map[fork]; !ok {
		panic(fmt.Sprintf("Couldn't find fork operator %s in LinkOut", fork))
	}
	for i := 0; i < k; i++ {
		if _, ok := g.Nodes_map[ops[i]]; !ok {
			panic(fmt.Sprintf("Couldn't find listed operator %s in LinkOut", ops[i]))
		}
	}

	f_offset := len(g.outChan_mask[fork])
	ops_offsets := make([]int, k)
	for i := 0; i < k; i++ {
		ops_offsets[i] = len(g.inChan_mask[ops[i]])
	}

	// perform the linking operation
	for i := 0; i < k; i++ {
		g.Connect(fork, ops[i], []int{f_offset + i}, []int{ops_offsets[i]})
	}
}

func (g *OGraph) _linkIn(join string, ops ...string) {
	// for free output channgels from the fork operator
	k := len(ops)
	if _, ok := g.Nodes_map[join]; !ok {
		panic(fmt.Sprintf("Couldn't find join operator %s in LinkIn", join))
	}
	for i := 0; i < k; i++ {
		if _, ok := g.Nodes_map[ops[i]]; !ok {
			panic(fmt.Sprintf("Couldn't find listed operator %s in LinkIn", ops[i]))
		}
	}
	j_offset := len(g.inChan_mask[join])
	ops_offsets := make([]int, k)
	for i := 0; i < k; i++ {
		ops_offsets[i] = len(g.outChan_mask[ops[i]])
	}

	// perform the linking operation
	for i := 0; i < k; i++ {
		g.Connect(ops[i], join, []int{ops_offsets[i]}, []int{j_offset + i})
	}
}

func (g *OGraph) LinkOut(fork string, ops ...string) {
	forkProc, opProc := g.Get(fork), g.Get(ops[0])
	if forkProc.IsComposite && opProc.IsComposite {
		for i := 0; i < len(forkProc.Composite.OutProcs); i++ {
			g._linkOut(forkProc.Composite.OutProcs[i].Name, opProc.Composite.InProcs[i].Name)
		}
	} else if opProc.IsComposite {
		for i := 0; i < len(opProc.Composite.InProcs); i++ {
			g._linkOut(fork, opProc.Composite.InProcs[i].Name)
		}
	} else if forkProc.IsComposite {
		for i := 0; i < len(forkProc.Composite.OutProcs); i++ {
			g._linkOut(forkProc.Composite.OutProcs[i].Name, opProc.Name)
		}
	} else {
		g._linkOut(fork, ops...)
	}
}

func (g *OGraph) LinkIn(join string, ops ...string) {
	joinProc, opProc := g.Get(join), g.Get(ops[0])
	if joinProc.IsComposite && opProc.IsComposite {
		for i := 0; i < len(joinProc.Composite.InProcs); i++ {
			g._linkIn(joinProc.Composite.InProcs[i].Name, opProc.Composite.OutProcs[i].Name)
		}
	} else if opProc.IsComposite {
		for i := 0; i < len(opProc.Composite.OutProcs); i++ {
			g._linkIn(join, opProc.Composite.OutProcs[i].Name)
		}
	} else if joinProc.IsComposite {
		for i := 0; i < len(joinProc.Composite.InProcs); i++ {
			g._linkIn(joinProc.Composite.InProcs[i].Name, opProc.Name)
		}
	} else {
		g._linkIn(join, ops...)
	}
}

func (g *OGraph) Register(proc *Processor, pproc *Processor) {
	if proc.IsGraphRemoved {
		return
	}
	g.AddProc(proc)
	g.ProcIdToName[proc.Id] = proc.Name
	if pproc != nil {
		if pproc.IsComposite || proc.IsComposite {
			g.LinkOut(pproc.Name, proc.Name)
		} else {
			idxs := make([]int, len(pproc.Outputs))
			for i, _ := range idxs {
				idxs[i] = i
			}
			g.Connect(pproc.Name, proc.Name, idxs, idxs)
		}
	}
}

func (g *OGraph) Wait() {
	g.group.Wait()
}

func (g *aGraph) Source(s Spout, attribs ...T) *aGraph {
	attribs = append(attribs, OP_ATTRIB_PREV_PROC, g.Proc)
	return g.OGraph.Source(s, attribs...)
}

func (g *aGraph) Ground(attribs ...T) *aGraph {
	attribs = append(attribs, OP_ATTRIB_PREV_PROC, g.Proc)
	return g.OGraph.Ground(attribs...)
}

func (g *aGraph) Map(funcs Functions, attribs ...T) *aGraph {
	attribs = append(attribs, OP_ATTRIB_PREV_PROC, g.Proc)
	return g.OGraph.Map(funcs, attribs...)
}

func (g *aGraph) Reduce(u0 T, funcs Functions, attribs ...T) *aGraph {
	attribs = append(attribs, OP_ATTRIB_PREV_PROC, g.Proc)
	return g.OGraph.Reduce(u0, funcs, attribs...)
}

func (g *aGraph) Copy(n int, attribs ...T) *aGraph {
	attribs = append(attribs, OP_ATTRIB_PREV_PROC, g.Proc)
	return g.OGraph.Copy(n, attribs...)
}

func (g *aGraph) Split(n int, attribs ...T) *aGraph {
	attribs = append(attribs, OP_ATTRIB_PREV_PROC, g.Proc)
	return g.OGraph.Split(n, attribs...)
}

func (g *aGraph) Filter(funcs Functions, attribs ...T) *aGraph {
	attribs = append(attribs, OP_ATTRIB_PREV_PROC, g.Proc)
	return g.OGraph.Filter(funcs, attribs...)
}

func (g *aGraph) Latch(attribs ...T) *aGraph {
	attribs = append(attribs, OP_ATTRIB_PREV_PROC, g.Proc)
	return g.OGraph.Latch(attribs...)
}

func (g *aGraph) Cut(attribs ...T) *aGraph {
	attribs = append(attribs, OP_ATTRIB_PREV_PROC, g.Proc)
	return g.OGraph.Cut(attribs...)
}

func (g *aGraph) LeftMultiply(attribs ...T) *aGraph {
	attribs = append(attribs, OP_ATTRIB_PREV_PROC, g.Proc)
	return g.OGraph.LeftMultiply(attribs...)
}

func (g *aGraph) Multiply(attribs ...T) *aGraph {
	attribs = append(attribs, OP_ATTRIB_PREV_PROC, g.Proc)
	return g.OGraph.Multiply(attribs...)
}

func (g *aGraph) Scatter(n int, f func(T) []T, p func(T, int, int) int, attribs ...T) *aGraph {
	attribs = append(attribs, OP_ATTRIB_PREV_PROC, g.Proc)
	return g.OGraph.Scatter(n, f, p, attribs...)
}

func (g *aGraph) Merge(p func([]T) (int, T), attribs ...T) *aGraph {
	attribs = append(attribs, OP_ATTRIB_PREV_PROC, g.Proc)
	return g.OGraph.Merge(p, attribs...)
}

func (g *aGraph) Add(attribs ...T) *aGraph {
	attribs = append(attribs, OP_ATTRIB_PREV_PROC, g.Proc)
	return g.OGraph.Add(attribs...)
}

func (g *aGraph) List(n int, f func(*OGraph, int) (*Processor, *Processor), attribs ...T) *aGraph {
	attribs = append(attribs, OP_ATTRIB_PREV_PROC, g.Proc)
	return g.OGraph.List(n, f, attribs...)
}

func (g *aGraph) Group(n_inputs, n_outputs int, f func(T) []T, p func(T, int, int) int, attribs ...T) *aGraph {
	attribs = append(attribs, OP_ATTRIB_PREV_PROC, g.Proc)
	return g.OGraph.Group(n_inputs, n_outputs, f, p, attribs...)
}

func (g *OGraph) Execute() {
	//g.scan()
	for name2, e_info := range g.Edges_info {
		chans := make([]chan T, e_info.NInchans)
		in_proc := (*g.Nodes_map[name2].Value).(*Processor)
		if in_proc.IsComposite {
			continue
		}
		// now connect chans
		for name1, chan_info := range e_info.Chans {
			out_proc := (*g.Nodes_map[name1].Value).(*Processor)
			for i, idx := range chan_info.In_idxs {
				if out_proc.Outputs[chan_info.Out_idxs[i]] == nil {
					out_proc.Outputs[chan_info.Out_idxs[i]] = make(chan T)
				}
				chans[idx] = out_proc.Outputs[chan_info.Out_idxs[i]]
			}
		}
		in_proc.F(chans...)
	}
	//g.monitor(g.group)
}

func (g *OGraph) scan() {
	for br_k, _ := range g.split_nodes {
		nodes := g.Neighbors(g.Nodes_map[br_k])
		for _, n := range nodes {
			c_k := (*n.Value).(*Processor).Name
			if g.Edges_info[c_k].NInchans > 1 {
				continue
			}
			b := &Branch{Br_start: br_k, G: g}
			if g.Edges_info[br_k].NOutchans == 1 {
				b.Start = br_k
				if g.Edges_info[br_k].NInchans <= 1 {
					g.Edges_info[br_k].br = b
				}
			} else {
				b.Start = c_k
				g.Edges_info[c_k].br = b
			}
			b.Nodes = []string{b.Start}
			b.End = c_k
			g.traverseBranch(c_k, b)
			g.Branches = append(g.Branches, b)
		}
	}
	// assign grounds
	for k, _ := range g.gnd_nodes {
		g.assignGrnds(k, k)
	}
	for _, b := range g.Branches {
		// create stats
		b.Stats = make([]*SStats, len(b.Nodes))
		for i := 0; i < len(b.Nodes); i++ {
			b.Stats[i] = &SStats{gem.Point{0, 0}, gem.Point{0, 0}, 0}
		}
		if brs, ok := g.GndBranches[b.Gnd]; ok {
			g.GndBranches[b.Gnd] = append(brs, b)
			continue
		}
		brs := make([]*Branch, 0, 3)
		brs = append(brs, b)
		g.GndBranches[b.Gnd] = brs
	}
}

func (g *OGraph) traverseBranch(c string, br *Branch) {
	nodes := g.Neighbors(g.Nodes_map[c])
	for _, n := range nodes {
		n_k := (*n.Value).(*Processor).Name
		if g.Edges_info[n_k].NOutchans > 1 {
			br.Br_end = n_k
			if g.Edges_info[n_k].NInchans <= 1 {
				br.End = n_k
			} else {
				br.End = c
			}
			if br.End != br.Start && br.End != br.Nodes[len(br.Nodes)-1] {
				br.Nodes = append(br.Nodes, br.End)
			}
			return
		} else if g.Edges_info[n_k].NOutchans == 0 {
			// we identified ground
			g.Edges_info[n_k].br = br
			br.End = n_k
			br.Gnd = n_k
			if br.End != br.Start {
				br.Nodes = append(br.Nodes, br.End)
			}
			return
		}
		g.Edges_info[n_k].br = br
		br.Nodes = append(br.Nodes, n_k)
		g.traverseBranch(n_k, br)
	}
}

func (g *OGraph) assignGrnds(c, gnd string) {
	c_info := g.Edges_info[c]
	c_proc := (*g.Nodes_map[c].Value).(*Processor)
	if c_info.NInchans <= 1 && c_info.NOutchans <= 1 {
		c_info.br.Gnd = gnd
	}
	if c_proc._type == OP_SOURCE {
		return
	}
	for k_n, k_ch := range c_info.Chans {
		proc := (*g.Nodes_map[k_n].Value).(*Processor)
		next := proc.Name
		if proc._type == OP_LATCH || proc._type == OP_CUT {
			for _, i := range k_ch.Out_idxs {
				if i == 0 {
					return
				}
			}
		}
		if proc._type == OP_LEFT_MULTIPLY {
			// find the chan index that matches my index
			for u_n, u_ch := range g.Edges_info[k_n].Chans {
				if k_ch.Out_idxs[0] == u_ch.In_idxs[0] {
					next = u_n
					break
				}
			}
		}
		g.assignGrnds(next, gnd)
	}
}

func (g *OGraph) monitor(group *sync.WaitGroup) {
	// implement the monitor task
	d, err := time.ParseDuration(fmt.Sprintf("%fms", g.SchInt))
	if err != nil {
		panic("Cannot parse schedule interval")
	}
	g.monProc = g.NewProcessor(nil, nil, OP_MISC)
	g.monProc.Name = "Monitor"
	g.monProc.F = func(inputs ...chan T) []chan T {
		group.Add(1)
		go func() {
			defer group.Done()
			ticks := time.Tick(d)
			for _ = range ticks {
				g.scheduleBranch()
			}
		}()
		return g.monProc.Outputs
	}
	g.monProc.F()
}

func (g *OGraph) scheduleBranch() {
	var (
		L, P float64
		S    []int
		// B    float64
		feas bool
	)

	for _, b := range g.Branches {
		L, P = 0, 0
		S, feas = nil, false
		T := make([]float64, len(b.Stats))
		for i, s := range b.Stats {
			m := s.Mean()
			L += m[0] + m[1]
			P = math.Max(P, m[0])
			T[i] = m[0]
		}
		b.L, b.P = L, P
		// solve according to the given values
		if (P < g.TP && L < g.TL) || b.Groups != nil {
			continue
		}
		W := prefixSum(T)
		for K := g.NumCpu; K > 0; K-- {
			Bopt := calcBottleNeck(W, K)
			if Bopt > g.TP {
				// This K is not feasible solution
				break
			}
			Sopt, ok := probe(W, Bopt, K)
			if !ok {
				// This K is not feasible solution
				break
			}
			S = Sopt
			// B = Bopt
			feas = true
		}
		if feas {
			// make a new schedule
			nth := 0
			for i, s := range S {
				if i > 0 && s == 0 {
					break
				}
				nth++
			}
			// buid groups
			b.Wait()
			b.Groups = make([]*NodesGroup, nth)
			for i := 0; i < nth; i++ {
				s, e := S[i], 0
				if i < nth-1 {
					e = S[i+1] - 1
				} else {
					e = len(T) - 1
				}

				if e == s {
					// just resume
					pm := g.Get(b.Nodes[s])
					pm.Resume(ST_RUN)
					continue
				}
				ng := &NodesGroup{Start: b.Nodes[s],
					End:   b.Nodes[e],
					Nodes: make([]string, e-s+1),
					Head:  b.Nodes[e]}

				for j := s; j <= e; j++ {
					ng.Nodes[j-s] = b.Nodes[j]
				}
				g.mergeForward(b, s, e)
				b.Groups[i] = ng
			}
		}
	}
}

func (g *OGraph) scheduleGraph() {

}

func (g *OGraph) mergeForward(b *Branch, s int, e int) {
	// create a new group
	eproc := g.Get(b.Nodes[e])
	sproc := g.Get(b.Nodes[s])
	eproc.Inputs = sproc.Inputs
	for i := e - 1; i >= s; i-- {
		pm := g.Get(b.Nodes[i])
		eproc.InStack.Push(pm.ProcessorInfo)
		pm.Resume(ST_EXIT)
	}
	eproc.Resume(ST_RUN)
}

func (g *OGraph) mergeBackward(b *Branch, ng *NodesGroup, e int, s int) {

}

//#################################################################
//                   Branch
//#################################################################

type NodesGroup struct {
	Start, End, Head string
	Nodes            []string
	P, L             float64
}

type Branch struct {
	Start, End       string
	Br_start, Br_end string
	Nodes            []string
	Gnd              string
	Stats            []*SStats
	Groups           []*NodesGroup
	P, L             float64
	G                *OGraph
}

func (b *Branch) Wait() {
	// Send Wait signals
	d, _ := time.ParseDuration("0.0001ms")
	ticks := time.Tick(d)
	proc := b.G.Get(b.Start)
	proc.Inputs[0] <- &cM{start: b.Start, end: b.End,
		ERStatus: ST_RUN,
		WRStatus: ST_REQWAIT}
	// wait until all nodes enter the wait state
	for _, n := range b.Nodes {
		proc = b.G.Get(n)
		for proc.WRStatus != ST_WAIT {
			<-ticks
		}
	}
}

func (b *Branch) Resume(newERState int) {
	// Send Wait signals
	// wait until all nodes enter the wait state
	d, _ := time.ParseDuration("0.0001ms")
	ticks := time.Tick(d)
	for _, n := range b.Nodes {
		proc := b.G.Get(n)
		proc.Resume(newERState)
		for proc.WRStatus != ST_RESUME {
			<-ticks
		}
	}
}

func (b *Branch) Close() {
	inDum, outDum := make(chan T), make(chan T)
	close(inDum)
	proc := b.G.Get(b.Start)
	proc.Inputs[0] = inDum
	proc = b.G.Get(b.End)
	proc.Outputs[0] = outDum

}

func AccumulateStats(brs []*Branch, alpha, dt float64, x T) {

	if x == nil || len(brs) == 0 {
		return
	}
	var xc *M
	switch t := x.(type) {
	case *M:
		xc = t
	default:
		return
	}
	if xc == nil {
		return
	}
	for _, b := range brs {
		for i, op := range b.Nodes {
			var s1, s2 float64 = 0, 0
			s1 = xc.TmInfo[op].OutTime.Sub(xc.TmInfo[op].InTime).Seconds() * 1000
			if i < len(b.Nodes)-1 {
				s2 = xc.TmInfo[b.Nodes[i+1]].InTime.Sub(xc.TmInfo[op].OutTime).Seconds() * 1000
			}
			if dt > 0 {
				b.Stats[i].Decay(alpha, dt)
			}

			b.Stats[i].AddVal(gem.Point{s1, s2}, gem.Point{s1 * s1, s2 * s2}, 1)
		}
	}
}
