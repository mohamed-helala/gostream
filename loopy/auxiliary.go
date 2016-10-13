package loopy

import (
	"gem"
	"math"
)

const (
	OP_SOURCE int = iota
	OP_GROUND
	OP_MAP
	OP_REDUCE
	OP_FILTER
	OP_COPY
	OP_COPYN
	OP_LATCH
	OP_CUT
	OP_LEFT_MULTIPLY
	OP_MULTIPLY
	OP_ADD
	OP_SCATTER
	OP_MERGE
	OP_SPLIT
	OP_MISC
	OP_COMPOSITE
)

const (
	OP_ATTRIB_NAME int = iota
	OP_ATTRIB_FUNC_IDX
	OP_ATTRIB_WR_STATUS
	OP_ATTRIB_ER_STATUS
	OP_ATTRIB_PREV_PROC // internal use only
	OP_ATTRIB_GRAPH_REMOVED
)

const (
	SHUFFLE_GROUPING int = iota
	ALL_GROUPING
	HASH_GROUPING
	NO_GROUPING
	BACK_GROUPING
)

// Status
const (
	ST_RUN int = iota
	ST_EXIT
)

const (
	ST_REQWAIT int = iota
	ST_WAIT
	ST_RESUME
)

const (
	PROC_ENTER_TIME int = iota
	PROC_LEAVE_TIME
	PROC_BOTH_TIME
)

//#################################################################
//                   Sufficient Statisitcs
//#################################################################

// SStats represents the sufficient statistics
type SStats struct {
	Xs  gem.Point
	Xss gem.Point
	N   float64
}

func (n *SStats) Add(e *SStats) {
	n.Xs.Add(e.Xs)
	n.Xss.Add(e.Xss)
	n.N += e.N
}

func (n *SStats) AddVal(xs, xss gem.Point, ns float64) {
	n.Xs.Add(xs)
	n.Xss.Add(xss)
	n.N += ns
}

func (n *SStats) Sub(e *SStats) {
	n.Xs.Sub(e.Xs)
	n.Xss.Sub(e.Xss)
	n.N -= e.N
}

func (n *SStats) Clear() {
	for i := range n.Xs {
		n.Xs[i] = 0
		n.Xss[i] = 0
	}
	n.N = 0
}

func (n *SStats) Clone() *SStats {
	return &SStats{n.Xs.Clone(), n.Xss.Clone(), n.N}
}

func (n *SStats) Decay(alpha float64, dt float64) {
	w := math.Pow(2, -alpha*dt)
	n.Xs.MulC(w)
	n.Xss.MulC(w)
	n.N *= w
}

func (n *SStats) Mean() gem.Point {
	c := make(gem.Point, len(n.Xs))
	N := math.Max(0.00000001, n.N)
	for i := range c {
		c[i] = n.Xs[i] / N
	}
	return c
}

func (n *SStats) MV() (c gem.Point, p gem.Point) {
	c, p = make(gem.Point, len(n.Xs)), make(gem.Point, len(n.Xs))
	N := math.Max(0.00000001, n.N)
	for i := range c {
		c[i] = n.Xs[i] / N
		p[i] = (n.Xss[i] / N) - math.Pow(c[i], 2)
		p[i] = math.Max(0.00000001, p[i])
	}
	return c, p
}
