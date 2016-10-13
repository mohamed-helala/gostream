/*
This package provides the basic functionality for
point and vector operations. These operations
include for example, distance measures,
vector addition and subtraction, ..etc.
*/
package gem

import (
	"fmt"
	"math"
	"strings"
)

// DimError represents a failure due to mismatched dimensions.
type DimError struct {
	Expected int
	Actual   int
}

func (err DimError) Error() string {
	return "rtreego: dimension mismatch"
}

// DistError is an improper distance measurement.  It implements the error
// and is generated when a distance-related assertion fails.
type DistError float64

func (err DistError) Error() string {
	return "rtreego: improper distance"
}

type Point []float64
type Segment struct {
	P1 Point
	P2 Point
}

func NewPoint(n int, init float64) Point {
	x := make(Point, n)
	for i := 0; i < n; i++ {
		x[i] = init
	}
	return x
}

// Calculate the Euclidean distance
// between point x and y
// Dist computes the Euclidean distance between two points p and q.
func (p Point) Dist(q Point) float64 {
	if len(p) != len(q) {
		panic(DimError{len(p), len(q)})
	}
	sum := 0.0
	for i := range p {
		dx := p[i] - q[i]
		sum += dx * dx
	}
	return math.Sqrt(sum)
}

func (x Point) Clone() Point {
	y := make(Point, len(x))
	copy(y, x)
	return y
}

func (x Point) Pow(r float64) Point {
	for i, v := range x {
		x[i] = math.Pow(v, r)
	}
	return x
}

func (p Point) Add(q Point) {
	if len(p) != len(q) {
		panic(DimError{len(p), len(q)})
	}
	for i, qi := range q {
		p[i] += qi
	}
}
func (x Point) AddC(n float64) Point {
	for i, _ := range x {
		x[i] += n
	}
	return x
}

func (p Point) Sub(q Point) {
	if len(p) != len(q) {
		panic(DimError{len(p), len(q)})
	}
	for i, qi := range q {
		p[i] -= qi
	}
}

func (x Point) DivC(n float64) Point {
	for i, _ := range x {
		x[i] /= n
	}
	return x
}

func (x Point) Div(p Point) Point {
	for i, _ := range x {
		x[i] /= p[i]
	}
	return x
}

func (x Point) Mul(p Point) Point {
	for i, _ := range x {
		x[i] *= p[i]
	}
	return x
}

func (x Point) MulC(n float64) Point {
	for i, _ := range x {
		x[i] *= n
	}
	return x
}

func (x Point) Dot(p Point) float64 {
	sum := 0.0
	for i := range x {
		sum += x[i] * p[i]
	}
	return sum
}

func (p Point) Norm() float64 {
	return math.Sqrt(p.Dot(p))
}

func (p Point) AbsMax() float64 {
	m := math.Abs(p[0])
	for i := range p {
		m = math.Max(m, math.Abs(p[i]))
	}
	return m
}

func (p Point) Normalize() {
	aa := p.Norm()
	for i, a := range p {
		p[i] = a / aa
	}
}

// minDist computes the square of the distance from a point to a rectangle.
// If the point is contained in the rectangle then the distance is zero.
//
// Implemented per Definition 2 of "Nearest Neighbor Queries" by
// N. Roussopoulos, S. Kelley and F. Vincent, ACM SIGMOD, pages 71-79, 1995.
func (p Point) MinDist(r *Rect) float64 {
	if len(p) != len(r.P) {
		panic(DimError{len(p), len(r.P)})
	}

	sum := 0.0
	for i, pi := range p {
		a, b := r.C[i]-r.P[i], r.C[i]+r.P[i]
		if pi < a {
			d := pi - a
			sum += d * d
		} else if pi > b {
			d := pi - b
			sum += d * d
		} else {
			sum += 0
		}
	}
	return sum
}

// minMaxDist computes the minimum of the maximum distances from p to points
// on r.  If r is the bounding box of some geometric objects, then there is
// at least one object contained in r within minMaxDist(p, r) of p.
//
// Implemented per Definition 4 of "Nearest Neighbor Queries" by
// N. Roussopoulos, S. Kelley and F. Vincent, ACM SIGMOD, pages 71-79, 1995.
func (p Point) MinMaxDist(r *Rect) float64 {
	if len(p) != len(r.P) {
		panic(DimError{len(p), len(r.P)})
	}

	// by definition, MinMaxDist(p, r) =
	// min{1<=k<=n}(|pk - rmk|^2 + sum{1<=i<=n, i != k}(|pi - rMi|^2))
	// where rmk and rMk are defined as follows:

	rm := func(k int) float64 {
		a, b := r.C[k]-r.P[k], r.C[k]+r.P[k]
		if p[k] <= (a+b)/2 {
			return a
		}
		return b
	}

	rM := func(k int) float64 {
		a, b := r.C[k]-r.P[k], r.C[k]+r.P[k]
		if p[k] >= (a+b)/2 {
			return a
		}
		return b
	}

	// This formula can be computed in linear time by precomputing
	// S = sum{1<=i<=n}(|pi - rMi|^2).

	S := 0.0
	for i := range p {
		d := p[i] - rM(i)
		S += d * d
	}

	// Compute MinMaxDist using the precomputed S.
	min := math.MaxFloat64
	for k := range p {
		d1 := p[k] - rM(k)
		d2 := p[k] - rm(k)
		d := S - d1*d1 + d2*d2
		if d < min {
			min = d
		}
	}

	return min
}

// The coordinate of the point of the rectangle at i
func (r *Rect) PointCoord(i int) float64 {
	return r.C[i]
}

// Rect represents a subset of n-dimensional Euclidean space of the form
// [a1, b1] x [a2, b2] x ... x [an, bn], where ai < bi for all 1 <= i <= n.
type Rect struct {
	C, P Point // Enforced by NewRect:  c[i] - p[i] <= c[i]+ p[i] for all i.
}

// returns the lengths of current rectangle
func (r *Rect) Lengths() []float64 {
	return r.P
}

// Returns true if the two rectangles are equal
func (r *Rect) Equal(other *Rect) bool {
	for i, ci := range r.C {
		if ci != other.C[i] {
			return false
		}
		if r.P[i] != other.P[i] {
			return false
		}
	}
	return true
}

func (r *Rect) String() string {
	s := make([]string, len(r.P))
	for i, a := range r.C {
		b := r.P[i]
		s[i] = fmt.Sprintf("[%.2f, %.2f]", a, b)
	}
	return strings.Join(s, "x")
}

// NewRect constructs and returns a pointer to a Rect given a center point and
// the lengths of each dimension. The point c should be the mean point
// on the rectangle (in every dimension) and every length should be positive.
func NewRect(c Point, lengths []float64) (r *Rect, err error) {
	r = new(Rect)
	r.C = c
	if len(c) != len(lengths) {
		err = &DimError{len(c), len(lengths)}
		return
	}
	r.P = make([]float64, len(c))
	for i := range c {
		if lengths[i] <= 0 {
			err = DistError(lengths[i])
			return
		}
		r.P[i] = lengths[i]
	}
	return
}

// size computes the measure of a rectangle (the product of its side lengths).
func (r *Rect) Size() float64 {
	size := 1.0
	for _, a := range r.P {
		size *= (2 * a)
	}
	return size
}

// margin computes the sum of the edge lengths of a rectangle.
func (r *Rect) Margin() float64 {
	// The number of edges in an n-dimensional rectangle is n * 2^(n-1)
	// (http://en.wikipedia.org/wiki/Hypercube_graph).  Thus the number
	// of edges of length (ai - bi), where the rectangle is determined
	// by p = (a1, a2, ..., an) and q = (b1, b2, ..., bn), is 2^(n-1).
	//
	// The margin of the rectangle, then, is given by the formula
	// 2^(n-1) * [(b1 - a1) + (b2 - a2) + ... + (bn - an)].
	dim := len(r.P)
	sum := 0.0
	for _, a := range r.P {
		sum += 2 * a
	}
	return math.Pow(2, float64(dim-1)) * sum
}

// containsPoint tests whether p is located inside or on the boundary of r.
func (r *Rect) containsPoint(p Point) bool {
	if len(p) != len(r.P) {
		panic(DimError{len(r.P), len(p)})
	}

	for i, a := range p {
		// p is contained in (or on) r if and only if c-p <= a <= c+p for
		// every dimension.
		if a < (r.C[i]-r.P[i]) || a > (r.C[i]+r.P[i]) {
			return false
		}
	}

	return true
}

func (r *Rect) Bounds() *Rect {
	return r
}

// containsRect tests whether r2 is is located inside r1.
func (r1 *Rect) ContainsRect(r2 *Rect) bool {
	if len(r1.P) != len(r2.P) {
		panic(DimError{len(r1.P), len(r2.P)})
	}

	for i, pi := range r1.P {
		a1, b1, a2, b2 := r1.C[i]-pi, r1.C[i]+pi,
			r2.C[i]-r2.P[i], r2.C[i]+r2.P[i]
		// enforced by constructor: a1 <= b1 and a2 <= b2.
		// so containment holds if and only if a1 <= a2 <= b2 <= b1
		// for every dimension.
		if a1 > a2 || b2 > b1 {
			return false
		}
	}

	return true
}

// intersect computes the intersection of two rectangles.  If no intersection
// exists, the intersection is nil.
func Intersect(r1, r2 *Rect) *Rect {
	dim := len(r1.P)
	if len(r2.P) != dim {
		panic(DimError{dim, len(r2.P)})
	}

	// There are four cases of overlap:
	//
	//     1.  a1------------b1
	//              a2------------b2
	//              p--------q
	//
	//     2.       a1------------b1
	//         a2------------b2
	//              p--------q
	//
	//     3.  a1-----------------b1
	//              a2-------b2
	//              p--------q
	//
	//     4.       a1-------b1
	//         a2-----------------b2
	//              p--------q
	//
	// Thus there are only two cases of non-overlap:
	//
	//     1. a1------b1
	//                    a2------b2
	//
	//     2.             a1------b1
	//        a2------b2
	//
	// Enforced by constructor: a1 <= b1 and a2 <= b2.  So we can just
	// check the endpoints.

	p := make([]float64, dim)
	c := make([]float64, dim)
	for i, _ := range p {
		a1, b1, a2, b2 := r1.C[i]-r1.P[i], r1.C[i]+r1.P[i],
			r2.C[i]-r2.P[i], r2.C[i]+r2.P[i]
		if b2 <= a1 || b1 <= a2 {
			return nil
		}

		pn, qn := math.Max(a1, a2), math.Min(b1, b2)
		c[i] = (qn + pn) / 2
		p[i] = (qn - pn) / 2
	}
	return &Rect{c, p}
}

// ToRect constructs a rectangle containing p with side lengths tol.
func (q Point) ToRect(tol float64) *Rect {
	dim := len(q)
	c, p := make([]float64, dim), make([]float64, dim)
	for i := range q {
		c[i] = q[i]
		p[i] = tol
	}
	return &Rect{c, p}
}

// boundingBox constructs the smallest rectangle containing both r1 and r2.
func BoundingBox(r1, r2 *Rect) (bb *Rect) {
	bb = new(Rect)
	dim := len(r1.P)
	bb.P = make([]float64, dim)
	bb.C = make([]float64, dim)
	if len(r2.P) != dim {
		panic(DimError{dim, len(r2.P)})
	}
	for i := 0; i < dim; i++ {
		a1, b1, a2, b2, a, b := r1.C[i]-r1.P[i], r1.C[i]+r1.P[i],
			r2.C[i]-r2.P[i], r2.C[i]+r2.P[i], float64(0), float64(0)
		if a1 <= a2 {
			a = a1
		} else {
			a = a2
		}
		if b1 <= b2 {
			b = b2
		} else {
			b = b1
		}
		bb.C[i] = (b + a) / 2
		bb.P[i] = (b - a) / 2
	}
	return
}

// boundingBoxN constructs the smallest rectangle containing all of r...
func BoundingBoxN(rects ...*Rect) (bb *Rect) {
	if len(rects) == 1 {
		bb = rects[0]
		return
	}
	bb = BoundingBox(rects[0], rects[1])
	for _, rect := range rects[2:] {
		bb = BoundingBox(bb, rect)
	}
	return
}

type Point2D []int32
type Contour []int32
type Segment2D struct {
	P1 Point2D
	P2 Point2D
}

func NewPoint2D(init int32) Point2D {
	p := make(Point2D, 2)
	for i, _ := range p {
		p[i] = init
	}
	return p
}

func (p Point2D) X() int32 {
	return p[0]
}

func (p Point2D) Y() int32 {
	return p[1]
}

func (c Contour) ToPoint2D() []Point2D {
	s := len(c) / 2
	pts := make([]Point2D, s)
	for i := 0; i < s; i += 2 {
		pts[i] = ([]int32)(c[i : i+2])
	}
	return pts
}

func (c Contour) ToSegments() []*Segment2D {
	s := len(c) / 2
	segs := make([]*Segment2D, s-1)
	for i := 0; i < s; i += 2 {
		segs[i] = &Segment2D{([]int32)(c[i : i+2]), ([]int32)(c[i+2 : i+4])}
	}
	return segs
}
