package gem

import (
	"math"
)

func PerpDistToLine(x0, y0, x1, y1, x2, y2 float64) float64 {
	d := math.Abs((x2-x1)*(y1-y0)-(x1-x0)*(y2-y1)) /
		math.Sqrt(math.Pow((x2-x1), 2)+math.Pow((y2-y1), 2))
	return d
}

func HghPars(x0, y0, x1, y1 float64) (rho float64, theta float64) {
	theta = math.Atan2((y1 - y0), (x1 - x0))
	if theta < 0 {
		theta += math.Pi
	}
	rho = PerpDistToLine(0, 0, x0, y0, x1, y1)
	return rho, theta
}

func IntMax(a, b int) int {
	if a > b {
		return a
	} else {
		return b
	}
}

/**
* Compute the intersection between two line segments, or two lines
* of infinite length.
*
*   x0              X coordinate first end point first line segment.
*   y0              Y coordinate first end point first line segment.
*   x1              X coordinate second end point first line segment.
*   y1              Y coordinate second end point first line segment.
*   x2              X coordinate first end point second line segment.
*   y2              Y coordinate first end point second line segment.
*   x3              X coordinate second end point second line segment.
*   y3              Y coordinate second end point second line segment.
*
*  return p, s
*    p  is the intersection point
*    s is the intersection state and can be:
*         -1 if lines are parallel (x,y unset),
*         -2 if lines are parallel and overlapping (x, y center)
*          0 if intesrection outside segments (x,y set)
*         +1 if segments intersect (x,y set)
 */
func IntersectLines(x0, y0, x1, y1, x2, y2, x3, y3 float64) (p Point, s int) {

	var (
		LIMIT, INFINITY              float64 = 1e-5, 1e10
		x, y, a0, a1, b0, b1         float64
		distanceFrom1, distanceFrom2 float64 = 0, 0
	)
	p = Point{0, 0}

	if a0 = (y0 - y1) / (x0 - x1); math.Abs(x0-x1) < LIMIT {
		a0 = INFINITY
	}
	if a1 = (y2 - y3) / (x2 - x3); math.Abs(x2-x3) < LIMIT {
		a1 = INFINITY
	}
	b0, b1 = y0-a0*x0, y2-a1*x2

	// Check if lines are parallel
	if math.Abs(a0-a1) < LIMIT {
		if !(math.Abs(b0-b1) < LIMIT) {
			return nil, -1 // Parallell non-overlapping
		} else {
			if math.Abs(x0-x1) < LIMIT {
				if math.Min(y0, y1) < math.Max(y2, y3) || math.Max(y0, y1) > math.Min(y2, y3) {
					twoMiddle := y0 + y1 + y2 + y3 -
						math.Min(y0, math.Min(y1, math.Min(y2, y3))) -
						math.Max(y0, math.Max(y1, math.Max(y2, y3)))
					y = (twoMiddle) / 2.0
					x = (y - b0) / a0
				} else {
					return nil, -1 // Parallell non-overlapping
				}
			} else {
				if math.Min(x0, x1) < math.Max(x2, x3) || math.Max(x0, x1) > math.Min(x2, x3) {
					twoMiddle := x0 + x1 + x2 + x3 -
						math.Min(x0, math.Min(x1, math.Min(x2, x3))) -
						math.Max(x0, math.Max(x1, math.Max(x2, x3)))
					x = (twoMiddle) / 2.0
					y = a0*x + b0
				} else {
					return nil, -1
				}
			}
			p[0], p[1] = x, y
			return p, -2
		}
	}
	// Find correct intersection point
	if math.Abs(a0-INFINITY) < LIMIT {
		x = x0
		y = a1*x + b1
	} else if math.Abs(a1-INFINITY) < LIMIT {
		x = x2
		y = a0*x + b0
	} else {
		x = -(b0 - b1) / (a0 - a1)
		y = a0*x + b0
	}

	p[0], p[1] = x, y

	// Then check if intersection is within line segments
	if math.Abs(x0-x1) < LIMIT {
		if y0 < y1 {
			if y < y0 {
				distanceFrom1 = math.Sqrt(math.Pow(x-x0, 2) - math.Pow(y-y0, 2))
			} else {
				if y > y1 {
					distanceFrom1 = math.Sqrt(math.Pow(x-x1, 2) - math.Pow(y-y1, 2))
				}
			}
		} else {
			if y < y1 {
				distanceFrom1 = math.Sqrt(math.Pow(x-x1, 2) - math.Pow(y-y1, 2))
			} else {
				if y > y0 {
					distanceFrom1 = math.Sqrt(math.Pow(x-x0, 2) - math.Pow(y-y0, 2))
				}
			}
		}
	} else {
		if x0 < x1 {
			if x < x0 {
				distanceFrom1 = math.Sqrt(math.Pow(x-x0, 2) - math.Pow(y-y0, 2))
			} else {
				if x > x1 {
					distanceFrom1 = math.Sqrt(math.Pow(x-x1, 2) - math.Pow(y-y1, 2))
				}
			}
		} else {
			if x < x1 {
				distanceFrom1 = math.Sqrt(math.Pow(x-x1, 2) - math.Pow(y-y1, 2))
			} else {
				if x > x0 {
					distanceFrom1 = math.Sqrt(math.Pow(x-x0, 2) - math.Pow(y-y0, 2))
				}
			}
		}
	}

	if math.Abs(x2-x3) < LIMIT {
		if y2 < y3 {
			if y < y2 {
				distanceFrom2 = math.Sqrt(math.Pow(x-x2, 2) - math.Pow(y-y2, 2))
			} else {
				if y > y3 {
					distanceFrom2 = math.Sqrt(math.Pow(x-x3, 2) - math.Pow(y-y3, 2))
				}
			}
		} else {
			if y < y3 {
				distanceFrom2 = math.Sqrt(math.Pow(x-x3, 2) - math.Pow(y-y3, 2))
			} else {
				if y > y2 {
					distanceFrom2 = math.Sqrt(math.Pow(x-x2, 2) - math.Pow(y-y2, 2))
				}
			}
		}
	} else {
		if x2 < x3 {
			if x < x2 {
				distanceFrom2 = math.Sqrt(math.Pow(x-x2, 2) - math.Pow(y-y2, 2))
			} else {
				if x > x3 {
					distanceFrom2 = math.Sqrt(math.Pow(x-x3, 2) - math.Pow(y-y3, 2))
				}
			}
		} else {
			if x < x3 {
				distanceFrom2 = math.Sqrt(math.Pow(x-x3, 2) - math.Pow(y-y3, 2))
			} else {
				if x > x2 {
					distanceFrom2 = math.Sqrt(math.Pow(x-x2, 2) - math.Pow(y-y2, 2))
				}
			}
		}
	}

	if math.Abs(distanceFrom1-0) < LIMIT && math.Abs(distanceFrom2-0) < LIMIT {
		return p, 1
	}
	return p, 0
}

func IsPointInPoly(x, y []float64, x0, y0 float64) bool {
	var (
		isInside bool = false
		npts, j  int  = len(x), 0
	)
	for i := range x {
		if j == npts {
			j = 0
		}
		if y[i] < y0 && y[j] >= y0 || y[j] < y0 && y[i] >= y0 {
			if x[i]+((y0-y[i])/(y[j]-y[i]))*(x[j]-x[i]) < x0 {
				isInside = !isInside
			}
		}
	}
	return isInside
}

func IsLineIntersectingPoly(x, y []float64, x0, y0, x1, y1 float64) bool {
	var (
		npts, j int = len(x), 0
	)
	for i := range x {
		if i == npts-1 {
			j = 0
		} else {
			j = i + 1
		}
		_, s := IntersectLines(x0, y0, x1, y1, x[i], y[i], x[j], y[j])
		if s == 1 {
			return true
		}
	}
	return false
}

func GetSegmentInPoly(x, y []float64, x0, y0, x1, y1 float64) *Segment {
	var (
		npts, j int      = len(x), 0
		seg     *Segment = nil
	)
	for i := range x {
		if i == npts-1 {
			j = 0
		} else {
			j = i + 1
		}
		p, s := IntersectLines(x0, y0, x1, y1, x[i], y[i], x[j], y[j])
		if s == 1 {
			if seg == nil {
				seg = &Segment{}
				seg.P1 = p
			} else {
				seg.P2 = p
			}
		}
	}
	if seg != nil && seg.P2 == nil {
		return nil
	}
	return seg
}

func PolyArea(x, y []float64) float64 {
	area, j := float64(0), len(x)-1
	for i := range x {
		area = area + (x[j]+x[i])*(y[j]-y[i])
		j = i //j is previous vertex to i
	}
	return area / 2
}

