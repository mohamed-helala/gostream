package gem

import (
	// "fmt"
	"testing"
)


func TestLineIntersect(t *testing.T) {
	p1, p2 := Point{0, 0}, Point{100, 100}
	p3, p4 := Point{0, 100}, Point{100, 0}

	p, s := IntersectLines(p1[0], p1[1], p2[0], p2[1], p3[0], p3[1], p4[0], p4[1])

	if !(p[0]== 50 && p[1] == 50) && s !=1{
		t.Errorf("expected intersection point [50,50]")
	}
}

func TestParallelIntersect(t *testing.T) {
	p1, p2 := Point{0, 0}, Point{100, 100}
	p3, p4 := Point{10, 10}, Point{110, 110}

	_, s := IntersectLines(p1[0], p1[1], p2[0], p2[1], p3[0], p3[1], p4[0], p4[1])

	if s >=0{
		t.Errorf("expected parallel lines")
	}
}

func TestPointInPoly(t *testing.T) {
	x0 := []float64{0, 0, 100, 100}
	y0 := []float64{0, 100, 100, 0}

	if !IsPointInPoly(x0, y0, 99, 99) {
		t.Errorf("(0, 0) inside polygon")
	}
	if !IsPointInPoly(x0, y0, 1, 20) {
		t.Errorf("(1, 20) inside polygon")
	}
	if IsPointInPoly(x0, y0, -5, 20) {
		t.Errorf("(-5, 20) outside polygon")
	}
	if IsPointInPoly(x0, y0, 120, 20) {
		t.Errorf("(-5, 20) outside polygon")
	}
}


func TestPolyArea(t *testing.T) {
	x0 := []float64{0, 0, 100, 100}
	y0 := []float64{0, 100, 100, 0}

	if area := PolyArea(x0, y0); area != 10000{
		t.Errorf("Area should be 10000. Now it is %v", area)
	}
}

