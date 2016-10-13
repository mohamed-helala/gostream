/** This file implements the load balancing and scheduling methods used in tuning the structure of
software pipelines costructed using our algebra

Copyright (C) 2015 Mohamed Helala <mohamed.helala@gmail.com>
**/
package loopy

import (
	// "fmt"
	"sort"
)

/** Implementing the Choi and Narahari O(N(N-K)) dynamic programming algorithm discussed in
Ali Pinar and Cevdet Aykanat. 2004. Fast optimal load balancing algorithms for 1D partitioning.
J. Parallel Distrib. Comput. 64, 8 (August 2004), 974-996
**/
func calcBottleNeck(W []float64, K int) float64 {
	var (
		N int         = len(W)
		B [][]float64 = make([][]float64, K)
	)
	for i := 0; i < K; i++ {
		B[i] = make([]float64, N)
	}
	for i, w := range W {
		B[0][i] = w
	}
	for k := 1; k < K; k++ {
		j := k - 1
		for i := k - 1; i < (N - K + k + 1); i++ {
			if (W[i] - W[j]) > B[k-1][j] {
				j += 1
				for (W[i] - W[j]) > B[k-1][j] {
					j += 1
				}
				if (W[i] - W[j-1]) < B[k-1][j] {
					j -= 1
					B[k][i] = W[i] - W[j]
				} else {
					B[k][i] = B[k-1][j]
				}
			} else {
				B[k][i] = B[k-1][j]
			}
		}
	}
	return B[K-1][N-1]
}

func prefixSum(T []float64) []float64 {
	var (
		W   []float64 = make([]float64, len(T))
		sum float64   = 0
	)
	for i, t := range T {
		W[i] = t + sum
		sum += t
	}
	return W
}

func probe(W []float64, B float64, K int) ([]int, bool) {
	var (
		S    []int   = make([]int, K)
		Bsum float64 = B
		Wt   float64 = W[len(W)-1]
		Ws   []float64
		N    int = len(W)
	)
	f := func(x int) bool {
		if Ws[x] > Bsum {
			return true
		}
		return false
	}
	S[0] = 0
	for k := 1; k < K; k++ {
		if Bsum >= Wt {
			return S, true
		}
		Ws = W[S[k-1]:N]
		S[k] = sort.Search(len(Ws), f) + S[k-1]
		if S[k]-1 < 0 {
			return S, false
		}
		Bsum = W[S[k]-1] + B
	}
	return S, false
}

func CCPSolveDB(T []float64, K int) ([]int, float64, bool) {
	W := prefixSum(T)
	Bopt := calcBottleNeck(W, K)
	S, u := probe(W, Bopt, K)
	return S, Bopt, u
}
