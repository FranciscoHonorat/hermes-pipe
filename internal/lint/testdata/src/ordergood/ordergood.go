// Package ordergood itera map de forma determinística.
package ordergood

import (
	"slices"
	"sort"
)

func Keys(m map[string]int) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	slices.Sort(out)
	return out
}

func KeysOld(m map[int]bool) []int {
	var out []int
	for k := range m {
		out = append(out, k)
	}
	sort.Ints(out)
	return out
}

func Sum(m map[string]int) int {
	n := 0
	//hermespipe:unordered-ok soma é comutativa
	for _, v := range m {
		n += v
	}
	return n
}

func Slice(xs []int) int {
	n := 0
	for _, x := range xs {
		n += x
	}
	return n
}
