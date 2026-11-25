// Package orderbad itera map sem ordenar.
package orderbad

func Keys(m map[string]int) []string {
	var out []string
	for k := range m { // want "range sobre map"
		out = append(out, k)
	}
	return out
}

func NoReason(m map[string]int) int {
	n := 0
	//hermespipe:unordered-ok // want "exige um motivo"
	for _, v := range m {
		n += v
	}
	return n
}
