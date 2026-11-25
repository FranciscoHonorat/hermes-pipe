// Package clockgood usa time sem relógio de parede.
package clockgood

import "time"

// Later usa métodos de time.Time e constantes: permitido.
func Later(a, b time.Time) bool {
	return a.After(b) && a.Sub(b) > time.Second && !a.Before(b.Add(time.Minute))
}

// Shadow: uma variável chamada time não é o pacote.
func Shadow() int {
	type clock struct{ Now func() int }
	time := clock{Now: func() int { return 1 }}
	return time.Now()
}
