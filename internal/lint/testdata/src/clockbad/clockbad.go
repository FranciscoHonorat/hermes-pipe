// Package clockbad viola a regra clock.
package clockbad

import (
	"math/rand"
	randv2 "math/rand/v2"
	"time"
)

func Stamp() time.Time {
	return time.Now() // want "relógio de parede \\(time.Now\\)"
}

func Age(t time.Time) time.Duration {
	return time.Since(t) // want "time.Since"
}

func Wait() <-chan time.Time { // want "chan"
	return time.After(time.Second) // want "time.After"
}

func Dice() int {
	return rand.Intn(6) + randv2.IntN(6) // want "math/rand" "math/rand/v2"
}
