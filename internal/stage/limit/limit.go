// Package limit implementa o stage que deixa passar só os N primeiros eventos,
// como head.
package limit

import (
	"errors"
	"strconv"

	"github.com/FranciscoHonorat/hermes-pipe/internal/event"
	"github.com/FranciscoHonorat/hermes-pipe/internal/stage"
)

// ErrLimitReached é devolvido junto com o N-ésimo evento. O runner o traduz
// em parada suave: o producer para de ler, e o que já estava em trânsito
// chega aqui e é descartado.
var ErrLimitReached error = &stage.StopError{Reason: "limite atingido"}

// Config é a configuração do limit.
type Config struct {
	N int64 `yaml:"n"`
}

// Limit tem estado (o contador), por isso não é Concurrent.
type Limit struct {
	n, seen int64
}

// New valida a config.
func New(cfg Config) (*Limit, error) {
	if cfg.N <= 0 {
		return nil, errors.New("limit: n precisa ser > 0")
	}
	return &Limit{n: cfg.N}, nil
}

// Name implementa pipeline.Namer.
func (l *Limit) Name() string { return "limit(" + strconv.FormatInt(l.n, 10) + ")" }

// Process emite os n primeiros eventos e descarta o resto.
func (l *Limit) Process(e event.Event, emit func(event.Event)) error {
	if l.seen >= l.n {
		return nil
	}
	l.seen++
	emit(e)
	if l.seen == l.n {
		return ErrLimitReached
	}
	return nil
}
