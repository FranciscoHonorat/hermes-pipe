// Package filter implementa o stage que deixa passar só os eventos que
// satisfazem uma expressão "campo OP valor".
//
// A linguagem das expressões mora em stage.Cond, compartilhada com o triage.
package filter

import (
	"errors"
	"fmt"
	"strings"

	"github.com/FranciscoHonorat/hermes-pipe/internal/event"
	"github.com/FranciscoHonorat/hermes-pipe/internal/stage"
)

// Config é a configuração do filter. Where é uma ou mais expressões, todas
// precisam ser verdadeiras (E lógico).
type Config struct {
	Where []string `yaml:"where"`
}

// Filter é um stage sem estado: seguro para Process concorrente.
type Filter struct {
	conds []stage.Cond
	name  string
}

// New valida a config e compila as expressões.
func New(cfg Config) (*Filter, error) {
	if len(cfg.Where) == 0 {
		return nil, errors.New("filter: precisa de pelo menos uma expressão em where")
	}
	conds, err := stage.ParseConds(cfg.Where)
	if err != nil {
		return nil, fmt.Errorf("filter: %w", err)
	}
	return &Filter{conds: conds, name: "filter(" + strings.Join(cfg.Where, " && ") + ")"}, nil
}

// Name implementa pipeline.Namer.
func (f *Filter) Name() string { return f.name }

// Concurrent implementa pipeline.Concurrent.
func (*Filter) Concurrent() bool { return true }

// Process emite e se todas as condições forem verdadeiras.
func (f *Filter) Process(e event.Event, emit func(event.Event)) error {
	if stage.All(f.conds, e) {
		emit(e)
	}
	return nil
}
