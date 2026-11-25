// Package filter implementa o stage que deixa passar só os eventos que
// satisfazem uma expressão "campo OP valor".
package filter

import (
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/FranciscoHonorat/hermes-pipe/internal/event"
	"github.com/FranciscoHonorat/hermes-pipe/internal/stage"
)

// Config é a configuração do filter. Where é uma ou mais expressões, todas
// precisam ser verdadeiras (E lógico).
type Config struct {
	Where []string `yaml:"where"`
}

type op int

const (
	opEq op = iota
	opNe
	opLt
	opLe
	opGt
	opGe
	opMatch
	opNotMatch
)

// A ordem importa: operadores de dois caracteres antes dos de um.
var ops = []struct {
	tok string
	op  op
}{
	{"==", opEq}, {"!=", opNe}, {"<=", opLe}, {">=", opGe},
	{"=~", opMatch}, {"!~", opNotMatch}, {"<", opLt}, {">", opGt},
}

type cond struct {
	src   string
	field string
	op    op
	value any
	re    *regexp.Regexp // compilada uma vez, no New
}

// Filter é um stage sem estado: seguro para Process concorrente.
type Filter struct {
	conds []cond
	name  string
}

// New valida a config e compila as expressões.
func New(cfg Config) (*Filter, error) {
	if len(cfg.Where) == 0 {
		return nil, errors.New("filter: precisa de pelo menos uma expressão em where")
	}
	f := &Filter{}
	for _, src := range cfg.Where {
		c, err := parseCond(src)
		if err != nil {
			return nil, fmt.Errorf("filter: %q: %w", src, err)
		}
		f.conds = append(f.conds, c)
	}
	f.name = "filter(" + strings.Join(cfg.Where, " && ") + ")"
	return f, nil
}

func parseCond(src string) (cond, error) {
	best, bestOp := -1, ops[0]
	for _, o := range ops {
		if i := strings.Index(src, o.tok); i >= 0 && (best < 0 || i < best) {
			best, bestOp = i, o
		}
	}
	if best < 0 {
		return cond{}, errors.New("operador ausente (use ==, !=, <, <=, >, >=, =~ ou !~)")
	}
	c := cond{
		src:   src,
		field: strings.TrimSpace(src[:best]),
		op:    bestOp.op,
	}
	raw := strings.TrimSpace(src[best+len(bestOp.tok):])
	if c.field == "" {
		return cond{}, errors.New("campo vazio")
	}
	if raw == "" {
		return cond{}, errors.New("valor vazio")
	}
	c.value = stage.ParseLiteral(raw)
	if c.op == opMatch || c.op == opNotMatch {
		pat, ok := c.value.(string)
		if !ok {
			pat = raw
		}
		re, err := regexp.Compile(pat)
		if err != nil {
			return cond{}, err
		}
		c.re = re
	}
	return c, nil
}

// Name implementa pipeline.Namer.
func (f *Filter) Name() string { return f.name }

// Concurrent implementa pipeline.Concurrent.
func (*Filter) Concurrent() bool { return true }

// Process emite e se todas as condições forem verdadeiras.
func (f *Filter) Process(e event.Event, emit func(event.Event)) error {
	for i := range f.conds {
		if !f.conds[i].eval(e) {
			return nil
		}
	}
	emit(e)
	return nil
}

func (c *cond) eval(e event.Event) bool {
	v, present := e.Get(c.field)
	switch c.op {
	case opMatch, opNotMatch:
		s, ok := v.(string)
		matched := ok && c.re.MatchString(s)
		return matched == (c.op == opMatch)
	case opEq, opNe:
		eq := false
		if !present {
			eq = c.value == nil // campo ausente == null
		} else if r, ok := stage.Compare(v, c.value); ok {
			eq = r == 0
		}
		return eq == (c.op == opEq)
	}
	if !present {
		return false
	}
	r, ok := stage.Compare(v, c.value)
	if !ok {
		return false
	}
	switch c.op {
	case opLt:
		return r < 0
	case opLe:
		return r <= 0
	case opGt:
		return r > 0
	default: // opGe
		return r >= 0
	}
}
