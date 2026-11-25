package stage

import (
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/FranciscoHonorat/hermes-pipe/internal/event"
)

// Cond é uma expressão "campo OP valor" compilada. É o avaliador comum do
// filter e do triage: os dois aceitam exatamente a mesma linguagem.
//
// Operadores: ==, !=, <, <=, >, >=, =~ (regex) e !~. Campo ausente == null.
// Tipos diferentes nunca são iguais nem ordenáveis.
type Cond struct {
	src   string
	field string
	op    condOp
	value any
	re    *regexp.Regexp // compilada uma vez, no ParseCond
}

type condOp int

const (
	opEq condOp = iota
	opNe
	opLt
	opLe
	opGt
	opGe
	opMatch
	opNotMatch
)

// A ordem importa: operadores de dois caracteres antes dos de um.
var condOps = []struct {
	tok string
	op  condOp
}{
	{"==", opEq}, {"!=", opNe}, {"<=", opLe}, {">=", opGe},
	{"=~", opMatch}, {"!~", opNotMatch}, {"<", opLt}, {">", opGt},
}

// ParseCond compila uma expressão.
func ParseCond(src string) (Cond, error) {
	best, bestOp := -1, condOps[0]
	for _, o := range condOps {
		if i := strings.Index(src, o.tok); i >= 0 && (best < 0 || i < best) {
			best, bestOp = i, o
		}
	}
	if best < 0 {
		return Cond{}, errors.New("operador ausente (use ==, !=, <, <=, >, >=, =~ ou !~)")
	}
	c := Cond{
		src:   src,
		field: strings.TrimSpace(src[:best]),
		op:    bestOp.op,
	}
	raw := strings.TrimSpace(src[best+len(bestOp.tok):])
	if c.field == "" {
		return Cond{}, errors.New("campo vazio")
	}
	if raw == "" {
		return Cond{}, errors.New("valor vazio")
	}
	c.value = ParseLiteral(raw)
	if c.op == opMatch || c.op == opNotMatch {
		pat, ok := c.value.(string)
		if !ok {
			pat = raw
		}
		re, err := regexp.Compile(pat)
		if err != nil {
			return Cond{}, err
		}
		c.re = re
	}
	return c, nil
}

// ParseConds compila uma lista de expressões; o erro cita a expressão.
func ParseConds(srcs []string) ([]Cond, error) {
	out := make([]Cond, 0, len(srcs))
	for _, src := range srcs {
		c, err := ParseCond(src)
		if err != nil {
			return nil, fmt.Errorf("%q: %w", src, err)
		}
		out = append(out, c)
	}
	return out, nil
}

// String devolve a expressão original.
func (c *Cond) String() string { return c.src }

// All diz se todas as condições são verdadeiras para e (E lógico). Lista
// vazia é verdadeira.
func All(conds []Cond, e event.Event) bool {
	for i := range conds {
		if !conds[i].Eval(e) {
			return false
		}
	}
	return true
}

// Eval avalia a condição em e. Não aloca.
func (c *Cond) Eval(e event.Event) bool {
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
		} else if r, ok := Compare(v, c.value); ok {
			eq = r == 0
		}
		return eq == (c.op == opEq)
	}
	if !present {
		return false
	}
	r, ok := Compare(v, c.value)
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
