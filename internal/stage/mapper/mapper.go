// Package mapper implementa o stage "map": transforma os campos de cada
// evento com uma lista de operações.
//
// O nome do pacote não é "map" porque map é palavra reservada.
package mapper

import (
	"errors"
	"fmt"
	"strings"

	"github.com/FranciscoHonorat/hermes-pipe/internal/event"
	"github.com/FranciscoHonorat/hermes-pipe/internal/stage"
)

// Config é a configuração do map. Cada operação é uma de:
//
//	rename:origem=destino
//	copy:origem=destino
//	set:campo=valor
//	delete:campo
type Config struct {
	Ops []string `yaml:"ops"`
}

type kind int

const (
	kRename kind = iota
	kCopy
	kSet
	kDelete
)

type op struct {
	kind  kind
	a, b  string
	value any
}

// Map é um stage sem estado: seguro para Process concorrente.
type Map struct {
	ops  []op
	name string
}

// New valida a config.
func New(cfg Config) (*Map, error) {
	if len(cfg.Ops) == 0 {
		return nil, errors.New("map: precisa de pelo menos uma operação em ops")
	}
	m := &Map{}
	for _, src := range cfg.Ops {
		o, err := parseOp(src)
		if err != nil {
			return nil, fmt.Errorf("map: %q: %w", src, err)
		}
		m.ops = append(m.ops, o)
	}
	m.name = "map(" + strings.Join(cfg.Ops, ", ") + ")"
	return m, nil
}

func parseOp(src string) (op, error) {
	verb, arg, ok := strings.Cut(src, ":")
	if !ok {
		return op{}, errors.New("formato é verbo:argumento")
	}
	var o op
	switch verb {
	case "rename", "copy", "set":
		a, b, ok := strings.Cut(arg, "=")
		if !ok {
			return op{}, fmt.Errorf("%s precisa de a=b", verb)
		}
		o.a, o.b = strings.TrimSpace(a), strings.TrimSpace(b)
		switch verb {
		case "rename":
			o.kind = kRename
		case "copy":
			o.kind = kCopy
		default:
			o.kind = kSet
			o.value = stage.ParseLiteral(b)
			o.b = ""
		}
	case "delete":
		o.kind, o.a = kDelete, strings.TrimSpace(arg)
	default:
		return op{}, fmt.Errorf("verbo desconhecido %q (use rename, copy, set ou delete)", verb)
	}
	if o.a == "" || (o.kind != kSet && o.kind != kDelete && o.b == "") {
		return op{}, errors.New("nome de campo vazio")
	}
	if o.a == event.TimeField || o.b == event.TimeField {
		if o.kind != kCopy || o.b == event.TimeField {
			return op{}, errors.New(`o campo "time" não pode ser alterado`)
		}
	}
	return o, nil
}

// Name implementa pipeline.Namer.
func (m *Map) Name() string { return m.name }

// Concurrent implementa pipeline.Concurrent.
func (*Map) Concurrent() bool { return true }

// Process emite uma cópia de e com as operações aplicadas. O evento recebido
// nunca é modificado (I4).
func (m *Map) Process(e event.Event, emit func(event.Event)) error {
	out := e.Clone()
	for _, o := range m.ops {
		switch o.kind {
		case kRename:
			if v, ok := out.Fields[o.a]; ok {
				delete(out.Fields, o.a)
				out.Fields[o.b] = v
			}
		case kCopy:
			if v, ok := out.Fields[o.a]; ok {
				out.Fields[o.b] = v
			}
		case kSet:
			out.Fields[o.a] = o.value
		case kDelete:
			delete(out.Fields, o.a)
		}
	}
	emit(out)
	return nil
}
