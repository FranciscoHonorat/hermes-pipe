// Package stage tem o contrato dos stages e o que eles compartilham.
//
// Um Stage é uma função pura com estado opcional: recebe um event.Event, emite
// zero ou mais, ou devolve erro. Não conhece channels, contexto, config,
// relógio nem I/O (I4). Stages não importam internal/pipeline nem uns aos
// outros: o que for comum mora aqui, no pacote pai.
//
// A interface que o runner usa é declarada em internal/pipeline; como
// interfaces em Go são satisfeitas por estrutura, os tipos daqui a satisfazem
// sem importar o runner.
package stage

import (
	"encoding/json"
	"strconv"
	"strings"

	"github.com/FranciscoHonorat/hermes-pipe/internal/event"
)

// Stage é a mesma interface de pipeline.Stage, repetida aqui para que os
// stages e o teste de contrato não dependam do runner.
type Stage interface {
	Process(e event.Event, emit func(event.Event)) error
}

// Flusher é implementado por stages com estado.
type Flusher interface {
	Flush(emit func(event.Event)) error
}

// StopError pede ao runner uma parada suave: o producer para de ler e o que
// está em trânsito é drenado.
type StopError struct{ Reason string }

func (e *StopError) Error() string { return e.Reason }

// Stop marca o erro como pedido de parada suave.
func (*StopError) Stop() bool { return true }

type fatalError struct{ err error }

func (e fatalError) Error() string { return e.err.Error() }
func (e fatalError) Unwrap() error { return e.err }
func (fatalError) Fatal() bool     { return true }

// Fatal embrulha err para que o runner cancele o pipeline inteiro, em vez de
// contar como erro do evento.
func Fatal(err error) error { return fatalError{err} }

// ParseLiteral interpreta um literal de expressão: "texto entre aspas" (JSON),
// true, false, null, número ou, se nada disso, a palavra como string.
func ParseLiteral(s string) any {
	s = strings.TrimSpace(s)
	switch s {
	case "true":
		return true
	case "false":
		return false
	case "null":
		return nil
	}
	if len(s) >= 2 && s[0] == '"' {
		var out string
		if err := json.Unmarshal([]byte(s), &out); err == nil {
			return out
		}
	}
	if _, err := strconv.ParseFloat(s, 64); err == nil {
		return json.Number(s)
	}
	return s
}

// ToFloat converte um valor numérico de evento (json.Number, float64, int) em
// float64.
func ToFloat(v any) (float64, bool) {
	switch v := v.(type) {
	case json.Number:
		f, err := strconv.ParseFloat(string(v), 64)
		return f, err == nil
	case float64:
		return v, true
	case int:
		return float64(v), true
	case int64:
		return float64(v), true
	}
	return 0, false
}

// Compare compara dois valores de evento do mesmo tipo. ok é false quando os
// tipos não são comparáveis (número com string, por exemplo).
func Compare(a, b any) (c int, ok bool) {
	if fa, aok := ToFloat(a); aok {
		fb, bok := ToFloat(b)
		if !bok {
			return 0, false
		}
		switch {
		case fa < fb:
			return -1, true
		case fa > fb:
			return 1, true
		}
		return 0, true
	}
	switch a := a.(type) {
	case string:
		b, ok := b.(string)
		if !ok {
			return 0, false
		}
		return strings.Compare(a, b), true
	case bool:
		b, ok := b.(bool)
		if !ok {
			return 0, false
		}
		if a == b {
			return 0, true
		}
		if !a {
			return -1, true
		}
		return 1, true
	case nil:
		if b == nil {
			return 0, true
		}
	}
	return 0, false
}

// KeyString devolve a representação de um valor usada como chave de
// agrupamento: strings sem aspas, o resto como JSON. Campo ausente vira "".
func KeyString(v any, present bool) string {
	if !present {
		return ""
	}
	switch v := v.(type) {
	case string:
		return v
	case json.Number:
		return string(v)
	case nil:
		return "null"
	case bool:
		return strconv.FormatBool(v)
	}
	b, err := json.Marshal(v)
	if err != nil {
		return ""
	}
	return string(b)
}
