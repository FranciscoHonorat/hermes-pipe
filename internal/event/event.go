// Package event define o Event do hermes-pipe e o parser de JSON Lines.
//
// Este pacote só importa a stdlib e não conhece nenhum outro pacote do módulo.
package event

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"
	"unicode/utf8"
)

// TimeField é o nome do campo obrigatório com o instante do evento.
const TimeField = "time"

// Erros de parse. Todos são erros de evento: a linha é rejeitada e o
// processamento segue.
var (
	ErrEmptyLine    = errors.New("linha vazia")
	ErrInvalidUTF8  = errors.New("UTF-8 inválido")
	ErrInvalidJSON  = errors.New("JSON inválido")
	ErrNotObject    = errors.New("não é um objeto JSON")
	ErrTrailingData = errors.New("dados depois do objeto JSON")
	ErrMissingTime  = errors.New(`campo "time" ausente`)
	ErrInvalidTime  = errors.New(`campo "time" inválido`)
	ErrLineTooLong  = errors.New("linha acima do tamanho máximo")
)

// Event é um evento JSON validado.
//
// Invariantes de todo Event devolvido por Parse ou construído por um Stage:
//   - Time nunca é o valor zero: sem timestamp é erro, não zero.
//   - Fields nunca é nil e Fields["time"] contém o instante em RFC 3339.
//
// Os valores de Fields seguem encoding/json com UseNumber: string, json.Number,
// bool, nil, []any e map[string]any.
//
// Event é passado por valor, mas Fields é um map: copiar um Event não copia os
// campos. Quem recebe um Event nunca o modifica; para alterar, use Clone.
type Event struct {
	// Seq é o número da linha de origem (a partir de 1). Eventos gerados por
	// um Stage no Flush têm Seq 0.
	Seq    uint64
	Time   time.Time
	Fields map[string]any
}

// Parse lê uma linha de JSON Lines e devolve o Event correspondente.
// Um "\r" final (de "\r\n") é ignorado. A linha não é retida pelo Event.
func Parse(line []byte) (Event, error) {
	line = bytes.TrimSuffix(line, []byte("\r"))
	if len(bytes.TrimSpace(line)) == 0 {
		return Event{}, ErrEmptyLine
	}
	if !utf8.Valid(line) {
		return Event{}, ErrInvalidUTF8
	}

	dec := json.NewDecoder(bytes.NewReader(line))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return Event{}, fmt.Errorf("%w: %v", ErrInvalidJSON, err) //nolint:errorlint // o erro do json é só texto aqui
	}
	fields, ok := v.(map[string]any)
	if !ok {
		return Event{}, ErrNotObject
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return Event{}, ErrTrailingData
	}

	t, err := parseTime(fields)
	if err != nil {
		return Event{}, err
	}
	return Event{Time: t, Fields: fields}, nil
}

func parseTime(fields map[string]any) (time.Time, error) {
	raw, ok := fields[TimeField]
	if !ok || raw == nil {
		return time.Time{}, ErrMissingTime
	}
	s, ok := raw.(string)
	if !ok {
		return time.Time{}, fmt.Errorf("%w: precisa ser string RFC 3339, veio %T", ErrInvalidTime, raw)
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return time.Time{}, fmt.Errorf("%w: %q", ErrInvalidTime, s)
	}
	if t.IsZero() {
		return time.Time{}, fmt.Errorf("%w: instante zero", ErrInvalidTime)
	}
	return t, nil
}

// Validate confere as invariantes do Event.
func (e Event) Validate() error {
	if e.Time.IsZero() {
		return ErrMissingTime
	}
	if e.Fields == nil {
		return errors.New("campo Fields nil")
	}
	if _, ok := e.Fields[TimeField]; !ok {
		return ErrMissingTime
	}
	return nil
}

// New cria um Event gerado (Seq 0) com o instante t e os campos dados.
// O campo "time" é sempre escrito a partir de t.
func New(t time.Time, fields map[string]any) Event {
	if fields == nil {
		fields = make(map[string]any, 1)
	}
	fields[TimeField] = t.UTC().Format(time.RFC3339Nano)
	return Event{Time: t, Fields: fields}
}

// Clone devolve uma cópia profunda do Event.
func (e Event) Clone() Event {
	e.Fields = cloneMap(e.Fields)
	return e
}

func cloneMap(m map[string]any) map[string]any {
	if m == nil {
		return nil
	}
	out := make(map[string]any, len(m))
	for k, v := range m {
		out[k] = cloneValue(v)
	}
	return out
}

func cloneValue(v any) any {
	switch v := v.(type) {
	case map[string]any:
		return cloneMap(v)
	case []any:
		out := make([]any, len(v))
		for i, x := range v {
			out[i] = cloneValue(x)
		}
		return out
	default:
		return v
	}
}

// Get devolve o valor no caminho dado. Caminhos com ponto ("http.status")
// descem em objetos aninhados. Um campo cujo nome contém ponto tem prioridade.
func (e Event) Get(path string) (any, bool) {
	if v, ok := e.Fields[path]; ok {
		return v, true
	}
	var cur any = e.Fields
	for part := range strings.SplitSeq(path, ".") {
		m, ok := cur.(map[string]any)
		if !ok {
			return nil, false
		}
		cur, ok = m[part]
		if !ok {
			return nil, false
		}
	}
	return cur, true
}

// AppendJSON acrescenta o Event em JSON (uma linha, sem "\n") a dst.
// As chaves saem ordenadas, então a saída é determinística.
func (e Event) AppendJSON(dst []byte) ([]byte, error) {
	b, err := json.Marshal(e.Fields)
	if err != nil {
		return dst, err
	}
	return append(dst, b...), nil
}
