// Package stagetest tem o teste de contrato que todo Stage precisa passar.
package stagetest

import (
	"bytes"
	"errors"
	"testing"

	"github.com/FranciscoHonorat/hermes-pipe/internal/event"
	"github.com/FranciscoHonorat/hermes-pipe/internal/stage"
)

// Inputs é um conjunto de eventos variados para o contrato.
func Inputs(t testing.TB) []event.Event {
	t.Helper()
	lines := []string{
		`{"time":"2026-01-01T00:00:00Z","level":"error","host":"a","msg":"x","n":1}`,
		`{"time":"2026-01-01T00:00:01Z","level":"info","host":"b","message":"y","n":2.5}`,
		`{"time":"2026-01-01T00:00:02Z","level":"error","host":"a","http":{"status":500},"tags":["p","q"]}`,
		`{"time":"2026-01-01T00:00:03Z"}`,
		`{"time":"2026-01-01T00:01:00Z","level":"warn","host":"c","n":"7","user":null}`,
	}
	out := make([]event.Event, 0, len(lines))
	for i, l := range lines {
		e, err := event.Parse([]byte(l))
		if err != nil {
			t.Fatalf("stagetest: entrada %d inválida: %v", i, err)
		}
		e.Seq = uint64(i + 1)
		out = append(out, e)
	}
	return out
}

func snapshot(t testing.TB, e event.Event) []byte {
	t.Helper()
	b, err := e.AppendJSON(nil)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// Run verifica o contrato de Stage com um construtor que devolve uma
// instância nova a cada chamada:
//
//   - não modifica o Event recebido (I4);
//   - todo Event emitido é válido;
//   - é determinístico: duas instâncias com a mesma entrada emitem o mesmo;
//   - erro Fatal ou Stop não é usado para erro de evento comum;
//   - Flush (se houver) emite eventos válidos e é determinístico.
func Run(t *testing.T, newStage func() stage.Stage) {
	t.Helper()
	t.Run("NoSideEffects", func(t *testing.T) {
		s := newStage()
		for _, e := range Inputs(t) {
			before := snapshot(t, e)
			_ = s.Process(e, func(event.Event) {})
			if after := snapshot(t, e); !bytes.Equal(before, after) {
				t.Fatalf("Process modificou a entrada:\nantes  %s\ndepois %s", before, after)
			}
		}
	})
	t.Run("ValidOutputs", func(t *testing.T) {
		s := newStage()
		check := func(out event.Event) {
			if err := out.Validate(); err != nil {
				t.Errorf("emitiu evento inválido: %v", err)
			}
		}
		for _, e := range Inputs(t) {
			_ = s.Process(e, check)
		}
		if f, ok := s.(stage.Flusher); ok {
			if err := f.Flush(check); err != nil {
				t.Errorf("Flush: %v", err)
			}
		}
	})
	t.Run("Deterministic", func(t *testing.T) {
		a, b := collect(t, newStage()), collect(t, newStage())
		if !bytes.Equal(a, b) {
			t.Fatalf("duas execuções diferentes:\n%s\n---\n%s", a, b)
		}
	})
	t.Run("ErrorKinds", func(t *testing.T) {
		s := newStage()
		for _, e := range Inputs(t) {
			err := s.Process(e, func(event.Event) {})
			var f interface{ Fatal() bool }
			if errors.As(err, &f) {
				t.Errorf("erro fatal com entrada válida: %v", err)
			}
		}
	})
}

func collect(t *testing.T, s stage.Stage) []byte {
	var buf bytes.Buffer
	emit := func(out event.Event) {
		buf.Write(snapshot(t, out))
		buf.WriteByte('\n')
	}
	for _, e := range Inputs(t) {
		if err := s.Process(e, emit); err != nil {
			buf.WriteString("err: " + err.Error() + "\n")
		}
	}
	if f, ok := s.(stage.Flusher); ok {
		if err := f.Flush(emit); err != nil {
			buf.WriteString("flush err: " + err.Error() + "\n")
		}
	}
	return buf.Bytes()
}
