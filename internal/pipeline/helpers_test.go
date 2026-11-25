package pipeline_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/FranciscoHonorat/hermes-pipe/internal/event"
	"github.com/FranciscoHonorat/hermes-pipe/internal/pipeline"
)

// lines gera n linhas válidas, com uma inválida a cada badEvery (0 = nunca).
func lines(n, badEvery int) []string {
	out := make([]string, n)
	levels := []string{"info", "error", "warn"}
	for i := range n {
		if badEvery > 0 && (i+1)%badEvery == 0 {
			out[i] = `{"sem":"time"}`
			continue
		}
		ts := time.Date(2026, 1, 1, 0, 0, i, 0, time.UTC).Format(time.RFC3339)
		out[i] = fmt.Sprintf(`{"time":%q,"i":%d,"level":%q,"host":"h%d"}`, ts, i, levels[i%3], i%5)
	}
	return out
}

// sliceProducer entrega linhas de memória. Opcionalmente bloqueia depois de
// block linhas até que ctx seja cancelado (simula um stdin parado).
type sliceProducer struct {
	lines []string
	i     int
	reads atomic.Int64
}

func newProducer(ls []string) *sliceProducer { return &sliceProducer{lines: ls} }

func (p *sliceProducer) Next(ctx context.Context) (pipeline.Record, error) {
	if err := ctx.Err(); err != nil {
		return pipeline.Record{}, err
	}
	if p.i >= len(p.lines) {
		return pipeline.Record{}, io.EOF
	}
	p.i++
	p.reads.Add(1)
	return pipeline.Record{Seq: uint64(p.i), Line: []byte(p.lines[p.i-1])}, nil
}

// bufConsumer guarda a saída em JSON Lines.
type bufConsumer struct {
	mu     sync.Mutex
	buf    bytes.Buffer
	n      atomic.Int64
	closed atomic.Int32
	failAt int64 // >0: Consume devolve erro no n-ésimo evento
	block  chan struct{}
}

func (c *bufConsumer) Consume(e event.Event) error {
	if c.block != nil {
		<-c.block
	}
	n := c.n.Add(1)
	if c.failAt > 0 && n == c.failAt {
		return errors.New("disco cheio")
	}
	b, err := e.AppendJSON(nil)
	if err != nil {
		return err
	}
	c.mu.Lock()
	c.buf.Write(b)
	c.buf.WriteByte('\n')
	c.mu.Unlock()
	return nil
}

func (c *bufConsumer) Close() error {
	if c.closed.Add(1) > 1 {
		panic("Consumer.Close chamado duas vezes")
	}
	return nil
}

func (c *bufConsumer) String() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.buf.String()
}

type bufErrors struct {
	buf bytes.Buffer
}

func (s *bufErrors) Report(r event.ErrorRecord) error {
	b, err := r.AppendJSON(nil)
	if err != nil {
		return err
	}
	s.buf.Write(b)
	s.buf.WriteByte('\n')
	return nil
}
func (s *bufErrors) Close() error { return nil }

// ---- stages de teste ----

// dup emite cada evento duas vezes (fan-out: gera eventos).
type dup struct{}

func (dup) Process(e event.Event, emit func(event.Event)) error { emit(e); emit(e); return nil }

// counter conta e emite um evento de resumo no Flush.
type counter struct {
	n       int
	flushes atomic.Int32
}

func (c *counter) Process(e event.Event, emit func(event.Event)) error {
	c.n++
	emit(e)
	return nil
}

func (c *counter) Flush(emit func(event.Event)) error {
	c.flushes.Add(1)
	emit(event.New(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), map[string]any{"count": c.n}))
	return nil
}

// failEvery rejeita os eventos com i múltiplo de k.
type failEvery struct{ k int }

func (f failEvery) Process(e event.Event, emit func(event.Event)) error {
	if n := idx(e); n%f.k == 0 {
		return fmt.Errorf("i=%d rejeitado", n)
	}
	emit(e)
	return nil
}

func (failEvery) Concurrent() bool { return true }

// dropOdd descarta eventos com i ímpar. Concurrent.
type dropOdd struct{}

func (dropOdd) Process(e event.Event, emit func(event.Event)) error {
	if idx(e)%2 == 0 {
		emit(e)
	}
	return nil
}
func (dropOdd) Concurrent() bool { return true }

// stopAfter emite até n eventos e pede parada suave no n-ésimo.
type stopAfter struct{ n, seen int }

type stopErr struct{}

func (stopErr) Error() string { return "limite atingido" }
func (stopErr) Stop() bool    { return true }

func (s *stopAfter) Process(e event.Event, emit func(event.Event)) error {
	if s.seen >= s.n {
		return nil
	}
	s.seen++
	emit(e)
	if s.seen == s.n {
		return stopErr{}
	}
	return nil
}

type fatalErr struct{}

func (fatalErr) Error() string { return "fatal" }
func (fatalErr) Fatal() bool   { return true }

// failAt devolve erro fatal (ou pânico) no evento de Seq n.
type failAt struct {
	n     uint64
	panic bool
}

func (f failAt) Process(e event.Event, emit func(event.Event)) error {
	if e.Seq == f.n {
		if f.panic {
			panic("boom")
		}
		return fatalErr{}
	}
	emit(e)
	return nil
}

// cancelAt cancela o contexto ao ver o evento de Seq n (hard cancel externo).
type cancelAt struct {
	n      uint64
	cancel context.CancelFunc
}

func (c cancelAt) Process(e event.Event, emit func(event.Event)) error {
	if e.Seq == c.n {
		c.cancel()
	}
	emit(e)
	return nil
}

// idx devolve o campo "i" do evento, ou -1.
func idx(e event.Event) int {
	n, ok := e.Fields["i"].(json.Number)
	if !ok {
		return -1
	}
	i, err := strconv.Atoi(string(n))
	if err != nil {
		return -1
	}
	return i
}

func checkConservation(t *testing.T, st pipeline.Stats) {
	t.Helper()
	if !st.Conserved() {
		t.Fatalf("I1 violada: read(%d)+generated(%d) != emitted(%d)+dropped(%d)+errored(%d)",
			st.Read, st.Generated, st.Emitted, st.Dropped, st.Errored)
	}
}

var errEOF = io.EOF
