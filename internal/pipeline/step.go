package pipeline

import (
	"runtime/debug"
	"sync"

	"github.com/FranciscoHonorat/hermes-pipe/internal/event"
)

// collector acumula o que um Stage emite numa chamada. O buffer é reusado
// entre chamadas e emit é criado uma vez só, para não alocar por evento.
type collector struct {
	buf  []event.Event
	emit func(event.Event)
}

func newCollector() *collector {
	c := &collector{}
	c.emit = func(e event.Event) { c.buf = append(c.buf, e) }
	return c
}

// step envolve um Stage com nome, recuperação de pânico e capacidades
// opcionais. É compartilhado pelo runner sequencial e pelo concorrente.
type step struct {
	stage Stage
	name  string
}

func newStep(s Stage) *step { return &step{stage: s, name: stageName(s)} }

func (s *step) concurrent() bool {
	c, ok := s.stage.(Concurrent)
	return ok && c.Concurrent()
}

func (s *step) idler() (Idler, bool) {
	i, ok := s.stage.(Idler)
	return i, ok
}

// process chama Process e devolve o que foi emitido (no buffer de col).
func (s *step) process(col *collector, e event.Event) (outs []event.Event, err error) {
	col.buf = col.buf[:0]
	defer s.recoverPanic(&err)
	err = s.stage.Process(e, col.emit)
	return col.buf, err
}

func (s *step) flush(col *collector) (outs []event.Event, err error) {
	col.buf = col.buf[:0]
	f, ok := s.stage.(Flusher)
	if !ok {
		return nil, nil
	}
	defer s.recoverPanic(&err)
	err = f.Flush(col.emit)
	return col.buf, err
}

func (s *step) idle(col *collector, i Idler) (outs []event.Event, err error) {
	col.buf = col.buf[:0]
	defer s.recoverPanic(&err)
	err = i.Idle(col.emit)
	return col.buf, err
}

func (s *step) recoverPanic(err *error) {
	if v := recover(); v != nil {
		*err = &PanicError{Stage: s.name, Value: v, Stack: debug.Stack()}
	}
}

// errorReporter serializa o ErrorSink e conta os erros.
type errorReporter struct {
	mu       sync.Mutex
	sink     ErrorSink
	counters *Counters
}

func (r *errorReporter) report(rec event.ErrorRecord) error {
	r.counters.errored.Add(1)
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.sink.Report(rec)
}

// parseRecord transforma um Record em Event. Se a linha é inválida, devolve
// o registro de rejeição no lugar.
func parseRecord(rec Record) (event.Event, *event.ErrorRecord) {
	if rec.Err != nil {
		return event.Event{}, &event.ErrorRecord{Seq: rec.Seq, Stage: "parse", Err: rec.Err}
	}
	e, err := event.Parse(rec.Line)
	if err != nil {
		raw := append([]byte(nil), rec.Line...)
		return event.Event{}, &event.ErrorRecord{Seq: rec.Seq, Stage: "parse", Err: err, Raw: raw}
	}
	e.Seq = rec.Seq
	return e, nil
}
