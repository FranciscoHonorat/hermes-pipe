package pipeline

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/FranciscoHonorat/hermes-pipe/internal/event"
)

// RunSequential processa o stream num laço, sem goroutines. É o oráculo dos
// testes diferenciais: Run precisa produzir a mesma saída, byte a byte.
//
// Ignora WithBuffer, WithWorkers e WithIdleFlush. ctx cancelado é parada dura;
// WithStop é parada suave, verificada entre linhas.
func RunSequential(ctx context.Context, p Producer, stages []Stage, c Consumer, opts ...Option) (st Stats, err error) {
	o := buildOptions(opts)
	start := time.Now()
	s := &seqRun{
		ctx:      ctx,
		counters: o.counters,
		rep:      &errorReporter{sink: o.errors, counters: o.counters},
		c:        c,
	}
	for _, stg := range stages {
		s.steps = append(s.steps, newStep(stg))
		s.cols = append(s.cols, newCollector())
	}

	defer func() {
		err = errors.Join(err, c.Close(), o.errors.Close())
		st = o.counters.Snapshot()
		st.Stopped = s.stopped
		st.Elapsed = time.Since(start)
	}()

	for s.stopped == nil {
		if err := context.Cause(ctx); err != nil {
			return st, err
		}
		if o.stop != nil && o.stop.Err() != nil {
			s.stopped = context.Cause(o.stop)
			break
		}
		rec, err := p.Next(ctx)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			if cerr := context.Cause(ctx); cerr != nil {
				return st, cerr
			}
			return st, fmt.Errorf("producer: %w", err)
		}
		o.counters.read.Add(1)
		e, rej := parseRecord(rec)
		if rej != nil {
			if err := s.rep.report(*rej); err != nil {
				return st, fmt.Errorf("errors: %w", err)
			}
			continue
		}
		if err := s.push(0, e); err != nil {
			return st, err
		}
	}

	for i := range s.steps {
		outs, err := s.steps[i].flush(s.cols[i])
		if err != nil {
			return st, fmt.Errorf("flush %s: %w", s.steps[i].name, err)
		}
		o.counters.generated.Add(int64(len(outs)))
		if err := s.pushAll(i+1, outs); err != nil {
			return st, err
		}
	}
	return st, nil
}

type seqRun struct {
	ctx      context.Context
	steps    []*step
	cols     []*collector
	counters *Counters
	rep      *errorReporter
	c        Consumer
	stopped  error
}

// push passa e pelo stage i e, recursivamente, pelos seguintes.
func (s *seqRun) push(i int, e event.Event) error {
	if i == len(s.steps) {
		if err := s.c.Consume(e); err != nil {
			return fmt.Errorf("consumer: %w", err)
		}
		s.counters.emitted.Add(1)
		return nil
	}
	st := s.steps[i]
	outs, err := st.process(s.cols[i], e)
	switch {
	case err == nil:
	case isStop(err):
		if s.stopped == nil {
			s.stopped = err
		}
	case isFatal(err):
		return err
	default:
		return s.rep.report(event.ErrorRecord{Seq: e.Seq, Stage: st.name, Err: err, Event: &e})
	}
	s.counters.account(len(outs))
	return s.pushAll(i+1, outs)
}

// pushAll empurra outs para o stage i. outs pode ser o buffer do collector do
// stage anterior, que só é reusado na próxima chamada daquele stage; a
// recursão termina antes disso, então não há cópia.
func (s *seqRun) pushAll(i int, outs []event.Event) error {
	for _, o := range outs {
		if err := s.push(i, o); err != nil {
			return err
		}
	}
	return nil
}
