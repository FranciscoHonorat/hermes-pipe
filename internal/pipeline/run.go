package pipeline

import (
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"sync"
	"time"

	"github.com/FranciscoHonorat/hermes-pipe/internal/event"
)

// item é o que trafega nos channels: um evento ou um evento rejeitado.
//
// Os rejeitados andam junto com os eventos, na mesma ordem, em vez de irem
// direto ao ErrorSink. Assim a ordem do stderr é a ordem da entrada, e um
// stage que pede parada suave descarta também os erros de linhas que o
// producer leu adiantado: a saída inteira é determinística (I5).
type item struct {
	e   event.Event
	rej *event.ErrorRecord
}

// Run executa o pipeline concorrente: uma goroutine para o producer, uma (ou
// WithWorkers, se Concurrent) por stage, e o consumer na goroutine de quem
// chamou.
//
// Posse dos channels (I3): cada channel é criado aqui e fechado uma vez, só
// pela goroutine que envia nele. Quando Run retorna, todas as goroutines que
// ele criou terminaram (I2).
//
// Cancelamento:
//   - ctx cancelado (ou erro fatal): parada dura. Ninguém mais envia, nenhum
//     Flush roda, o que estava em trânsito se perde. Run devolve a causa.
//   - WithStop cancelado, ou um Stage devolve erro Stop(): parada suave. O
//     producer para de ler; stages e consumer drenam, Flush roda em ordem
//     (I9). Run devolve nil e Stats.Stopped tem a causa. O stage que pediu a
//     parada descarta (e conta como descartado) tudo o que chega depois.
func Run(ctx context.Context, p Producer, stages []Stage, c Consumer, opts ...Option) (st Stats, err error) {
	o := buildOptions(opts)
	start := time.Now()

	hard, hardCancel := context.WithCancelCause(ctx)
	defer hardCancel(nil)
	soft, softCancel := context.WithCancelCause(hard)
	defer softCancel(nil)
	if o.stop != nil {
		unregister := context.AfterFunc(o.stop, func() { softCancel(context.Cause(o.stop)) })
		defer unregister()
	}

	r := &run{
		o:          o,
		hard:       hard,
		hardCancel: hardCancel,
		softCancel: softCancel,
		rep:        &errorReporter{sink: o.errors, counters: o.counters},
	}

	var wg sync.WaitGroup
	src := make(chan item, o.buffer)
	wg.Go(func() { r.produce(soft, p, src) })
	// last é sempre o channel de saída do último elo montado. Cada closure
	// recebe o seu "in" como cópia: capturar last direto faria todos os stages
	// lerem do último channel (a variável é reatribuída no laço).
	var last <-chan item = src
	for _, s := range stages {
		in, out := last, make(chan item, o.buffer)
		stp := newStep(s)
		if stp.concurrent() && o.workers > 1 {
			wg.Go(func() { r.parallel(stp, in, out) })
		} else {
			wg.Go(func() { r.serial(stp, in, out) })
		}
		last = out
	}
	r.consume(last, c)
	wg.Wait()

	closeErr := errors.Join(c.Close(), o.errors.Close())
	st = o.counters.Snapshot()
	st.Elapsed = time.Since(start)
	if cause := context.Cause(hard); cause != nil {
		return st, errors.Join(cause, closeErr)
	}
	if cause := context.Cause(soft); cause != nil {
		st.Stopped = cause
	}
	return st, closeErr
}

type run struct {
	o          options
	hard       context.Context
	hardCancel context.CancelCauseFunc
	softCancel context.CancelCauseFunc
	rep        *errorReporter
}

func (r *run) fail(err error) { r.hardCancel(err) }

// send envia it, ou desiste se houve parada dura.
func (r *run) send(out chan<- item, it item) bool {
	select {
	case out <- it:
		return true
	case <-r.hard.Done():
		return false
	}
}

func (r *run) produce(soft context.Context, p Producer, out chan<- item) {
	defer close(out)
	for soft.Err() == nil {
		rec, err := p.Next(soft)
		if errors.Is(err, io.EOF) {
			return
		}
		if err != nil {
			if soft.Err() == nil {
				r.fail(fmt.Errorf("producer: %w", err))
			}
			return
		}
		r.o.counters.read.Add(1)
		e, rej := parseRecord(rec)
		if !r.send(out, item{e, rej}) {
			return
		}
	}
}

// stageState é o estado do runner para um stage (não do stage em si).
type stageState struct {
	s       *step
	stopped bool // o stage pediu parada suave: descarta o que vier depois
}

// apply aplica o resultado de uma chamada de Process e envia o que sai.
// Devolve false se o pipeline deve parar.
func (r *run) apply(st *stageState, in event.Event, outs []event.Event, err error, out chan<- item) bool {
	switch {
	case err == nil:
	case isStop(err):
		st.stopped = true
		r.softCancel(err)
	case isFatal(err):
		r.fail(err)
		return false
	default:
		return r.send(out, item{rej: &event.ErrorRecord{Seq: in.Seq, Stage: st.s.name, Err: err, Event: &in}})
	}
	r.o.counters.account(len(outs))
	for _, o := range outs {
		if !r.send(out, item{e: o}) {
			return false
		}
	}
	return true
}

// passThrough trata um item que não vai para Process: um rejeitado segue
// adiante; depois da parada suave do stage, tudo é descartado. Devolve
// handled == false se o item é um evento normal que deve ser processado.
func (r *run) passThrough(st *stageState, it item, out chan<- item) (handled, ok bool) {
	if st.stopped {
		r.o.counters.dropped.Add(1)
		return true, true
	}
	if it.rej != nil {
		return true, r.send(out, it)
	}
	return false, true
}

// finish roda o Flush (se não houve parada dura) e envia o resultado.
func (r *run) finish(s *step, col *collector, out chan<- item) {
	if r.hard.Err() != nil {
		return
	}
	outs, err := s.flush(col)
	if err != nil {
		r.fail(fmt.Errorf("flush %s: %w", s.name, err))
		return
	}
	r.o.counters.generated.Add(int64(len(outs)))
	for _, o := range outs {
		if !r.send(out, item{e: o}) {
			return
		}
	}
}

// serial é a goroutine de um stage com uma instância só.
func (r *run) serial(s *step, in <-chan item, out chan<- item) {
	defer close(out)
	st := &stageState{s: s}
	col := newCollector()
	idler, isIdler := s.idler()
	idleOn := isIdler && r.o.idle > 0
	for {
		var timer <-chan time.Time
		if idleOn {
			timer = r.o.clock.After(r.o.idle)
		}
		select {
		case it, ok := <-in:
			if !ok {
				r.finish(s, col, out)
				return
			}
			if r.hard.Err() != nil {
				return
			}
			if handled, ok := r.passThrough(st, it, out); handled {
				if !ok {
					return
				}
				continue
			}
			outs, err := s.process(col, it.e)
			if !r.apply(st, it.e, outs, err, out) {
				return
			}
		case <-timer:
			outs, err := s.idle(col, idler)
			if err != nil {
				r.fail(fmt.Errorf("idle %s: %w", s.name, err))
				return
			}
			r.o.counters.generated.Add(int64(len(outs)))
			for _, o := range outs {
				if !r.send(out, item{e: o}) {
					return
				}
			}
		case <-r.hard.Done():
			return
		}
	}
}

type result struct {
	it   item // o item de entrada
	done bool // it não precisa de Process (rejeitado): só repassar
	outs []event.Event
	err  error
}

// parallel roda um stage Concurrent em N workers e preserva a ordem (I7).
//
// O dispatcher reserva um lugar na fila order antes de entregar o trabalho;
// esta goroutine lê order em sequência e espera cada resultado. A capacidade
// de order limita quantos eventos podem estar fora de ordem: é o buffer de
// reordenação, e é ele que propaga a backpressure (I8).
func (r *run) parallel(s *step, in <-chan item, out chan<- item) {
	defer close(out)
	n := r.o.workers
	type job struct {
		it  item
		res chan<- result
	}
	jobs := make(chan job, n)
	order := make(chan chan result, 2*n)

	var wg sync.WaitGroup
	defer wg.Wait()
	wg.Go(func() {
		defer close(jobs)
		defer close(order)
		for it := range in {
			res := make(chan result, 1)
			select {
			case order <- res:
			case <-r.hard.Done():
				return
			}
			if it.rej != nil {
				res <- result{it: it, done: true} // nada a processar
				continue
			}
			select {
			case jobs <- job{it, res}:
			case <-r.hard.Done():
				return
			}
		}
	})
	for range n {
		wg.Go(func() {
			col := newCollector()
			for j := range jobs {
				outs, err := s.process(col, j.it.e)
				j.res <- result{it: j.it, outs: slices.Clone(outs), err: err} // buffer 1: nunca bloqueia
			}
		})
	}

	st := &stageState{s: s}
	for res := range order {
		var rr result
		select {
		case rr = <-res:
		case <-r.hard.Done():
			return
		}
		if handled, ok := r.passThrough(st, rr.it, out); handled {
			if !ok {
				return
			}
			continue
		}
		if !r.apply(st, rr.it.e, rr.outs, rr.err, out) {
			return
		}
	}
	r.finish(s, newCollector(), out)
}

func (r *run) consume(in <-chan item, c Consumer) {
	for it := range in {
		if r.hard.Err() != nil {
			return
		}
		if it.rej != nil {
			if err := r.rep.report(*it.rej); err != nil {
				r.fail(fmt.Errorf("errors: %w", err))
				return
			}
			continue
		}
		if err := c.Consume(it.e); err != nil {
			r.fail(fmt.Errorf("consumer: %w", err))
			return
		}
		r.o.counters.emitted.Add(1)
	}
}
