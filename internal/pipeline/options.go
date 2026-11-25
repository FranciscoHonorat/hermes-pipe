package pipeline

import (
	"context"
	"time"
)

// DefaultBuffer é a capacidade padrão de cada channel entre goroutines.
// Escolhida por medição (ADR 0006, PERFORMANCE.md).
const DefaultBuffer = 64

// Clock é o relógio de parede do runner. Só o runner tem relógio: os stages
// trabalham com o tempo do evento.
type Clock interface {
	After(d time.Duration) <-chan time.Time
}

type realClock struct{}

func (realClock) After(d time.Duration) <-chan time.Time { return time.After(d) }

// RealClock devolve o relógio do sistema.
func RealClock() Clock { return realClock{} }

type options struct {
	buffer   int
	workers  int
	errors   ErrorSink
	stop     context.Context
	counters *Counters
	clock    Clock
	idle     time.Duration
}

// Option configura Run e RunSequential.
type Option func(*options)

// WithBuffer define a capacidade dos channels (>= 0).
func WithBuffer(n int) Option { return func(o *options) { o.buffer = max(n, 0) } }

// WithWorkers define quantas goroutines processam cada stage Concurrent.
func WithWorkers(n int) Option { return func(o *options) { o.workers = max(n, 1) } }

// WithErrors define para onde vão os eventos rejeitados.
func WithErrors(s ErrorSink) Option { return func(o *options) { o.errors = s } }

// WithStop liga uma parada suave: quando ctx é cancelado, o producer para de
// ler e o que está em trânsito é drenado e flushado. O ctx de Run, quando
// cancelado, é a parada dura: tudo para sem flush.
func WithStop(ctx context.Context) Option { return func(o *options) { o.stop = ctx } }

// WithCounters usa c como contadores, para que o chamador os leia ao vivo.
func WithCounters(c *Counters) Option { return func(o *options) { o.counters = c } }

// WithClock injeta o relógio (para testes).
func WithClock(c Clock) Option { return func(o *options) { o.clock = c } }

// WithIdleFlush chama Idle nos stages Idler quando a entrada deles fica parada
// por d. Zero desliga (o padrão, e o certo para replay determinístico).
func WithIdleFlush(d time.Duration) Option { return func(o *options) { o.idle = d } }

func buildOptions(opts []Option) options {
	o := options{buffer: DefaultBuffer, workers: 1, errors: discardErrors{}, clock: realClock{}}
	for _, opt := range opts {
		opt(&o)
	}
	if o.counters == nil {
		o.counters = &Counters{}
	}
	return o
}
