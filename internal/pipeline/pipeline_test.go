package pipeline_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/FranciscoHonorat/hermes-pipe/internal/event"
	"github.com/FranciscoHonorat/hermes-pipe/internal/pipeline"
	"github.com/FranciscoHonorat/hermes-pipe/internal/stage/filter"
	"github.com/FranciscoHonorat/hermes-pipe/internal/stage/mapper"
)

type runFunc func(context.Context, pipeline.Producer, []pipeline.Stage, pipeline.Consumer, ...pipeline.Option) (pipeline.Stats, error)

// scenario monta stages novos a cada chamada (stages com estado não podem
// ser reusados entre execuções).
type scenario struct {
	name   string
	stages func(t *testing.T) []pipeline.Stage
}

func mustFilter(t *testing.T, where ...string) pipeline.Stage {
	t.Helper()
	f, err := filter.New(filter.Config{Where: where})
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func mustMap(t *testing.T, ops ...string) pipeline.Stage {
	t.Helper()
	m, err := mapper.New(mapper.Config{Ops: ops})
	if err != nil {
		t.Fatal(err)
	}
	return m
}

var scenarios = []scenario{
	{"vazio", func(*testing.T) []pipeline.Stage { return nil }},
	{"filter", func(t *testing.T) []pipeline.Stage { return []pipeline.Stage{mustFilter(t, "level==error")} }},
	{"map+filter", func(t *testing.T) []pipeline.Stage {
		return []pipeline.Stage{mustMap(t, "rename:host=h", "set:x=1"), mustFilter(t, "level!=info")}
	}},
	{"drop+dup", func(*testing.T) []pipeline.Stage { return []pipeline.Stage{dropOdd{}, dup{}} }},
	{"erros+flush", func(*testing.T) []pipeline.Stage { return []pipeline.Stage{failEvery{k: 3}, &counter{}} }},
	{"limit", func(*testing.T) []pipeline.Stage { return []pipeline.Stage{&stopAfter{n: 7}} }},
	{"dup+limit+flush", func(*testing.T) []pipeline.Stage {
		return []pipeline.Stage{dup{}, &stopAfter{n: 5}, &counter{}}
	}},
	{"paralelo+estado", func(t *testing.T) []pipeline.Stage {
		return []pipeline.Stage{mustFilter(t, "level!=warn"), dropOdd{}, &counter{}, mustMap(t, "delete:host")}
	}},
}

type outcome struct {
	out    string
	errs   []string
	stats  pipeline.Stats
	runErr error
}

func execute(t *testing.T, run runFunc, sc scenario, in []string, opts ...pipeline.Option) outcome {
	t.Helper()
	c := &bufConsumer{}
	es := &bufErrors{}
	st, err := run(context.Background(), newProducer(in), sc.stages(t), c, append(opts, pipeline.WithErrors(es))...)
	errs := strings.Split(strings.TrimSpace(es.buf.String()), "\n")
	return outcome{c.String(), errs, st, err}
}

// TestPipeline_DifferentialVsSequential: Run produz exatamente a saída de
// RunSequential, para todo cenário, buffer e número de workers (I5, I7).
func TestPipeline_DifferentialVsSequential(t *testing.T) {
	in := lines(200, 17)
	for _, sc := range scenarios {
		want := execute(t, pipeline.RunSequential, sc, in)
		if want.runErr != nil {
			t.Fatalf("%s: sequencial falhou: %v", sc.name, want.runErr)
		}
		checkConservation(t, want.stats)
		for _, buf := range []int{0, 1, 16, 1024} {
			for _, workers := range []int{1, 2, 4, 8} {
				t.Run(fmt.Sprintf("%s/buf=%d/w=%d", sc.name, buf, workers), func(t *testing.T) {
					got := execute(t, pipeline.Run, sc, in, pipeline.WithBuffer(buf), pipeline.WithWorkers(workers))
					if got.runErr != nil {
						t.Fatal(got.runErr)
					}
					if got.out != want.out {
						t.Fatalf("saída difere do oráculo\n--- got\n%s\n--- want\n%s", got.out, want.out)
					}
					if !slices.Equal(got.errs, want.errs) {
						t.Fatalf("erros diferem\n--- got\n%v\n--- want\n%v", got.errs, want.errs)
					}
					checkConservation(t, got.stats)
					if want.stats.Stopped == nil && got.stats != withElapsed(want.stats, got.stats.Elapsed) {
						t.Fatalf("stats diferem: got %+v want %+v", got.stats, want.stats)
					}
				})
			}
		}
	}
}

func withElapsed(s pipeline.Stats, d time.Duration) pipeline.Stats { s.Elapsed = d; return s }

func TestPipeline_Conservation(t *testing.T) {
	for _, run := range []struct {
		name string
		f    runFunc
	}{{"Run", pipeline.Run}, {"RunSequential", pipeline.RunSequential}} {
		for _, sc := range scenarios {
			t.Run(run.name+"/"+sc.name, func(t *testing.T) {
				o := execute(t, run.f, sc, lines(100, 7))
				checkConservation(t, o.stats)
				if o.stats.Read != 100 && o.stats.Stopped == nil {
					t.Fatalf("read = %d", o.stats.Read)
				}
			})
		}
	}
}

// TestPipeline_NoLeak: quando Run retorna, nenhuma goroutine criada por ele
// vive (I2). synctest.Test falha se a bolha terminar com goroutines presas.
func TestPipeline_NoLeak(t *testing.T) {
	cases := []struct {
		name   string
		stages func(cancel context.CancelFunc) []pipeline.Stage
		failAt int64
	}{
		{"eof", func(context.CancelFunc) []pipeline.Stage { return []pipeline.Stage{dup{}, &counter{}} }, 0},
		{"soft", func(context.CancelFunc) []pipeline.Stage { return []pipeline.Stage{&stopAfter{n: 3}} }, 0},
		{"hard", func(c context.CancelFunc) []pipeline.Stage { return []pipeline.Stage{cancelAt{n: 5, cancel: c}, dup{}} }, 0},
		{"fatal", func(context.CancelFunc) []pipeline.Stage { return []pipeline.Stage{dup{}, failAt{n: 9}} }, 0},
		{"panic", func(context.CancelFunc) []pipeline.Stage { return []pipeline.Stage{failAt{n: 9, panic: true}} }, 0},
		{"consumer", func(context.CancelFunc) []pipeline.Stage { return []pipeline.Stage{dup{}} }, 4},
	}
	for _, tc := range cases {
		for _, workers := range []int{1, 4} {
			t.Run(fmt.Sprintf("%s/w=%d", tc.name, workers), func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					ctx, cancel := context.WithCancel(t.Context())
					defer cancel()
					c := &bufConsumer{failAt: tc.failAt}
					_, _ = pipeline.Run(ctx, newProducer(lines(500, 0)), tc.stages(cancel), c,
						pipeline.WithBuffer(0), pipeline.WithWorkers(workers))
					synctest.Wait()
				})
			})
		}
	}
}

// TestPipeline_CancelAtEveryStep: hard cancel em cada evento. Run devolve o
// cancelamento, o Close roda uma vez, Flush nunca roda e nada vaza.
func TestPipeline_CancelAtEveryStep(t *testing.T) {
	const n = 30
	for k := uint64(1); k <= n; k++ {
		for _, buf := range []int{0, 4} {
			t.Run(fmt.Sprintf("k=%d/buf=%d", k, buf), func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					ctx, cancel := context.WithCancel(t.Context())
					defer cancel()
					cnt := &counter{}
					c := &bufConsumer{}
					_, err := pipeline.Run(ctx, newProducer(lines(n, 0)),
						[]pipeline.Stage{dup{}, cancelAt{n: k, cancel: cancel}, cnt}, c, pipeline.WithBuffer(buf))
					if !errors.Is(err, context.Canceled) {
						t.Fatalf("err = %v", err)
					}
					if cnt.flushes.Load() != 0 {
						t.Fatal("Flush rodou depois de hard cancel")
					}
					if c.closed.Load() != 1 {
						t.Fatalf("Close chamado %d vezes", c.closed.Load())
					}
				})
			})
		}
	}
}

// TestPipeline_CloseOnce: fechar um channel duas vezes (ou do lado errado)
// entra em pânico. Rodamos todos os caminhos de saída com -race; qualquer
// close duplicado derruba o binário de teste.
func TestPipeline_CloseOnce(t *testing.T) {
	for _, workers := range []int{1, 3} {
		for k := uint64(1); k <= 20; k += 3 {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				_, _ = pipeline.Run(ctx, newProducer(lines(20, 4)),
					[]pipeline.Stage{dropOdd{}, failAt{n: k}, dup{}, cancelAt{n: k + 1, cancel: cancel}}, &bufConsumer{},
					pipeline.WithWorkers(workers), pipeline.WithBuffer(1))
			})
		}
	}
}

func TestPipeline_FlushOnEOF(t *testing.T) {
	for _, run := range []runFunc{pipeline.Run, pipeline.RunSequential} {
		cnt := &counter{}
		c := &bufConsumer{}
		st, err := run(context.Background(), newProducer(lines(10, 0)), []pipeline.Stage{cnt}, c)
		if err != nil {
			t.Fatal(err)
		}
		if cnt.flushes.Load() != 1 {
			t.Fatalf("flushes = %d", cnt.flushes.Load())
		}
		if !strings.HasSuffix(c.String(), `{"count":10,"time":"2026-01-01T00:00:00Z"}`+"\n") {
			t.Fatalf("saída sem o resumo do Flush:\n%s", c.String())
		}
		checkConservation(t, st)
	}
}

// TestPipeline_ShutdownGraceful: parada suave (Ctrl+C) no meio do stream.
// O producer para, o que estava em trânsito é drenado na ordem producer →
// stages → consumer, o Flush roda e I1 vale (I9).
func TestPipeline_ShutdownGraceful(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		stop, interrupt := context.WithCancelCause(t.Context())
		cause := errors.New("SIGINT")
		p := &gate{lines: lines(1000, 0), after: 50, interrupt: func() { interrupt(cause) }}
		cnt := &counter{}
		c := &bufConsumer{}
		st, err := pipeline.Run(t.Context(), p, []pipeline.Stage{dropOdd{}, cnt}, c,
			pipeline.WithStop(stop), pipeline.WithBuffer(8))
		if err != nil {
			t.Fatal(err)
		}
		if !errors.Is(st.Stopped, cause) {
			t.Fatalf("Stopped = %v", st.Stopped)
		}
		if cnt.flushes.Load() != 1 {
			t.Fatal("Flush não rodou na parada suave")
		}
		if st.Read != 50 {
			t.Fatalf("read = %d, quer 50", st.Read)
		}
		checkConservation(t, st)
		out := strings.Split(strings.TrimSpace(c.String()), "\n")
		if last := out[len(out)-1]; !strings.Contains(last, `"count":25`) {
			t.Fatalf("última linha = %s", last)
		}
	})
}

// gate é um producer que, depois de after linhas, dispara interrupt e passa a
// bloquear como um stdin parado, até o ctx ser cancelado.
type gate struct {
	lines     []string
	after     int
	i         int
	interrupt func()
}

func (g *gate) Next(ctx context.Context) (pipeline.Record, error) {
	if g.i == g.after {
		g.interrupt()
		<-ctx.Done()
		return pipeline.Record{}, context.Cause(ctx)
	}
	g.i++
	return pipeline.Record{Seq: uint64(g.i), Line: []byte(g.lines[g.i-1])}, nil
}

func TestPipeline_StopFromStage(t *testing.T) {
	st, err := pipeline.Run(context.Background(), newProducer(lines(10_000, 0)),
		[]pipeline.Stage{&stopAfter{n: 3}}, &bufConsumer{}, pipeline.WithBuffer(0))
	if err != nil {
		t.Fatal(err)
	}
	if st.Emitted != 3 {
		t.Fatalf("emitted = %d", st.Emitted)
	}
	if st.Read > 10 {
		t.Fatalf("o producer não parou cedo: read = %d", st.Read)
	}
	var s interface{ Stop() bool }
	if !errors.As(st.Stopped, &s) {
		t.Fatalf("Stopped = %v", st.Stopped)
	}
	checkConservation(t, st)
}

func TestPipeline_FatalAndPanic(t *testing.T) {
	for _, run := range []runFunc{pipeline.Run, pipeline.RunSequential} {
		_, err := run(context.Background(), newProducer(lines(50, 0)), []pipeline.Stage{failAt{n: 10}}, &bufConsumer{})
		if !errors.As(err, new(fatalErr)) {
			t.Fatalf("fatal: err = %v", err)
		}
		_, err = run(context.Background(), newProducer(lines(50, 0)), []pipeline.Stage{failAt{n: 10, panic: true}}, &bufConsumer{})
		var pe *pipeline.PanicError
		if !errors.As(err, &pe) || pe.Value != "boom" || len(pe.Stack) == 0 {
			t.Fatalf("panic: err = %v", err)
		}
	}
}

func TestPipeline_ConsumerError(t *testing.T) {
	for _, run := range []runFunc{pipeline.Run, pipeline.RunSequential} {
		c := &bufConsumer{failAt: 3}
		_, err := run(context.Background(), newProducer(lines(50, 0)), nil, c)
		if err == nil || !strings.Contains(err.Error(), "disco cheio") {
			t.Fatalf("err = %v", err)
		}
		if c.closed.Load() != 1 {
			t.Fatal("Close não foi chamado uma vez")
		}
	}
}

// TestPipeline_Backpressure: com o consumer parado, o producer lê no máximo
// o que cabe nos channels e nas mãos das goroutines (I8). Medido por
// contadores, não por tempo.
func TestPipeline_Backpressure(t *testing.T) {
	for _, tc := range []struct {
		buf, workers int
		stages       int
	}{{0, 1, 2}, {4, 1, 2}, {16, 1, 3}, {4, 4, 2}} {
		t.Run(fmt.Sprintf("buf=%d/w=%d/s=%d", tc.buf, tc.workers, tc.stages), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				p := newProducer(lines(100_000, 0))
				c := &bufConsumer{block: make(chan struct{})}
				var stages []pipeline.Stage
				for range tc.stages {
					stages = append(stages, mustFilter(t, "i>=0"))
				}
				done := make(chan struct{})
				go func() {
					defer close(done)
					st, err := pipeline.Run(t.Context(), p, stages, c,
						pipeline.WithBuffer(tc.buf), pipeline.WithWorkers(tc.workers))
					if err != nil || st.Emitted != 100_000 {
						t.Errorf("st=%+v err=%v", st, err)
					}
				}()
				synctest.Wait() // todo mundo bloqueado: o consumer não anda
				// channels entre goroutines + um evento na mão de cada uma
				// (producer, stages, consumer); no modo paralelo, cada stage
				// ainda tem o buffer de reordenação (2n) e o dispatcher.
				bound := (tc.stages+1)*tc.buf + tc.stages + 2
				if tc.workers > 1 {
					bound += tc.stages * (2*tc.workers + 1)
				}
				if got := p.reads.Load(); got > int64(bound) || got == 0 {
					t.Fatalf("producer leu %d com o consumer parado; limite %d", got, bound)
				}
				close(c.block)
				<-done
			})
		})
	}
}

// idler emite um marcador quando a entrada fica parada.
type idler struct{ calls int }

func (*idler) Process(e event.Event, emit func(event.Event)) error { emit(e); return nil }
func (i *idler) Idle(emit func(event.Event)) error {
	i.calls++
	emit(event.New(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), map[string]any{"idle": i.calls}))
	return nil
}

// stalled entrega n linhas e depois espera release (ou ctx).
type stalled struct {
	lines   []string
	i       int
	release chan struct{}
}

func (s *stalled) Next(ctx context.Context) (pipeline.Record, error) {
	if s.i == len(s.lines) {
		select {
		case <-s.release:
			return pipeline.Record{}, errEOF
		case <-ctx.Done():
			return pipeline.Record{}, ctx.Err()
		}
	}
	s.i++
	return pipeline.Record{Seq: uint64(s.i), Line: []byte(s.lines[s.i-1])}, nil
}

// TestRunner_IdleFlush simula uma hora de silêncio sem esperar uma hora.
func TestRunner_IdleFlush(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := &stalled{lines: lines(3, 0), release: make(chan struct{})}
		c := &bufConsumer{}
		id := &idler{}
		done := make(chan pipeline.Stats)
		go func() {
			st, err := pipeline.Run(t.Context(), p, []pipeline.Stage{id}, c, pipeline.WithIdleFlush(time.Minute))
			if err != nil {
				t.Error(err)
			}
			done <- st
		}()
		synctest.Wait()
		if c.n.Load() != 3 {
			t.Fatalf("emitidos antes do silêncio = %d", c.n.Load())
		}
		time.Sleep(time.Hour)
		synctest.Wait()
		if got := c.n.Load(); got != 3+60 {
			t.Fatalf("depois de 1h parado: %d eventos, quer 63", got)
		}
		close(p.release)
		st := <-done
		checkConservation(t, st)
	})
}

func TestRunner_IdleFlushOffByDefault(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := &stalled{lines: lines(3, 0), release: make(chan struct{})}
		c := &bufConsumer{}
		go func() {
			time.Sleep(time.Hour)
			close(p.release)
		}()
		if _, err := pipeline.Run(t.Context(), p, []pipeline.Stage{&idler{}}, c); err != nil {
			t.Fatal(err)
		}
		if c.n.Load() != 3 {
			t.Fatalf("idle-flush rodou sem ser pedido: %d eventos", c.n.Load())
		}
	})
}

// TestClockContract: After(d) dispara depois de exatamente d.
func TestClockContract(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := pipeline.RealClock()
		start := time.Now()
		<-c.After(90 * time.Second)
		if got := time.Since(start); got != 90*time.Second {
			t.Fatalf("After(90s) disparou em %v", got)
		}
	})
}
