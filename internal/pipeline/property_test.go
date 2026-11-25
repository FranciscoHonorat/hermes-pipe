package pipeline_test

import (
	"context"
	"math/rand/v2"
	"testing"
	"testing/synctest"
	"time"

	"github.com/FranciscoHonorat/hermes-pipe/internal/pipeline"
)

// genCase gera, a partir de uma seed, entrada, stages e opções. A seed sai
// em toda falha para reproduzir.
type genCase struct {
	in      []string
	stages  func(cancel context.CancelFunc) []pipeline.Stage
	buf     int
	workers int
	cancel  uint64 // 0 = sem hard cancel
}

func generate(seed uint64) genCase {
	r := rand.New(rand.NewPCG(seed, 0xBEEF))
	n := r.IntN(300)
	bad := 0
	if r.IntN(2) == 0 {
		bad = 2 + r.IntN(10)
	}
	kinds := r.Perm(5)[:1+r.IntN(4)]
	stopN := 1 + r.IntN(50)
	c := genCase{
		in:      lines(n, bad),
		buf:     []int{0, 1, 3, 64}[r.IntN(4)],
		workers: 1 + r.IntN(6),
	}
	if r.IntN(3) == 0 && n > 0 {
		c.cancel = uint64(1 + r.IntN(n))
	}
	c.stages = func(cancel context.CancelFunc) []pipeline.Stage {
		var ss []pipeline.Stage
		for _, k := range kinds {
			switch k {
			case 0:
				ss = append(ss, dropOdd{})
			case 1:
				ss = append(ss, dup{})
			case 2:
				ss = append(ss, failEvery{k: 4})
			case 3:
				ss = append(ss, &counter{})
			case 4:
				ss = append(ss, &stopAfter{n: stopN})
			}
		}
		if cancel != nil {
			ss = append(ss, cancelAt{n: c.cancel, cancel: cancel})
		}
		return ss
	}
	return c
}

// TestProperty_Conservation: para entradas e pipelines gerados, sem hard
// cancel, I1 vale e Run é igual a RunSequential.
func TestProperty_Conservation(t *testing.T) {
	base := uint64(time.Now().UnixNano())
	for i := range uint64(200) {
		seed := base + i
		g := generate(seed)
		ctx := context.Background()
		seqC, runC := &bufConsumer{}, &bufConsumer{}
		seqE, runE := &bufErrors{}, &bufErrors{}
		seqSt, err1 := pipeline.RunSequential(ctx, newProducer(g.in), g.stages(nil), seqC, pipeline.WithErrors(seqE))
		runSt, err2 := pipeline.Run(ctx, newProducer(g.in), g.stages(nil), runC, pipeline.WithErrors(runE),
			pipeline.WithBuffer(g.buf), pipeline.WithWorkers(g.workers))
		if err1 != nil || err2 != nil {
			t.Fatalf("seed=%d: %v / %v", seed, err1, err2)
		}
		if !seqSt.Conserved() || !runSt.Conserved() {
			t.Fatalf("seed=%d: I1 violada: seq=%+v run=%+v", seed, seqSt, runSt)
		}
		if seqC.String() != runC.String() || seqE.buf.String() != runE.buf.String() {
			t.Fatalf("seed=%d: Run difere de RunSequential", seed)
		}
	}
}

// TestProperty_NoLeak: com hard cancel em ponto gerado, Run retorna e não
// sobra goroutine (synctest falha se sobrar).
func TestProperty_NoLeak(t *testing.T) {
	base := uint64(time.Now().UnixNano())
	for i := range uint64(100) {
		seed := base + i
		g := generate(seed)
		t.Run("", func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				st, err := pipeline.Run(ctx, newProducer(g.in), g.stages(cancel), &bufConsumer{},
					pipeline.WithBuffer(g.buf), pipeline.WithWorkers(g.workers))
				if err == nil && !st.Conserved() {
					t.Fatalf("seed=%d: I1 violada sem cancelamento: %+v", seed, st)
				}
			})
		})
	}
}

// FuzzPipeline: cancelamento em ponto aleatório, buffers e workers
// aleatórios. Nada pode entrar em pânico, travar ou vazar.
func FuzzPipeline(f *testing.F) {
	f.Add(uint64(1))
	f.Add(uint64(42))
	f.Fuzz(func(t *testing.T, seed uint64) {
		g := generate(seed)
		synctest.Test(t, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			st, err := pipeline.Run(ctx, newProducer(g.in), g.stages(cancel), &bufConsumer{},
				pipeline.WithBuffer(g.buf), pipeline.WithWorkers(g.workers))
			if err == nil && !st.Conserved() {
				t.Fatalf("I1 violada: %+v", st)
			}
		})
	})
}
