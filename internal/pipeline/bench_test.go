package pipeline_test

import (
	"context"
	"crypto/sha256"
	"fmt"
	"testing"

	"github.com/FranciscoHonorat/hermes-pipe/internal/event"
	"github.com/FranciscoHonorat/hermes-pipe/internal/pipeline"
)

// discard é um Consumer que não faz nada.
type discard struct{}

func (discard) Consume(event.Event) error { return nil }
func (discard) Close() error              { return nil }

// burn simula um stage caro (custo ajustável), sem estado: Concurrent.
type burn struct{ rounds int }

func (b burn) Process(e event.Event, emit func(event.Event)) error {
	sum := [32]byte{}
	for range b.rounds {
		sum = sha256.Sum256(sum[:])
	}
	if sum[0] == 0xFF && sum[1] == 0xFF && sum[2] == 0xFF {
		return nil // nunca na prática; impede o compilador de remover o laço
	}
	emit(e)
	return nil
}
func (burn) Concurrent() bool { return true }

const benchEvents = 10_000

var benchLines = lines(benchEvents, 0)

type runner func(context.Context, pipeline.Producer, []pipeline.Stage, pipeline.Consumer, ...pipeline.Option) (pipeline.Stats, error)

func benchRun(b *testing.B, run runner, stages func() []pipeline.Stage, opts ...pipeline.Option) {
	b.ReportAllocs()
	for b.Loop() {
		if _, err := run(context.Background(), newProducer(benchLines), stages(), discard{}, opts...); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(benchEvents*b.N)/b.Elapsed().Seconds(), "events/s")
}

// BenchmarkPipeline_Buffer: a tabela buffer × throughput do ADR 0006.
// Três stages baratos: o custo dominante é a comunicação entre goroutines.
func BenchmarkPipeline_Buffer(b *testing.B) {
	stages := func() []pipeline.Stage { return []pipeline.Stage{burn{}, burn{}, burn{}} }
	b.Run("sequential", func(b *testing.B) { benchRun(b, pipeline.RunSequential, stages) })
	for _, buf := range []int{0, 1, 16, 64, 256, 1024} {
		b.Run(fmt.Sprintf("buf=%d", buf), func(b *testing.B) {
			benchRun(b, pipeline.Run, stages, pipeline.WithBuffer(buf))
		})
	}
}

// BenchmarkPipeline_StageCost: para qual custo de stage o concorrente ganha
// do sequencial (ADR 0006, pergunta 4).
func BenchmarkPipeline_StageCost(b *testing.B) {
	for _, rounds := range []int{0, 1, 10, 100} {
		stages := func() []pipeline.Stage { return []pipeline.Stage{burn{rounds}, burn{rounds}, burn{rounds}} }
		b.Run(fmt.Sprintf("rounds=%d/sequential", rounds), func(b *testing.B) { benchRun(b, pipeline.RunSequential, stages) })
		b.Run(fmt.Sprintf("rounds=%d/concurrent", rounds), func(b *testing.B) { benchRun(b, pipeline.Run, stages) })
	}
}

// BenchmarkPipeline_Parallel: throughput × workers com um stage caro.
func BenchmarkPipeline_Parallel(b *testing.B) {
	stages := func() []pipeline.Stage { return []pipeline.Stage{burn{rounds: 400}} }
	for _, w := range []int{1, 2, 4, 8} {
		b.Run(fmt.Sprintf("workers=%d", w), func(b *testing.B) {
			benchRun(b, pipeline.Run, stages, pipeline.WithWorkers(w))
		})
	}
}
