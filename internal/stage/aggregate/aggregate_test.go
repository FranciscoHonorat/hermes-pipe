package aggregate

import (
	"fmt"
	"runtime"
	"runtime/metrics"
	"strings"
	"testing"
	"time"

	"github.com/FranciscoHonorat/hermes-pipe/internal/event"
	"github.com/FranciscoHonorat/hermes-pipe/internal/stage"
	"github.com/FranciscoHonorat/hermes-pipe/internal/stage/stagetest"
)

func TestStageContract(t *testing.T) {
	stagetest.Run(t, func() stage.Stage {
		a, err := New(Config{By: []string{"host"}, Metrics: []string{"count", "sum:n", "p50:n"}, MaxKeys: 2})
		if err != nil {
			t.Fatal(err)
		}
		return a
	})
}

func ev(t testing.TB, i int, fields string) event.Event {
	t.Helper()
	ts := time.Date(2026, 1, 1, 0, 0, i, 0, time.UTC).Format(time.RFC3339)
	e, err := event.Parse(fmt.Appendf(nil, `{"time":%q,%s}`, ts, fields))
	if err != nil {
		t.Fatal(err)
	}
	return e
}

func run(t *testing.T, a *Aggregate, in []event.Event) string {
	t.Helper()
	var b strings.Builder
	emit := func(e event.Event) {
		j, _ := e.AppendJSON(nil)
		b.Write(j)
		b.WriteByte('\n')
	}
	for _, e := range in {
		if err := a.Process(e, emit); err != nil {
			t.Fatal(err)
		}
	}
	if err := a.Flush(emit); err != nil {
		t.Fatal(err)
	}
	return b.String()
}

func TestAggregate_Metrics(t *testing.T) {
	a, err := New(Config{By: []string{"host"}, Metrics: []string{"count", "sum:ms", "min:ms", "max:ms", "avg:ms", "p50:ms", "p100:ms"}})
	if err != nil {
		t.Fatal(err)
	}
	in := []event.Event{
		ev(t, 0, `"host":"b","ms":10`),
		ev(t, 1, `"host":"a","ms":1`),
		ev(t, 2, `"host":"a","ms":3`),
		ev(t, 3, `"host":"a","ms":"x"`),
		ev(t, 4, `"host":"a","ms":2`),
	}
	got := run(t, a, in)
	want := `{"avg_ms":2,"count":4,"host":"a","max_ms":3,"min_ms":1,"p100_ms":3,"p50_ms":2,"sum_ms":6,"time":"2026-01-01T00:00:04Z"}
{"avg_ms":10,"count":1,"host":"b","max_ms":10,"min_ms":10,"p100_ms":10,"p50_ms":10,"sum_ms":10,"time":"2026-01-01T00:00:00Z"}
`
	if got != want {
		t.Fatalf("got\n%s\nwant\n%s", got, want)
	}
}

func TestAggregate_TopN(t *testing.T) {
	a, _ := New(Config{By: []string{"host"}, Top: 2})
	var in []event.Event
	for i, h := range []string{"c", "a", "b", "a", "c", "a", "d", "b"} {
		in = append(in, ev(t, i, fmt.Sprintf(`"host":%q`, h)))
	}
	got := run(t, a, in)
	want := `{"count":3,"host":"a","time":"2026-01-01T00:00:05Z"}
{"count":2,"host":"b","time":"2026-01-01T00:00:07Z"}
`
	if got != want {
		t.Fatalf("got\n%s\nwant\n%s", got, want)
	}
}

func TestAggregate_MultiKeyAndMissing(t *testing.T) {
	a, _ := New(Config{By: []string{"host", "level"}})
	got := run(t, a, []event.Event{ev(t, 0, `"host":"a","level":"x"`), ev(t, 1, `"host":"a"`), ev(t, 2, `"host":"a","level":"x"`)})
	want := `{"count":1,"host":"a","level":null,"time":"2026-01-01T00:00:01Z"}
{"count":2,"host":"a","level":"x","time":"2026-01-01T00:00:02Z"}
`
	if got != want {
		t.Fatalf("got\n%s\nwant\n%s", got, want)
	}
}

// TestAggregate_Eviction: com o teto cheio, a chave quente sobrevive e a fria
// sai, emitida na hora como parcial.
func TestAggregate_Eviction(t *testing.T) {
	a, _ := New(Config{By: []string{"k"}, MaxKeys: 2})
	in := []event.Event{
		ev(t, 0, `"k":"hot"`), ev(t, 1, `"k":"cold"`), ev(t, 2, `"k":"hot"`),
		ev(t, 3, `"k":"new"`), ev(t, 4, `"k":"hot"`),
	}
	got := run(t, a, in)
	want := `{"count":1,"evicted":true,"k":"cold","time":"2026-01-01T00:00:01Z"}
{"count":3,"k":"hot","time":"2026-01-01T00:00:04Z"}
{"count":1,"k":"new","time":"2026-01-01T00:00:03Z"}
`
	if got != want {
		t.Fatalf("got\n%s\nwant\n%s", got, want)
	}
	if a.Evicted() != 1 {
		t.Fatalf("evicted = %d", a.Evicted())
	}
}

func TestAggregate_PercentileBounded(t *testing.T) {
	a, _ := New(Config{By: []string{"k"}, Metrics: []string{"p50:v", "p99:v"}})
	var in []event.Event
	for i := range 10_000 {
		in = append(in, ev(t, 0, fmt.Sprintf(`"k":"x","v":%d`, i)))
	}
	out := run(t, a, in)
	e, err := event.Parse([]byte(strings.TrimSpace(out)))
	if err != nil {
		t.Fatal(err)
	}
	p50, _ := stage.ToFloat(e.Fields["p50_v"])
	p99, _ := stage.ToFloat(e.Fields["p99_v"])
	// Amostra de 512 de 10000: tolerância larga, mas o resultado é sempre o mesmo.
	if p50 < 4000 || p50 > 6000 || p99 < 9500 {
		t.Fatalf("p50=%v p99=%v", p50, p99)
	}
	if again := run(t, mustNew(t, Config{By: []string{"k"}, Metrics: []string{"p50:v", "p99:v"}}), in); again != out {
		t.Fatal("percentil não é determinístico")
	}
}

func mustNew(t *testing.T, c Config) *Aggregate {
	a, err := New(c)
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func TestNew_Errors(t *testing.T) {
	for _, c := range []Config{
		{},
		{By: []string{"h"}, Metrics: []string{"sum"}},
		{By: []string{"h"}, Metrics: []string{"count:x"}},
		{By: []string{"h"}, Metrics: []string{"p0:x"}},
		{By: []string{"h"}, Metrics: []string{"p101:x"}},
		{By: []string{"h"}, Metrics: []string{"median:x"}},
		{By: []string{"h"}, Metrics: []string{"count", "count"}},
		{By: []string{"h"}, Top: -1},
	} {
		if _, err := New(c); err == nil {
			t.Errorf("%+v: esperava erro", c)
		}
	}
}

func liveHeap() uint64 {
	runtime.GC()
	s := []metrics.Sample{{Name: "/gc/heap/live:bytes"}}
	metrics.Read(s)
	return s[0].Value.Uint64()
}

// TestAggregate_Bounded: 1 milhão de chaves distintas com teto de 1000. O
// heap vivo cresce pelo teto, não pela cardinalidade (I6).
func TestAggregate_Bounded(t *testing.T) {
	if testing.Short() {
		t.Skip("lento")
	}
	const keys, maxKeys = 1_000_000, 1000
	a := mustNew(t, Config{By: []string{"k"}, Metrics: []string{"count", "sum:v"}, MaxKeys: maxKeys})
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	emit := func(event.Event) {}
	before := liveHeap()
	for i := range keys {
		e := event.New(base, map[string]any{"k": fmt.Sprint(i), "v": i})
		_ = a.Process(e, emit)
	}
	after := liveHeap()
	t.Logf("heap vivo: antes %d KiB, depois %d KiB", before>>10, after>>10)
	if a.Len() != maxKeys || a.Evicted() != keys-maxKeys {
		t.Fatalf("len=%d evicted=%d", a.Len(), a.Evicted())
	}
	// ~1 KiB por grupo é uma margem folgada; sem teto seriam centenas de MB.
	const budget = maxKeys*1024 + 4<<20
	if after > before && after-before > budget {
		t.Fatalf("heap vivo cresceu %d bytes; orçamento %d", after-before, budget)
	}
	runtime.KeepAlive(a)
}

func BenchmarkAggregate(b *testing.B) {
	for _, card := range []int{10, 100, 1000, 10_000} {
		b.Run(fmt.Sprintf("keys=%d", card), func(b *testing.B) {
			a, _ := New(Config{By: []string{"k"}, Metrics: []string{"count", "sum:v"}})
			base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
			evs := make([]event.Event, card)
			for i := range evs {
				evs[i] = event.New(base, map[string]any{"k": fmt.Sprint(i), "v": i})
			}
			emit := func(event.Event) {}
			i := 0
			b.ReportAllocs()
			for b.Loop() {
				_ = a.Process(evs[i%card], emit)
				i++
			}
		})
	}
}
