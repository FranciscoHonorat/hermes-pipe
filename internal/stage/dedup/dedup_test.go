package dedup

import (
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/FranciscoHonorat/hermes-pipe/internal/event"
	"github.com/FranciscoHonorat/hermes-pipe/internal/stage"
	"github.com/FranciscoHonorat/hermes-pipe/internal/stage/stagetest"
)

var t0 = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func TestStageContract(t *testing.T) {
	stagetest.Run(t, func() stage.Stage {
		d, err := New(Config{By: []string{"host"}, Window: time.Minute})
		if err != nil {
			t.Fatal(err)
		}
		return d
	})
}

func ev(sec int, id string, n int) event.Event {
	return event.New(t0.Add(time.Duration(sec)*time.Second), map[string]any{"id": id, "n": n})
}

func passed(t *testing.T, d *Dedup, in []event.Event) []int {
	t.Helper()
	var out []int
	for _, e := range in {
		if err := d.Process(e, func(o event.Event) { out = append(out, o.Fields["n"].(int)) }); err != nil {
			t.Fatal(err)
		}
	}
	return out
}

func TestDedup_FirstWins(t *testing.T) {
	d, _ := New(Config{By: []string{"id"}, Window: 10 * time.Second})
	in := []event.Event{
		ev(0, "a", 1), ev(1, "a", 2), ev(2, "b", 3), ev(9, "a", 4),
		ev(10, "a", 5), // 10s depois da primeira: fora da janela, passa
		ev(15, "a", 6), // dentro da janela da 5
		ev(15, "b", 7),
	}
	if got := passed(t, d, in); !slices.Equal(got, []int{1, 3, 5, 7}) {
		t.Fatalf("passaram %v", got)
	}
}

func TestDedup_Bounded(t *testing.T) {
	d, _ := New(Config{By: []string{"id"}, Window: time.Hour, MaxKeys: 100})
	var in []event.Event
	for i := range 10_000 {
		in = append(in, ev(i%60, fmt.Sprint(i), i))
	}
	passed(t, d, in)
	if d.Len() > 100 {
		t.Fatalf("len = %d, teto 100", d.Len())
	}
	// O teto esquece a chave mais antiga: o duplicado dela passa.
	if got := passed(t, d, []event.Event{ev(0, "0", -1), ev(0, "9999", -2)}); !slices.Equal(got, []int{-1}) {
		t.Fatalf("passaram %v", got)
	}
}

func TestDedup_Expires(t *testing.T) {
	d, _ := New(Config{By: []string{"id"}, Window: 5 * time.Second})
	for i := range 100 {
		passed(t, d, []event.Event{ev(i, fmt.Sprint(i), i)})
	}
	if d.Len() > 6 {
		t.Fatalf("expiração não liberou memória: len = %d", d.Len())
	}
}

// TestDedup_DistinctKeysAllPass: chaves diferentes nunca são duplicadas.
// Com 1000 chaves, um hash fraco (8 bits, por exemplo) colide e este teste
// cai, enquanto o de determinismo continua passando.
func TestDedup_DistinctKeysAllPass(t *testing.T) {
	d, _ := New(Config{By: []string{"id"}, Window: time.Hour})
	var in []event.Event
	for i := range 1000 {
		in = append(in, ev(0, fmt.Sprintf("req-%d", i), i))
	}
	if got := passed(t, d, in); len(got) != 1000 {
		t.Fatalf("passaram %d de 1000 chaves distintas: colisão de hash", len(got))
	}
}

// TestDedup_ForcedCollision: com um hash que colide sempre, chaves
// diferentes viram "duplicadas". É o efeito documentado de guardar só o hash.
func TestDedup_ForcedCollision(t *testing.T) {
	d, _ := NewWithHasher(Config{By: []string{"id"}, Window: time.Minute}, func(string) uint64 { return 42 })
	got := passed(t, d, []event.Event{ev(0, "a", 1), ev(1, "b", 2), ev(2, "c", 3)})
	if !slices.Equal(got, []int{1}) {
		t.Fatalf("passaram %v; com colisão total só a primeira passa", got)
	}
}

func TestDedup_Deterministic(t *testing.T) {
	var in []event.Event
	for i := range 2000 {
		in = append(in, ev(i/10, fmt.Sprint(i%37), i))
	}
	a, _ := New(Config{By: []string{"id"}, Window: 3 * time.Second, MaxKeys: 20})
	b, _ := New(Config{By: []string{"id"}, Window: 3 * time.Second, MaxKeys: 20})
	if !slices.Equal(passed(t, a, in), passed(t, b, in)) {
		t.Fatal("não determinístico")
	}
}

func TestNew_Errors(t *testing.T) {
	for _, c := range []Config{{Window: time.Second}, {By: []string{"x"}}, {By: []string{"x"}, Window: time.Second, MaxKeys: -1}} {
		if _, err := New(c); err == nil {
			t.Errorf("%+v: esperava erro", c)
		}
	}
}

func BenchmarkDedup(b *testing.B) {
	d, _ := New(Config{By: []string{"id"}, Window: time.Minute})
	evs := make([]event.Event, 1000)
	for i := range evs {
		evs[i] = ev(i, fmt.Sprint(i%100), i)
	}
	emit := func(event.Event) {}
	i := 0
	b.ReportAllocs()
	for b.Loop() {
		_ = d.Process(evs[i%len(evs)], emit)
		i++
	}
}
