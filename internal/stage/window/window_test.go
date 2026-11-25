package window

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/FranciscoHonorat/hermes-pipe/internal/event"
	"github.com/FranciscoHonorat/hermes-pipe/internal/stage"
	"github.com/FranciscoHonorat/hermes-pipe/internal/stage/stagetest"
)

var t0 = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func TestStageContract(t *testing.T) {
	stagetest.Run(t, func() stage.Stage {
		w, err := New(Config{Size: 10 * time.Second, By: []string{"host"}, Metrics: []string{"count"}})
		if err != nil {
			t.Fatal(err)
		}
		return w
	})
}

func ev(sec float64, host string) event.Event {
	return event.New(t0.Add(time.Duration(sec*float64(time.Second))), map[string]any{"host": host})
}

type out struct {
	lines []string
	errs  []int
}

func runW(t *testing.T, w *Window, in []event.Event) out {
	t.Helper()
	var o out
	emit := func(e event.Event) {
		j, _ := e.AppendJSON(nil)
		o.lines = append(o.lines, string(j))
	}
	for i, e := range in {
		if err := w.Process(e, emit); err != nil {
			if !errors.Is(err, ErrLate) {
				t.Fatal(err)
			}
			o.errs = append(o.errs, i)
		}
	}
	if err := w.Flush(emit); err != nil {
		t.Fatal(err)
	}
	return o
}

func mustNew(t *testing.T, c Config) *Window {
	t.Helper()
	if c.By == nil {
		c.By = []string{"host"}
	}
	w, err := New(c)
	if err != nil {
		t.Fatal(err)
	}
	return w
}

// summarize reduz cada linha a "início host count[ evicted]".
func summarize(o out) []string {
	var s []string
	for _, l := range o.lines {
		e, err := event.Parse([]byte(l))
		if err != nil {
			panic(err)
		}
		start, _ := time.Parse(time.RFC3339, e.Fields["window_start"].(string))
		item := fmt.Sprintf("%d %v %v", int(start.Sub(t0).Seconds()), e.Fields["host"], e.Fields["count"])
		if e.Fields["evicted"] == true {
			item += " evicted"
		}
		s = append(s, item)
	}
	return s
}

func TestWindow_Edges(t *testing.T) {
	const s = time.Second
	tests := []struct {
		name     string
		cfg      Config
		in       []event.Event
		want     []string
		wantLate []int
	}{
		{"uma janela", Config{Size: 10 * s}, []event.Event{ev(0, "a"), ev(1, "a"), ev(9.999, "a")}, []string{"0 a 3"}, nil},
		{"fim é aberto", Config{Size: 10 * s}, []event.Event{ev(9, "a"), ev(10, "a")}, []string{"0 a 1", "10 a 1"}, nil},
		{"início é fechado", Config{Size: 10 * s}, []event.Event{ev(10, "a"), ev(19.9, "a")}, []string{"10 a 2"}, nil},
		{"duas chaves ordenadas", Config{Size: 10 * s}, []event.Event{ev(1, "b"), ev(2, "a")}, []string{"0 a 1", "0 b 1"}, nil},
		{"fecha pelo watermark", Config{Size: 10 * s}, []event.Event{ev(1, "a"), ev(25, "a")}, []string{"0 a 1", "20 a 1"}, nil},
		{"atrasado sem tolerância", Config{Size: 10 * s}, []event.Event{ev(11, "a"), ev(5, "a")}, []string{"10 a 1"}, []int{1}},
		{"atrasado dentro da tolerância", Config{Size: 10 * s, Lateness: 5 * s}, []event.Event{ev(12, "a"), ev(8, "a")}, []string{"0 a 1", "10 a 1"}, nil},
		{"atrasado fora da tolerância", Config{Size: 10 * s, Lateness: 5 * s}, []event.Event{ev(16, "a"), ev(8, "a")}, []string{"10 a 1"}, []int{1}},
		{"tolerância exata fecha", Config{Size: 10 * s, Lateness: 5 * s}, []event.Event{ev(15, "a"), ev(9, "a")}, []string{"10 a 1"}, []int{1}},
		{"watermark não recua", Config{Size: 10 * s, Lateness: 2 * s}, []event.Event{ev(30, "a"), ev(29, "a"), ev(5, "a"), ev(29.5, "a")}, []string{"20 a 2", "30 a 1"}, []int{2}},
		{"lacuna grande", Config{Size: 10 * s, MaxOpen: 3}, []event.Event{ev(0, "a"), ev(1000, "a")}, []string{"0 a 1", "1000 a 1"}, nil},
		{"teto de janelas abertas", Config{Size: 10 * s, Lateness: 100 * s, MaxOpen: 2}, []event.Event{ev(0, "a"), ev(10, "a"), ev(20, "a"), ev(5, "a")}, []string{"0 a 1", "10 a 1", "20 a 1"}, []int{3}},
		{"teto de chaves", Config{Size: 10 * s, MaxKeys: 1}, []event.Event{ev(0, "a"), ev(1, "b")}, []string{"0 a 1 evicted", "0 b 1"}, nil},
		{"vazio", Config{Size: 10 * s}, nil, nil, nil},
		{"janelas vazias no meio não emitem", Config{Size: 10 * s, Lateness: 60 * s}, []event.Event{ev(0, "a"), ev(35, "a")}, []string{"0 a 1", "30 a 1"}, nil},
		{"antes de 1970", Config{Size: 10 * s}, []event.Event{event.New(time.Unix(-5, 0), map[string]any{"host": "a"})}, []string{"-1767225610 a 1"}, nil},
		{"tamanho de 1 minuto", Config{Size: time.Minute}, []event.Event{ev(59, "a"), ev(60, "a"), ev(119, "a")}, []string{"0 a 1", "60 a 2"}, nil},
		{"fora de ordem dentro da janela", Config{Size: 10 * s}, []event.Event{ev(5, "a"), ev(1, "a"), ev(3, "a")}, []string{"0 a 3"}, nil},
		{"muitas janelas", Config{Size: s}, []event.Event{ev(0, "a"), ev(1, "a"), ev(2, "a")}, []string{"0 a 1", "1 a 1", "2 a 1"}, nil},
		{"evento no watermark exato é aceito se a janela está aberta", Config{Size: 10 * s, Lateness: 3 * s}, []event.Event{ev(13, "a"), ev(10, "a")}, []string{"10 a 2"}, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			o := runW(t, mustNew(t, tt.cfg), tt.in)
			if got := summarize(o); !slices.Equal(got, tt.want) {
				t.Fatalf("saída\n got %q\nwant %q", got, tt.want)
			}
			if !slices.Equal(o.errs, tt.wantLate) {
				t.Fatalf("atrasados = %v, quer %v", o.errs, tt.wantLate)
			}
		})
	}
}

func genEvents(r *rand.Rand, n int) []event.Event {
	in := make([]event.Event, n)
	for i := range in {
		in[i] = ev(float64(i)*0.5, fmt.Sprintf("h%d", r.IntN(4)))
	}
	return in
}

// TestWindow_Determinism: a mesma entrada dá a mesma saída, sempre.
func TestWindow_Determinism(t *testing.T) {
	seed := uint64(time.Now().UnixNano())
	t.Logf("seed=%d", seed)
	in := genEvents(rand.New(rand.NewPCG(seed, 1)), 500)
	cfg := Config{Size: 7 * time.Second, Lateness: 3 * time.Second, Metrics: []string{"count"}}
	first := runW(t, mustNew(t, cfg), in)
	for range 5 {
		if again := runW(t, mustNew(t, cfg), in); !slices.Equal(first.lines, again.lines) {
			t.Fatal("saída mudou entre execuções")
		}
	}
}

// TestWindow_ReorderWithinTolerance: embaralhar eventos com deslocamento
// menor que a tolerância não muda o resultado (propriedade).
func TestWindow_ReorderWithinTolerance(t *testing.T) {
	for i := range 50 {
		seed := uint64(i)
		r := rand.New(rand.NewPCG(seed, 2))
		in := genEvents(r, 300)
		cfg := Config{Size: 10 * time.Second, Lateness: 5 * time.Second}
		want := runW(t, mustNew(t, cfg), in)

		// Troca vizinhos cuja distância no tempo é < tolerância.
		shuffled := slices.Clone(in)
		for j := 0; j+1 < len(shuffled); j += 2 {
			if r.IntN(2) == 0 && shuffled[j+1].Time.Sub(shuffled[j].Time) < 5*time.Second {
				shuffled[j], shuffled[j+1] = shuffled[j+1], shuffled[j]
			}
		}
		got := runW(t, mustNew(t, cfg), shuffled)
		if len(got.errs) > 0 || !slices.Equal(got.lines, want.lines) {
			t.Fatalf("seed=%d: reordenar dentro da tolerância mudou a saída", seed)
		}
	}
}

func TestWindow_IdleClosesAll(t *testing.T) {
	w := mustNew(t, Config{Size: 10 * time.Second, Lateness: time.Hour})
	var n int
	emit := func(event.Event) { n++ }
	_ = w.Process(ev(1, "a"), emit)
	_ = w.Process(ev(11, "a"), emit)
	if n != 0 {
		t.Fatal("emitiu antes do Idle")
	}
	_ = w.Idle(emit)
	if n != 2 {
		t.Fatalf("Idle emitiu %d", n)
	}
	if err := w.Process(ev(2, "a"), emit); !errors.Is(err, ErrLate) {
		t.Fatalf("evento de janela fechada pelo Idle: %v", err)
	}
}

func TestNew_Errors(t *testing.T) {
	for _, c := range []Config{
		{By: []string{"h"}},
		{Size: time.Second},
		{Size: time.Second, By: []string{"h"}, Lateness: -1},
		{Size: time.Second, By: []string{"h"}, Metrics: []string{"x"}},
		{Size: time.Second, By: []string{"h"}, MaxOpen: -1},
	} {
		if _, err := New(c); err == nil {
			t.Errorf("%+v: esperava erro", c)
		}
	}
}

func FuzzWindow(f *testing.F) {
	f.Add([]byte{0, 5, 3, 200, 1, 9}, uint8(10), uint8(3))
	f.Fuzz(func(t *testing.T, offs []byte, size, late uint8) {
		if size == 0 {
			return
		}
		cfg := Config{Size: time.Duration(size) * time.Second, Lateness: time.Duration(late) * time.Second, MaxOpen: 4, MaxKeys: 2}
		in := make([]event.Event, len(offs))
		for i, o := range offs {
			in[i] = ev(float64(o), fmt.Sprintf("h%d", o%3))
		}
		w := mustNew(t, cfg)
		var lastWM time.Time
		total := 0
		emit := func(e event.Event) {
			if err := e.Validate(); err != nil {
				t.Fatal(err)
			}
			c, _ := stage.ToFloat(toF(e.Fields["count"]))
			total += int(c)
		}
		accepted := 0
		for _, e := range in {
			if err := w.Process(e, emit); err == nil {
				accepted++
			}
			if w.Watermark().Before(lastWM) {
				t.Fatal("watermark recuou")
			}
			lastWM = w.Watermark()
		}
		_ = w.Flush(emit)
		// Conservação dentro do stage: todo evento aceito é contado uma vez.
		if total != accepted {
			t.Fatalf("contou %d, aceitou %d", total, accepted)
		}
		if again := runW(t, mustNew(t, cfg), in); strings.Join(again.lines, "") == "" && accepted > 0 {
			t.Fatal("sem saída")
		}
	})
}

func toF(v any) any {
	if n, ok := v.(int64); ok {
		return float64(n)
	}
	return v
}
