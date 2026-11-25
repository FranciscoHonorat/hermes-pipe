package triage

import (
	"strings"
	"testing"

	"github.com/FranciscoHonorat/hermes-pipe/internal/event"
	"github.com/FranciscoHonorat/hermes-pipe/internal/stage"
	"github.com/FranciscoHonorat/hermes-pipe/internal/stage/stagetest"
)

func cfg() Config {
	return Config{
		DefaultDecision: AutoOK,
		Rules: []Rule{
			{Name: "erro_lento", Where: []string{"level == error", "n >= 1"}, Decision: HumanReview, Reason: "erro com n alto", RequiresAck: true},
			{Name: "qualquer_erro", Where: []string{"level == error"}, Decision: Quarantine, Reason: "erro"},
		},
	}
}

func mustNew(t testing.TB, c Config) *Triage {
	t.Helper()
	tr, err := New(c)
	if err != nil {
		t.Fatal(err)
	}
	return tr
}

func TestStageContract(t *testing.T) {
	stagetest.Run(t, func() stage.Stage { return mustNew(t, cfg()) })
}

func run(t *testing.T, tr *Triage, line string) string {
	t.Helper()
	e, err := event.Parse([]byte(line))
	if err != nil {
		t.Fatal(err)
	}
	var out []event.Event
	if err := tr.Process(e, func(o event.Event) { out = append(out, o) }); err != nil {
		t.Fatal(err)
	}
	if len(out) != 1 {
		t.Fatalf("triage precisa emitir exatamente 1 evento, emitiu %d", len(out))
	}
	b, err := out[0].AppendJSON(nil)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func TestTriage_Decisions(t *testing.T) {
	tr := mustNew(t, cfg())
	tests := []struct {
		name, in, want string
	}{
		{
			"primeira regra vence no empate",
			`{"time":"2026-01-01T00:00:00Z","level":"error","n":5}`,
			`{"level":"error","n":5,"time":"2026-01-01T00:00:00Z","triage":{"decision":"human_review","reason":"erro com n alto","requires_ack":true,"rule":"erro_lento"}}`,
		},
		{
			"segunda regra quando a primeira não casa",
			`{"time":"2026-01-01T00:00:00Z","level":"error","n":0}`,
			`{"level":"error","n":0,"time":"2026-01-01T00:00:00Z","triage":{"decision":"quarantine","reason":"erro","requires_ack":false,"rule":"qualquer_erro"}}`,
		},
		{
			"nenhuma regra: decisão padrão, rule e reason null",
			`{"time":"2026-01-01T00:00:00Z","level":"info"}`,
			`{"level":"info","time":"2026-01-01T00:00:00Z","triage":{"decision":"auto_ok","reason":null,"requires_ack":false,"rule":null}}`,
		},
		{
			"campo triage pré-existente é substituído",
			`{"time":"2026-01-01T00:00:00Z","level":"info","triage":"x"}`,
			`{"level":"info","time":"2026-01-01T00:00:00Z","triage":{"decision":"auto_ok","reason":null,"requires_ack":false,"rule":null}}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := run(t, tr, tt.in); got != tt.want {
				t.Fatalf("\ngot  %s\nwant %s", got, tt.want)
			}
		})
	}
}

// TestTriage_DefaultDecision: default_decision vazio vira auto_ok, e pode
// ser outra decisão (ex.: tudo vai para revisão, salvo o que uma regra libera).
func TestTriage_DefaultDecision(t *testing.T) {
	in := `{"time":"2026-01-01T00:00:00Z","level":"info"}`
	tr := mustNew(t, Config{Rules: []Rule{{Name: "r", Where: []string{"level == error"}, Decision: Quarantine, Reason: "x"}}})
	if got := run(t, tr, in); !strings.Contains(got, `"decision":"auto_ok"`) {
		t.Fatal(got)
	}
	tr = mustNew(t, Config{DefaultDecision: HumanReview, Rules: []Rule{{Name: "libera", Where: []string{"level == info"}, Decision: AutoOK, Reason: "ok"}}})
	if got := run(t, tr, `{"time":"2026-01-01T00:00:00Z","level":"warn"}`); !strings.Contains(got, `"decision":"human_review"`) {
		t.Fatal(got)
	}
}

func TestTriage_DottedPathWorksDownstream(t *testing.T) {
	tr := mustNew(t, cfg())
	e, _ := event.Parse([]byte(`{"time":"2026-01-01T00:00:00Z","level":"error","n":5}`))
	var out event.Event
	_ = tr.Process(e, func(o event.Event) { out = o })
	for path, want := range map[string]any{"triage.decision": "human_review", "triage.rule": "erro_lento", "triage.requires_ack": true} {
		if v, ok := out.Get(path); !ok || v != want {
			t.Errorf("Get(%q) = %v, %v; want %v", path, v, ok, want)
		}
	}
}

func TestNew_Errors(t *testing.T) {
	ok := Rule{Name: "r", Where: []string{"a == 1"}, Decision: Quarantine, Reason: "x"}
	with := func(f func(*Rule)) Config {
		r := ok
		f(&r)
		return Config{Rules: []Rule{r}}
	}
	tests := []struct {
		name string
		cfg  Config
		msg  string
	}{
		{"sem regras", Config{}, "pelo menos uma regra"},
		{"default inválido", Config{DefaultDecision: "talvez", Rules: []Rule{ok}}, `default_decision "talvez"`},
		{"sem nome", with(func(r *Rule) { r.Name = "" }), "sem name"},
		{"nome repetido", Config{Rules: []Rule{ok, ok}}, `"r" repetida`},
		{"sem where", with(func(r *Rule) { r.Where = nil }), "where"},
		{"where inválido", with(func(r *Rule) { r.Where = []string{"a"} }), "operador ausente"},
		{"decisão inválida", with(func(r *Rule) { r.Decision = "x" }), `decision "x"`},
		{"decisão vazia", with(func(r *Rule) { r.Decision = "" }), `decision ""`},
		{"sem reason", with(func(r *Rule) { r.Reason = " " }), "reason"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := New(tt.cfg)
			if err == nil || !strings.Contains(err.Error(), tt.msg) || !strings.HasPrefix(err.Error(), "triage: ") {
				t.Fatalf("err = %v; want contendo %q", err, tt.msg)
			}
		})
	}
}

func TestTriage_Concurrent(t *testing.T) {
	if !mustNew(t, cfg()).Concurrent() {
		t.Fatal("triage é sem estado: precisa ser Concurrent")
	}
}

var sink event.Event

func BenchmarkTriage(b *testing.B) {
	tr := mustNew(b, cfg())
	e, _ := event.Parse([]byte(`{"time":"2026-01-01T00:00:00Z","level":"error","host":"a","n":5,"msg":"x"}`))
	emit := func(o event.Event) { sink = o }
	b.ReportAllocs()
	for b.Loop() {
		_ = tr.Process(e, emit)
	}
}
