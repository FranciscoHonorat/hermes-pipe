package reconcile

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/FranciscoHonorat/hermes-pipe/internal/event"
	"github.com/FranciscoHonorat/hermes-pipe/internal/stage"
	"github.com/FranciscoHonorat/hermes-pipe/internal/stage/stagetest"
)

func baseCfg() Config {
	return Config{CorrelateBy: "incident_id", Window: time.Minute}
}

func mustNew(t testing.TB, c Config) *Reconcile {
	t.Helper()
	r, err := New(c)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestStageContract(t *testing.T) {
	stagetest.Run(t, func() stage.Stage { return mustNew(t, baseCfg()) })
}

// Atalhos para montar linhas.
func exc(ts, id string) string {
	return `{"time":"2026-01-01T00:` + ts + `Z","kind":"exception","incident_id":"` + id + `"}`
}

func dec(ts, decisionID, target string) string {
	return `{"time":"2026-01-01T00:` + ts + `Z","kind":"human_decision","decision_id":"` + decisionID +
		`","target_key":"` + target + `","operator_id":"op-7","action":"approve","reason":"calibrado"}`
}

func tele(ts string) string { return `{"time":"2026-01-01T00:` + ts + `Z","v":1}` }

// feed roda as linhas e devolve uma linha de saída por evento emitido (JSON)
// ou "err: ..." por erro; flush acrescenta o que o Flush emitir.
func feed(t *testing.T, r *Reconcile, flush bool, lines ...string) []string {
	t.Helper()
	var out []string
	emit := func(e event.Event) {
		b, err := e.AppendJSON(nil)
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, string(b))
	}
	for i, l := range lines {
		e, err := event.Parse([]byte(l))
		if err != nil {
			t.Fatalf("linha %d: %v", i, err)
		}
		if err := r.Process(e, emit); err != nil {
			out = append(out, "err: "+err.Error())
		}
	}
	if flush {
		if err := r.Flush(emit); err != nil {
			t.Fatal(err)
		}
	}
	return out
}

func expect(t *testing.T, got []string, want ...string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("emitiu %d, quer %d:\n%s", len(got), len(want), strings.Join(got, "\n"))
	}
	for i := range want {
		if !strings.Contains(got[i], want[i]) {
			t.Errorf("saída %d:\n got  %s\n want contendo %s", i, got[i], want[i])
		}
	}
}

func TestReconcile_TelemetryPassesThrough(t *testing.T) {
	got := feed(t, mustNew(t, baseCfg()), true, tele("00:00"), tele("00:01"))
	expect(t, got, `{"time":"2026-01-01T00:00:00Z","v":1}`, `{"time":"2026-01-01T00:00:01Z","v":1}`)
}

func TestReconcile_Resolved(t *testing.T) {
	got := feed(t, mustNew(t, baseCfg()), true,
		exc("00:00", "inc-1"),
		tele("00:10"),
		dec("00:30", "d-1", "inc-1"),
	)
	expect(t, got,
		`"v":1`, // a exceção é retida; a telemetria passa
		`"incident_id":"inc-1","kind":"exception","reconcile":{"action":"approve","decided_at":"2026-01-01T00:00:30Z","decision_id":"d-1","operator_id":"op-7","reason":"calibrado","status":"resolved"},"time":"2026-01-01T00:00:00Z"}`,
	)
}

// TestReconcile_Idempotent: a mesma decisão duas vezes não tem efeito
// duplicado; outra decisão para um incidente já resolvido é órfã.
func TestReconcile_Idempotent(t *testing.T) {
	r := mustNew(t, baseCfg())
	got := feed(t, r, true,
		exc("00:00", "inc-1"),
		dec("00:10", "d-1", "inc-1"),
		dec("00:11", "d-1", "inc-1"), // reenvio da rede: descartado
		dec("00:12", "d-2", "inc-1"), // segundo clique, outro id: órfã
	)
	expect(t, got, `"status":"resolved"`, `"decision_id":"d-2"`)
	if !strings.Contains(got[1], `"reconcile":{"status":"orphan_decision"}`) {
		t.Fatal(got[1])
	}
	if r.Duplicates() != 1 {
		t.Fatalf("Duplicates = %d", r.Duplicates())
	}
}

func TestReconcile_DuplicateMark(t *testing.T) {
	c := baseCfg()
	c.OnDuplicate = DuplicateMark
	got := feed(t, mustNew(t, c), false,
		exc("00:00", "inc-1"),
		dec("00:10", "d-1", "inc-1"),
		dec("00:11", "d-1", "inc-1"),
	)
	expect(t, got, `"status":"resolved"`, `"reconcile":{"decision_id":"d-1","status":"duplicate_ignored"}`)
}

func TestReconcile_Timeout(t *testing.T) {
	for _, tt := range []struct {
		policy TimeoutPolicy
		want   []string
	}{
		{"", []string{`"incident_id":"inc-1","kind":"exception","reconcile":{"deadline":"2026-01-01T00:01:00Z","status":"escalated_timeout"}`, `"v":1`}},
		{TimeoutQuarantine, []string{`"status":"quarantined_timeout"`, `"v":1`}},
		{TimeoutDrop, []string{`"v":1`}},
	} {
		t.Run(string(tt.policy), func(t *testing.T) {
			c := baseCfg()
			c.OnTimeout = tt.policy
			// 00:01:00 é o prazo exato: ainda vale. 00:01:01 passa dele.
			got := feed(t, mustNew(t, c), true, exc("00:00", "inc-1"), tele("01:01"))
			expect(t, got, tt.want...)
		})
	}
}

func TestReconcile_DeadlineInclusive(t *testing.T) {
	got := feed(t, mustNew(t, baseCfg()), false, exc("00:00", "inc-1"), dec("01:00", "d-1", "inc-1"))
	expect(t, got, `"status":"resolved"`)
}

// TestReconcile_LateDecision: depois do prazo a decisão não ressuscita a
// pendência: a exceção sai escalada e a decisão sai órfã.
func TestReconcile_LateDecision(t *testing.T) {
	got := feed(t, mustNew(t, baseCfg()), true, exc("00:00", "inc-1"), dec("02:00", "d-1", "inc-1"))
	expect(t, got, `"status":"escalated_timeout"`, `"status":"orphan_decision"`)
}

func TestReconcile_ExpiryOrderIsDeadlineThenArrival(t *testing.T) {
	got := feed(t, mustNew(t, baseCfg()), false,
		exc("00:20", "b"),
		exc("00:10", "a"), // fora de ordem: prazo menor, expira antes
		exc("00:20", "c"), // mesmo prazo de b: chegou depois, sai depois
		tele("05:00"),
	)
	expect(t, got, `"incident_id":"a"`, `"incident_id":"b"`, `"incident_id":"c"`, `"v":1`)
}

func TestReconcile_Bounded(t *testing.T) {
	c := baseCfg()
	c.MaxKeys = 2
	r := mustNew(t, c)
	got := feed(t, r, false, exc("00:00", "a"), exc("00:01", "b"), exc("00:02", "c"))
	expect(t, got, `"evicted":true,"incident_id":"a","kind":"exception","reconcile":{"deadline":"2026-01-01T00:01:00Z","status":"evicted"}`)
	if r.Len() != 2 || r.Evicted() != 1 {
		t.Fatalf("Len=%d Evicted=%d", r.Len(), r.Evicted())
	}
}

// TestReconcile_DecisionMemoryBounded: o conjunto de decision_id tem teto;
// esquecido, um reenvio vira órfão (a pendência já foi resolvida), nunca
// uma segunda aplicação.
func TestReconcile_DecisionMemoryBounded(t *testing.T) {
	c := baseCfg()
	c.MaxDecisions = 1
	got := feed(t, mustNew(t, c), false,
		exc("00:00", "inc-1"),
		dec("00:01", "d-1", "inc-1"),
		dec("00:02", "d-x", "nada"),
		dec("00:03", "d-1", "inc-1"),
	)
	expect(t, got, `"status":"resolved"`, `"status":"orphan_decision"`, `"status":"orphan_decision"`)
}

// TestReconcile_LateExceptionDoesNotEvict: uma exceção que já chega depois
// do prazo fecha na hora e não toma o lugar de uma pendência viva.
func TestReconcile_LateExceptionDoesNotEvict(t *testing.T) {
	c := baseCfg()
	c.MaxKeys = 1
	r := mustNew(t, c)
	got := feed(t, r, false, exc("05:00", "viva"), exc("00:00", "atrasada"))
	expect(t, got, `"incident_id":"atrasada","kind":"exception","reconcile":{"deadline":"2026-01-01T00:01:00Z","status":"escalated_timeout"}`)
	if r.Evicted() != 0 || r.Len() != 1 {
		t.Fatalf("Evicted=%d Len=%d", r.Evicted(), r.Len())
	}
}

func TestReconcile_DuplicatePending(t *testing.T) {
	got := feed(t, mustNew(t, baseCfg()), true, exc("00:00", "inc-1"), exc("00:05", "inc-1"))
	expect(t, got, `"reconcile":{"status":"duplicate_pending"},"time":"2026-01-01T00:00:05Z"`, `"status":"unresolved"`)
}

func TestReconcile_TriageRequiresAck(t *testing.T) {
	line := `{"time":"2026-01-01T00:00:00Z","incident_id":"r9","triage":{"decision":"quarantine","requires_ack":true}}`
	noAck := `{"time":"2026-01-01T00:00:00Z","incident_id":"r8","triage":{"decision":"auto_ok","requires_ack":false}}`
	got := feed(t, mustNew(t, baseCfg()), true, line, noAck)
	expect(t, got, `"incident_id":"r8"`, `"incident_id":"r9","reconcile":{"deadline":"2026-01-01T00:01:00Z","status":"unresolved"}`)
}

func TestReconcile_Flush(t *testing.T) {
	got := feed(t, mustNew(t, baseCfg()), true, exc("00:30", "b"), exc("00:00", "a"))
	expect(t, got, `"incident_id":"a"`, `"incident_id":"b"`)
	for _, g := range got {
		if !strings.Contains(g, `"status":"unresolved"`) {
			t.Fatal(g)
		}
	}
}

func TestReconcile_EventErrors(t *testing.T) {
	r := mustNew(t, baseCfg())
	for _, l := range []string{
		`{"time":"2026-01-01T00:00:00Z","kind":"exception"}`,
		`{"time":"2026-01-01T00:00:00Z","kind":"human_decision","target_key":"x","operator_id":"o","action":"a","reason":"r"}`,
		`{"time":"2026-01-01T00:00:00Z","kind":"human_decision","decision_id":"d","operator_id":"o","action":"a","reason":"r"}`,
		`{"time":"2026-01-01T00:00:00Z","kind":"human_decision","decision_id":"d","target_key":"x","action":"a","reason":"r"}`,
		`{"time":"2026-01-01T00:00:00Z","kind":"human_decision","decision_id":"d","target_key":"x","operator_id":"o","reason":"r"}`,
		`{"time":"2026-01-01T00:00:00Z","kind":"human_decision","decision_id":"d","target_key":"x","operator_id":"o","action":"a","reason":" "}`,
		`{"time":"2026-01-01T00:00:00Z","kind":"human_decision","decision_id":7,"target_key":"x","operator_id":"o","action":"a","reason":"r"}`,
	} {
		e, _ := event.Parse([]byte(l))
		err := r.Process(e, func(event.Event) { t.Errorf("emitiu com entrada inválida: %s", l) })
		if !errors.Is(err, ErrNoKey) && !errors.Is(err, ErrInvalidDecision) {
			t.Errorf("%s: err = %v", l, err)
		}
	}
	if r.Len() != 0 {
		t.Fatal("entrada inválida abriu pendência")
	}
}

func TestNew_Errors(t *testing.T) {
	for _, tt := range []struct {
		name string
		mut  func(*Config)
		msg  string
	}{
		{"sem correlate_by", func(c *Config) { c.CorrelateBy = " " }, "correlate_by"},
		{"window zero", func(c *Config) { c.Window = 0 }, "window"},
		{"max_keys negativo", func(c *Config) { c.MaxKeys = -1 }, "max_keys"},
		{"max_decisions negativo", func(c *Config) { c.MaxDecisions = -1 }, "max_decisions"},
		{"on_timeout", func(c *Config) { c.OnTimeout = "x" }, `on_timeout "x"`},
		{"on_duplicate", func(c *Config) { c.OnDuplicate = "x" }, `on_duplicate "x"`},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c := baseCfg()
			tt.mut(&c)
			_, err := New(c)
			if err == nil || !strings.HasPrefix(err.Error(), "reconcile: ") || !strings.Contains(err.Error(), tt.msg) {
				t.Fatalf("err = %v", err)
			}
		})
	}
}

func BenchmarkReconcile(b *testing.B) {
	r := mustNew(b, Config{CorrelateBy: "incident_id", Window: time.Hour, MaxKeys: 1024})
	e1, _ := event.Parse([]byte(exc("00:00", "inc-1")))
	e2, _ := event.Parse([]byte(dec("00:01", "d-1", "inc-1")))
	emit := func(event.Event) {}
	b.ReportAllocs()
	for b.Loop() {
		_ = r.Process(e1, emit)
		_ = r.Process(e2, emit)
		r.seen.Delete("d-1") // cada iteração é um ciclo novo
	}
}
