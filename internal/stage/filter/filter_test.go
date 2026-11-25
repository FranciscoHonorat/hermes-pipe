package filter

import (
	"reflect"
	"regexp"
	"testing"

	"github.com/FranciscoHonorat/hermes-pipe/internal/event"
	"github.com/FranciscoHonorat/hermes-pipe/internal/stage"
	"github.com/FranciscoHonorat/hermes-pipe/internal/stage/stagetest"
)

func TestStageContract(t *testing.T) {
	stagetest.Run(t, func() stage.Stage {
		f, err := New(Config{Where: []string{"level==error"}})
		if err != nil {
			t.Fatal(err)
		}
		return f
	})
}

const sample = `{"time":"2026-01-01T00:00:00Z","level":"error","host":"web-03","n":10,"f":2.5,"ok":true,"nil":null,"msg":"upstream timeout","http":{"status":504},"s":"10"}`

func TestFilter(t *testing.T) {
	e, err := event.Parse([]byte(sample))
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		expr string
		want bool
	}{
		// igualdade de string
		{"level==error", true},
		{"level == error", true},
		{`level=="error"`, true},
		{"level==info", false},
		{"level!=info", true},
		{"level!=error", false},
		{"host==web-03", true},
		{`host=="web-03"`, true},
		// números
		{"n==10", true},
		{"n==10.0", true},
		{"n!=10", false},
		{"n>5", true},
		{"n>10", false},
		{"n>=10", true},
		{"n<11", true},
		{"n<10", false},
		{"n<=10", true},
		{"f>2", true},
		{"f<2.6", true},
		{"f==2.5", true},
		{"n>-1", true},
		{"n>1e3", false},
		// tipos diferentes nunca são iguais
		{"s==10", false},
		{`s=="10"`, true},
		{`n=="10"`, false},
		{"s>5", false},
		{"level>5", false},
		// bool e null
		{"ok==true", true},
		{"ok==false", false},
		{"ok!=false", true},
		{"nil==null", true},
		{"nil!=null", false},
		{"ausente==null", true},
		{"ausente!=null", false},
		{"ausente==x", false},
		{"ausente!=x", true},
		{"ausente>1", false},
		{"ausente<1", false},
		// ordem lexicográfica
		{"level>abc", true},
		{"level<f", true},
		// caminho aninhado
		{"http.status==504", true},
		{"http.status>=500", true},
		{"http.status<500", false},
		{"http.nada==1", false},
		// regex
		{"msg=~timeout", true},
		{"msg=~^upstream", true},
		{`msg=~"^time"`, false},
		{"msg!~^upstream", false},
		{"msg!~refused", true},
		{"n=~1", false},
		{"ausente=~.*", false},
		{"ausente!~.*", true},
	}
	for _, tt := range tests {
		t.Run(tt.expr, func(t *testing.T) {
			f, err := New(Config{Where: []string{tt.expr}})
			if err != nil {
				t.Fatal(err)
			}
			var got bool
			if err := f.Process(e, func(event.Event) { got = true }); err != nil {
				t.Fatal(err)
			}
			if got != tt.want {
				t.Fatalf("%s: got %v want %v", tt.expr, got, tt.want)
			}
		})
	}
}

func TestFilter_And(t *testing.T) {
	e, _ := event.Parse([]byte(sample))
	f, err := New(Config{Where: []string{"level==error", "n>100"}})
	if err != nil {
		t.Fatal(err)
	}
	called := false
	_ = f.Process(e, func(event.Event) { called = true })
	if called {
		t.Fatal("E lógico falhou")
	}
}

func TestNew_Errors(t *testing.T) {
	for _, expr := range []string{"", "level", "==x", "level==", "msg=~(", " "} {
		if _, err := New(Config{Where: []string{expr}}); err == nil {
			t.Errorf("%q: esperava erro", expr)
		}
	}
	if _, err := New(Config{}); err == nil {
		t.Error("config vazia: esperava erro")
	}
}

// TestFilter_ZeroAllocs trava o orçamento de alocação: Filter não aloca.
func TestFilter_ZeroAllocs(t *testing.T) {
	e, _ := event.Parse([]byte(sample))
	f, _ := New(Config{Where: []string{"level==error", "n>=10", "http.status==504", "msg=~timeout"}})
	n := 0
	emit := func(event.Event) { n++ }
	allocs := testing.AllocsPerRun(1000, func() {
		_ = f.Process(e, emit)
	})
	if allocs != 0 {
		t.Fatalf("Filter.Process aloca %.1f por evento; orçamento é 0", allocs)
	}
}

var sinkBool bool

func BenchmarkFilter(b *testing.B) {
	e, _ := event.Parse([]byte(sample))
	f, _ := New(Config{Where: []string{"level==error", "n>=10"}})
	emit := func(event.Event) { sinkBool = true }
	b.ReportAllocs()
	for b.Loop() {
		_ = f.Process(e, emit)
	}
}

// Alternativas medidas para o ADR 0004.

func BenchmarkFilterAlt_DeepEqual(b *testing.B) {
	e, _ := event.Parse([]byte(sample))
	want := any("error")
	b.ReportAllocs()
	for b.Loop() {
		sinkBool = reflect.DeepEqual(e.Fields["level"], want)
	}
}

func BenchmarkFilterAlt_RegexPerEvent(b *testing.B) {
	e, _ := event.Parse([]byte(sample))
	b.ReportAllocs()
	for b.Loop() {
		re := regexp.MustCompile("timeout")
		sinkBool = re.MatchString(e.Fields["msg"].(string))
	}
}

func BenchmarkFilterAlt_RegexPrecompiled(b *testing.B) {
	e, _ := event.Parse([]byte(sample))
	re := regexp.MustCompile("timeout")
	b.ReportAllocs()
	for b.Loop() {
		sinkBool = re.MatchString(e.Fields["msg"].(string))
	}
}
