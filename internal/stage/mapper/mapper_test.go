package mapper

import (
	"testing"

	"github.com/FranciscoHonorat/hermes-pipe/internal/event"
	"github.com/FranciscoHonorat/hermes-pipe/internal/stage"
	"github.com/FranciscoHonorat/hermes-pipe/internal/stage/stagetest"
)

func TestStageContract(t *testing.T) {
	stagetest.Run(t, func() stage.Stage {
		m, err := New(Config{Ops: []string{"rename:msg=message", "set:x=1", "delete:host", "copy:level=sev"}})
		if err != nil {
			t.Fatal(err)
		}
		return m
	})
}

func TestMap(t *testing.T) {
	const in = `{"time":"2026-01-01T00:00:00Z","msg":"oi","level":"error","host":"a"}`
	tests := []struct {
		ops  []string
		want string
	}{
		{[]string{"rename:msg=message"}, `{"host":"a","level":"error","message":"oi","time":"2026-01-01T00:00:00Z"}`},
		{[]string{"rename:nada=x"}, `{"host":"a","level":"error","msg":"oi","time":"2026-01-01T00:00:00Z"}`},
		{[]string{"copy:level=sev"}, `{"host":"a","level":"error","msg":"oi","sev":"error","time":"2026-01-01T00:00:00Z"}`},
		{[]string{"copy:time=ts"}, `{"host":"a","level":"error","msg":"oi","time":"2026-01-01T00:00:00Z","ts":"2026-01-01T00:00:00Z"}`},
		{[]string{"delete:host"}, `{"level":"error","msg":"oi","time":"2026-01-01T00:00:00Z"}`},
		{[]string{"set:n=42"}, `{"host":"a","level":"error","msg":"oi","n":42,"time":"2026-01-01T00:00:00Z"}`},
		{[]string{"set:ok=true"}, `{"host":"a","level":"error","msg":"oi","ok":true,"time":"2026-01-01T00:00:00Z"}`},
		{[]string{`set:s="a b"`}, `{"host":"a","level":"error","msg":"oi","s":"a b","time":"2026-01-01T00:00:00Z"}`},
		{[]string{"set:z=null"}, `{"host":"a","level":"error","msg":"oi","time":"2026-01-01T00:00:00Z","z":null}`},
		{[]string{"rename:msg=m", "rename:m=msg2", "delete:level"}, `{"host":"a","msg2":"oi","time":"2026-01-01T00:00:00Z"}`},
	}
	for _, tt := range tests {
		e, _ := event.Parse([]byte(in))
		m, err := New(Config{Ops: tt.ops})
		if err != nil {
			t.Fatalf("%v: %v", tt.ops, err)
		}
		var got []byte
		_ = m.Process(e, func(out event.Event) { got, _ = out.AppendJSON(nil) })
		if string(got) != tt.want {
			t.Errorf("%v:\ngot  %s\nwant %s", tt.ops, got, tt.want)
		}
	}
}

func TestNew_Errors(t *testing.T) {
	for _, op := range []string{"", "rename", "rename:a", "rename:=b", "rename:a=", "frob:a=b",
		"delete:", "delete:time", "rename:time=t", "rename:a=time", "set:time=1", "copy:a=time"} {
		if _, err := New(Config{Ops: []string{op}}); err == nil {
			t.Errorf("%q: esperava erro", op)
		}
	}
}

func BenchmarkMap(b *testing.B) {
	e, _ := event.Parse([]byte(`{"time":"2026-01-01T00:00:00Z","msg":"oi","level":"error","host":"a"}`))
	m, _ := New(Config{Ops: []string{"rename:msg=message"}})
	emit := func(event.Event) {}
	b.ReportAllocs()
	for b.Loop() {
		_ = m.Process(e, emit)
	}
}
