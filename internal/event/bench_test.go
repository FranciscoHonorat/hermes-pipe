package event

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"
)

var benchLine = []byte(`{"time":"2026-01-01T12:00:00.123Z","level":"error","host":"web-03","msg":"upstream timeout","latency_ms":1532,"http":{"method":"GET","status":504}}`)

var sinkEvent Event

// BenchmarkParseEvent mede o parser escolhido (Decoder + UseNumber + map[string]any).
func BenchmarkParseEvent(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		e, err := Parse(benchLine)
		if err != nil {
			b.Fatal(err)
		}
		sinkEvent = e
	}
}

// As alternativas abaixo existem para o ADR 0003: medem o custo de cada
// escolha que o Parse não fez.

func BenchmarkParseAlt_UnmarshalAny(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		var m map[string]any
		if err := json.Unmarshal(benchLine, &m); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkParseAlt_UnmarshalRaw(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		var m map[string]json.RawMessage
		if err := json.Unmarshal(benchLine, &m); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkParseAlt_DecoderToken(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		dec := json.NewDecoder(bytes.NewReader(benchLine))
		dec.UseNumber()
		for {
			if _, err := dec.Token(); err != nil {
				break
			}
		}
	}
}

func BenchmarkTimeAlt_TimeTime(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		if _, err := time.Parse(time.RFC3339Nano, "2026-01-01T12:00:00.123Z"); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkAppendJSON(b *testing.B) {
	e, _ := Parse(benchLine)
	buf := make([]byte, 0, 512)
	b.ReportAllocs()
	for b.Loop() {
		buf, _ = e.AppendJSON(buf[:0])
	}
}
