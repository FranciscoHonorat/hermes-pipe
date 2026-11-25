package event

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"
	"time"
)

func TestParseEvent(t *testing.T) {
	big := `{"time":"2026-01-01T00:00:00Z","msg":"` + strings.Repeat("x", 1<<20) + `"}`
	tests := []struct {
		name    string
		in      string
		wantErr error
		check   func(t *testing.T, e Event)
	}{
		{"mínimo", `{"time":"2026-01-01T00:00:00Z"}`, nil, nil},
		{"nanos e fuso", `{"time":"2026-01-01T03:04:05.123456789-03:00"}`, nil, func(t *testing.T, e Event) {
			if e.Time.Nanosecond() != 123456789 {
				t.Errorf("nanos = %d", e.Time.Nanosecond())
			}
		}},
		{"vazia", ``, ErrEmptyLine, nil},
		{"só espaços", "   \t ", ErrEmptyLine, nil},
		{"CR sozinho", "\r", ErrEmptyLine, nil},
		{"CRLF", "{\"time\":\"2026-01-01T00:00:00Z\"}\r", nil, nil},
		{"objeto vazio", `{}`, ErrMissingTime, nil},
		{"time null", `{"time":null}`, ErrMissingTime, nil},
		{"time número", `{"time":1700000000}`, ErrInvalidTime, nil},
		{"time não RFC3339", `{"time":"ontem"}`, ErrInvalidTime, nil},
		{"time zero", `{"time":"0001-01-01T00:00:00Z"}`, ErrInvalidTime, nil},
		{"UTF-8 inválido", "{\"time\":\"2026-01-01T00:00:00Z\",\"m\":\"\xff\"}", ErrInvalidUTF8, nil},
		{"array", `[1,2]`, ErrNotObject, nil},
		{"string", `"oi"`, ErrNotObject, nil},
		{"null", `null`, ErrNotObject, nil},
		{"truncado", `{"time":"2026-01-01T00:00:00Z"`, ErrInvalidJSON, nil},
		{"lixo depois", `{"time":"2026-01-01T00:00:00Z"} x`, ErrTrailingData, nil},
		{"dois objetos", `{"time":"2026-01-01T00:00:00Z"}{}`, ErrTrailingData, nil},
		{"número grande preservado", `{"time":"2026-01-01T00:00:00Z","id":12345678901234567890}`, nil, func(t *testing.T, e Event) {
			if e.Fields["id"] != json.Number("12345678901234567890") {
				t.Errorf("id = %#v", e.Fields["id"])
			}
		}},
		{"1 MB", big, nil, func(t *testing.T, e Event) {
			if len(e.Fields["msg"].(string)) != 1<<20 {
				t.Error("msg truncada")
			}
		}},
		{"aninhado", `{"time":"2026-01-01T00:00:00Z","http":{"status":500}}`, nil, func(t *testing.T, e Event) {
			if v, ok := e.Get("http.status"); !ok || v != json.Number("500") {
				t.Errorf("http.status = %v %v", v, ok)
			}
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e, err := Parse([]byte(tt.in))
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, quer %v", err, tt.wantErr)
			}
			if err != nil {
				return
			}
			if err := e.Validate(); err != nil {
				t.Fatalf("Validate: %v", err)
			}
			if tt.check != nil {
				tt.check(t, e)
			}
		})
	}
}

func TestEvent_AppendJSON_Deterministic(t *testing.T) {
	e, err := Parse([]byte(`{"z":1,"time":"2026-01-01T00:00:00Z","a":{"y":2,"b":3}}`))
	if err != nil {
		t.Fatal(err)
	}
	got, err := e.AppendJSON(nil)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"a":{"b":3,"y":2},"time":"2026-01-01T00:00:00Z","z":1}`
	if string(got) != want {
		t.Fatalf("got %s\nwant %s", got, want)
	}
}

func TestEvent_CloneIsDeep(t *testing.T) {
	e, _ := Parse([]byte(`{"time":"2026-01-01T00:00:00Z","a":{"b":[1,{"c":2}]}}`))
	c := e.Clone()
	c.Fields["a"].(map[string]any)["b"].([]any)[1].(map[string]any)["c"] = "mudou"
	before, _ := e.AppendJSON(nil)
	if strings.Contains(string(before), "mudou") {
		t.Fatal("Clone compartilha estado com o original")
	}
}

func TestNew(t *testing.T) {
	ts := time.Date(2026, 1, 2, 3, 4, 5, 0, time.FixedZone("x", -3*3600))
	e := New(ts, map[string]any{"k": "v"})
	if err := e.Validate(); err != nil {
		t.Fatal(err)
	}
	if e.Fields["time"] != "2026-01-02T06:04:05Z" {
		t.Fatalf("time = %v", e.Fields["time"])
	}
}

func TestErrorRecord_JSON(t *testing.T) {
	r := ErrorRecord{Seq: 3, Stage: "parse", Err: ErrMissingTime, Raw: []byte(`{}`)}
	got, err := r.AppendJSON(nil)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"line":3,"stage":"parse","error":"campo \"time\" ausente","raw":"{}"}`
	if string(got) != want {
		t.Fatalf("got %s\nwant %s", got, want)
	}
}

func TestLineReader(t *testing.T) {
	in := "a\n\nbbbbbbbbbbbb\nc\r\nd"
	lr := NewLineReader(strings.NewReader(in), 5)
	type res struct {
		seq  uint64
		line string
		err  error
	}
	want := []res{{1, "a", nil}, {2, "", nil}, {3, "", ErrLineTooLong}, {4, "c\r", nil}, {5, "d", nil}, {0, "", io.EOF}}
	for i, w := range want {
		seq, line, err := lr.Next()
		if seq != w.seq || string(line) != w.line || !errors.Is(err, w.err) {
			t.Fatalf("#%d: got (%d,%q,%v) want (%d,%q,%v)", i, seq, line, err, w.seq, w.line, w.err)
		}
	}
}

func TestLineReader_LongLastLineWithoutNewline(t *testing.T) {
	lr := NewLineReader(strings.NewReader("123456"), 5)
	if _, _, err := lr.Next(); !errors.Is(err, ErrLineTooLong) {
		t.Fatalf("err = %v", err)
	}
	if _, _, err := lr.Next(); !errors.Is(err, io.EOF) {
		t.Fatalf("err = %v", err)
	}
}

func TestLineReader_BiggerThanBufio(t *testing.T) {
	line := strings.Repeat("x", 200_000)
	lr := NewLineReader(strings.NewReader(line+"\nok\n"), 300_000)
	_, got, err := lr.Next()
	if err != nil || len(got) != len(line) {
		t.Fatalf("len=%d err=%v", len(got), err)
	}
	_, got, _ = lr.Next()
	if string(got) != "ok" {
		t.Fatalf("got %q", got)
	}
}

func FuzzParseEvent(f *testing.F) {
	for _, s := range []string{`{"time":"2026-01-01T00:00:00Z"}`, `{}`, `[]`, `{"time":null}`, "\xff", `{"time":"2026-01-01T00:00:00Z"} {}`} {
		f.Add([]byte(s))
	}
	f.Fuzz(func(t *testing.T, line []byte) {
		orig := bytes.Clone(line)
		e, err := Parse(line)
		if !bytes.Equal(orig, line) {
			t.Fatal("Parse modificou a entrada")
		}
		if err != nil {
			return
		}
		if err := e.Validate(); err != nil {
			t.Fatalf("Parse aceitou evento inválido: %v", err)
		}
		// Ida e volta: o JSON emitido é parseável e estável.
		out, err := e.AppendJSON(nil)
		if err != nil {
			t.Fatal(err)
		}
		e2, err := Parse(out)
		if err != nil {
			t.Fatalf("saída não reparseia: %v\n%s", err, out)
		}
		out2, _ := e2.AppendJSON(nil)
		if !bytes.Equal(out, out2) {
			t.Fatalf("não estável:\n%s\n%s", out, out2)
		}
	})
}

func FuzzLineReader(f *testing.F) {
	f.Add([]byte("a\nb\r\n\nccc"), 2)
	f.Fuzz(func(t *testing.T, in []byte, maxLine int) {
		if maxLine <= 0 || maxLine > 1<<16 {
			return
		}
		lr := NewLineReader(bytes.NewReader(in), maxLine)
		var rebuilt int
		var last uint64
		for {
			seq, line, err := lr.Next()
			if errors.Is(err, io.EOF) {
				break
			}
			if seq != last+1 {
				t.Fatalf("seq %d depois de %d", seq, last)
			}
			last = seq
			if err == nil && len(line) > maxLine {
				t.Fatalf("linha de %d > max %d", len(line), maxLine)
			}
			rebuilt++
		}
		if want := bytes.Count(in, []byte("\n")); len(in) > 0 && in[len(in)-1] != '\n' {
			if rebuilt != want+1 {
				t.Fatalf("linhas = %d, quer %d", rebuilt, want+1)
			}
		} else if rebuilt != want {
			t.Fatalf("linhas = %d, quer %d", rebuilt, want)
		}
	})
}
