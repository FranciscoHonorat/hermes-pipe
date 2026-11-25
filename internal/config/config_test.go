package config

import (
	"context"
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/FranciscoHonorat/hermes-pipe/internal/pipeline"
)

var update = flag.Bool("update", false, "reescreve os arquivos golden")

// TestLoad_Golden: cada testdata/config/*.yaml tem um .golden com a mensagem
// de erro (com linha e coluna) ou "ok: <stages>".
func TestLoad_Golden(t *testing.T) {
	files, err := filepath.Glob("testdata/config/*.yaml")
	if err != nil || len(files) < 25 {
		t.Fatalf("esperava >= 25 casos, achou %d (%v)", len(files), err)
	}
	for _, f := range files {
		t.Run(filepath.Base(f), func(t *testing.T) {
			data, err := os.ReadFile(f)
			if err != nil {
				t.Fatal(err)
			}
			var got string
			p, loadErr := Load(filepath.Base(f), data)
			if loadErr != nil {
				got = "erro: " + loadErr.Error() + "\n"
			} else {
				got = "ok: " + strings.Join(p.StageTypes(), " → ") + "\n"
				if _, err := p.Stages(); err != nil {
					t.Fatalf("validou mas não constrói: %v", err)
				}
			}
			golden := strings.TrimSuffix(f, ".yaml") + ".golden"
			if *update {
				if err := os.WriteFile(golden, []byte(got), 0o644); err != nil {
					t.Fatal(err)
				}
			}
			want, err := os.ReadFile(golden)
			if err != nil {
				t.Fatalf("%v (rode com -update)", err)
			}
			if got != string(want) {
				t.Fatalf("got  %swant %s", got, want)
			}
			ok := strings.Contains(filepath.Base(f), "-ok-")
			if ok != (loadErr == nil) {
				t.Fatalf("caso %s: err = %v", f, loadErr)
			}
		})
	}
}

func TestLoad_Settings(t *testing.T) {
	data, _ := os.ReadFile("testdata/config/02-ok-completo.yaml")
	p, err := Load("x", data)
	if err != nil {
		t.Fatal(err)
	}
	if p.Input != "logs.jsonl" || p.Buffer != 16 || p.Workers != 4 || p.IdleFlush.String() != "30s" || p.MaxLine != 4096 {
		t.Fatalf("%+v", p)
	}
}

// TestStages_FreshInstances: cada chamada devolve stages novos (com estado
// zerado).
func TestStages_FreshInstances(t *testing.T) {
	p, err := Load("x", []byte("version: 1\nstages:\n  - type: limit\n    n: 1\n"))
	if err != nil {
		t.Fatal(err)
	}
	a, _ := p.Stages()
	b, _ := p.Stages()
	if a[0] == b[0] {
		t.Fatal("Stages reusou a instância")
	}
}

func TestFromFlags(t *testing.T) {
	p, err := FromFlags(Flags{Filters: []string{"level==error"}, Maps: []string{"rename:a=b"}, Limit: 3})
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(p.StageTypes(), ","); got != "filter,map,limit" {
		t.Fatalf("got %s", got)
	}
	for _, f := range []Flags{{Filters: []string{"x"}}, {Maps: []string{"x"}}, {Limit: -1}} {
		if _, err := FromFlags(f); err == nil {
			t.Errorf("%+v: esperava erro", f)
		}
	}
}

// TestConfig_EndToEnd: o pipeline carregado roda no runner.
func TestConfig_EndToEnd(t *testing.T) {
	p, err := Load("x", []byte(`version: 1
stages:
  - type: filter
    where: ["level == error"]
  - type: aggregate
    by: [host]
`))
	if err != nil {
		t.Fatal(err)
	}
	stages, _ := p.Stages()
	in := `{"time":"2026-01-01T00:00:00Z","level":"error","host":"a"}
{"time":"2026-01-01T00:00:01Z","level":"info","host":"a"}
{"time":"2026-01-01T00:00:02Z","level":"error","host":"a"}
`
	var out strings.Builder
	st, err := pipeline.Run(context.Background(), pipeline.NewReaderProducer(strings.NewReader(in), 0), stages, pipeline.NewWriterConsumer(&out))
	if err != nil {
		t.Fatal(err)
	}
	if out.String() != `{"count":2,"host":"a","time":"2026-01-01T00:00:02Z"}`+"\n" || !st.Conserved() {
		t.Fatalf("out=%q st=%+v", out.String(), st)
	}
}

func TestTypes(t *testing.T) {
	if got := strings.Join(Types(), ","); got != "aggregate,dedup,filter,limit,map,reconcile,triage,window" {
		t.Fatal(got)
	}
}

func FuzzConfig(f *testing.F) {
	files, _ := filepath.Glob("testdata/config/*.yaml")
	for _, file := range files {
		data, _ := os.ReadFile(file)
		f.Add(data)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		p, err := Load("fuzz", data)
		if err != nil {
			if !strings.HasPrefix(err.Error(), "fuzz") {
				t.Fatalf("erro sem o nome do arquivo: %v", err)
			}
			return
		}
		if _, err := p.Stages(); err != nil {
			t.Fatalf("Load aceitou, Stages falhou: %v", err)
		}
	})
}

func TestSchema_CoversRegistry(t *testing.T) {
	for _, typ := range Types() {
		if schemaTypes[typ] == nil {
			t.Errorf("stage %q sem tipo em schemaTypes", typ)
		}
	}
	if !strings.Contains(Schema(), "stage window:\n  size: time.Duration") {
		t.Fatal(Schema())
	}
}
