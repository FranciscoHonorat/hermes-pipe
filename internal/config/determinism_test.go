package config

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/FranciscoHonorat/hermes-pipe/internal/pipeline"
	"github.com/FranciscoHonorat/hermes-pipe/internal/stage"
	"github.com/FranciscoHonorat/hermes-pipe/internal/stage/stagetest"
)

const input = "../../examples/logs.jsonl"

type execution struct {
	out, errs string
	stats     pipeline.Stats
}

func execPipeline(t *testing.T, path string, run func(context.Context, pipeline.Producer, []pipeline.Stage, pipeline.Consumer, ...pipeline.Option) (pipeline.Stats, error), opts ...pipeline.Option) execution {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	p, err := Load(path, data)
	if err != nil {
		t.Fatal(err)
	}
	stages, err := p.Stages()
	if err != nil {
		t.Fatal(err)
	}
	in, err := os.ReadFile(input)
	if err != nil {
		t.Fatal(err)
	}
	var out, errs bytes.Buffer
	st, err := run(context.Background(), pipeline.NewReaderProducer(bytes.NewReader(in), 0), stages,
		pipeline.NewWriterConsumer(&out), append(opts, pipeline.WithErrors(pipeline.NewWriterErrorSink(&errs)))...)
	if err != nil {
		t.Fatal(err)
	}
	if !st.Conserved() {
		t.Fatalf("I1: %+v", st)
	}
	return execution{out.String(), errs.String(), st}
}

// TestPipeline_Determinism: todo pipeline de testdata/pipelines produz, byte
// a byte, o golden — com 1 worker, com N workers, com buffers diferentes, e
// igual ao runner sequencial (I5).
func TestPipeline_Determinism(t *testing.T) {
	files, _ := filepath.Glob("testdata/pipelines/*.yaml")
	if len(files) < 5 {
		t.Fatalf("só %d pipelines", len(files))
	}
	for _, f := range files {
		t.Run(filepath.Base(f), func(t *testing.T) {
			seq := execPipeline(t, f, pipeline.RunSequential)
			got := "--- stdout\n" + seq.out + "--- stderr\n" + seq.errs
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
				t.Fatalf("sequencial difere do golden:\n%s", got)
			}
			for _, workers := range []int{1, 2, 8} {
				for _, buf := range []int{0, 1, 64} {
					for rep := range 3 {
						c := execPipeline(t, f, pipeline.Run, pipeline.WithWorkers(workers), pipeline.WithBuffer(buf))
						if c.out != seq.out || c.errs != seq.errs {
							t.Fatalf("workers=%d buf=%d rep=%d: saída difere do sequencial\n%s\n---\n%s", workers, buf, rep, c.out, seq.out)
						}
					}
				}
			}
		})
	}
}

// TestPipeline_Oracle compara com uma implementação independente, escrita
// sem olhar o pipeline: "quantos erros por host, top 3, com p95".
func TestPipeline_Oracle(t *testing.T) {
	got := execPipeline(t, "testdata/pipelines/erros-por-host.yaml", pipeline.Run, pipeline.WithWorkers(4))

	// --- oráculo: laço direto sobre o arquivo, com encoding/json puro.
	data, _ := os.ReadFile(input)
	type acc struct {
		n    int
		last string
		lat  []float64
	}
	groups := map[string]*acc{}
	for line := range strings.SplitSeq(strings.TrimSpace(string(data)), "\n") {
		var m map[string]any
		if json.Unmarshal([]byte(line), &m) != nil {
			continue
		}
		ts, ok := m["time"].(string)
		if !ok {
			continue
		}
		if _, err := time.Parse(time.RFC3339, ts); err != nil || m["level"] != "error" {
			continue
		}
		h := m["host"].(string)
		if groups[h] == nil {
			groups[h] = &acc{}
		}
		g := groups[h]
		g.n++
		if ts > g.last {
			g.last = ts
		}
		g.lat = append(g.lat, m["latency_ms"].(float64))
	}
	hosts := make([]string, 0, len(groups))
	for h := range groups {
		hosts = append(hosts, h)
	}
	slices.SortFunc(hosts, func(a, b string) int {
		if groups[a].n != groups[b].n {
			return groups[b].n - groups[a].n
		}
		return strings.Compare(a, b)
	})
	var want strings.Builder
	for _, h := range hosts[:min(3, len(hosts))] {
		g := groups[h]
		slices.Sort(g.lat)
		rank := max(int((95*len(g.lat)+99)/100)-1, 0) // nearest-rank, teto inteiro
		fmt.Fprintf(&want, `{"count":%d,"host":%q,"p95_latency_ms":%v,"time":%q}`+"\n", g.n, h, g.lat[rank], g.last)
	}
	if got.out != want.String() {
		t.Fatalf("pipeline difere do oráculo\n--- pipeline\n%s--- oráculo\n%s", got.out, want.String())
	}
}

// sampleStages tem uma config válida de cada tipo registrado.
var sampleStages = map[string]string{
	"filter":    `where: ["level == error"]`,
	"map":       `ops: ["rename:msg=message", "set:x=1"]`,
	"limit":     `n: 3`,
	"aggregate": `by: [host]` + "\n    metrics: [count, \"p50:n\"]\n    max_keys: 2",
	"window":    `size: 10s` + "\n    by: [host]",
	"dedup":     `by: [host]` + "\n    window: 1m",
	"reconcile": "correlate_by: host\n    window: 30s\n    max_keys: 2",
	"triage":    "rules:\n      - name: e\n        where: [\"level == error\"]\n        decision: quarantine\n        reason: x",
}

// TestStage_NoSideEffects roda o contrato de Stage em todo stage registrado:
// um stage novo que entra no registro sem passar no contrato quebra aqui (I4).
func TestStage_NoSideEffects(t *testing.T) {
	for _, typ := range Types() {
		t.Run(typ, func(t *testing.T) {
			cfg, ok := sampleStages[typ]
			if !ok {
				t.Fatalf("stage %q sem config de exemplo em sampleStages", typ)
			}
			yml := fmt.Sprintf("version: 1\nstages:\n  - type: %s\n    %s\n", typ, cfg)
			p, err := Load("contrato", []byte(yml))
			if err != nil {
				t.Fatal(err)
			}
			stagetest.Run(t, func() stage.Stage {
				ss, err := p.Stages()
				if err != nil {
					t.Fatal(err)
				}
				return ss[0]
			})
		})
	}
}
