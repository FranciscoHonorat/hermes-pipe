package main

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"
)

var update = flag.Bool("update", false, "reescreve os arquivos golden")

// bin é o binário compilado uma vez para todos os testes.
var bin string

func TestMain(m *testing.M) {
	flag.Parse()
	dir, err := os.MkdirTemp("", "hermes-pipe-e2e")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	bin = filepath.Join(dir, "hermes-pipe")
	if out, err := exec.Command("go", "build", "-o", bin, ".").CombinedOutput(); err != nil {
		fmt.Fprintf(os.Stderr, "build: %v\n%s", err, out)
		os.Exit(2)
	}
	code := m.Run()
	_ = os.RemoveAll(dir)
	os.Exit(code)
}

type result struct {
	stdout, stderr string
	exit           int
}

func runBin(t *testing.T, stdin string, args ...string) result {
	t.Helper()
	cmd := exec.Command(bin, args...)
	cmd.Dir = "../.." // caminhos relativos à raiz do repositório
	if stdin != "" {
		f, err := os.Open(filepath.Join("../..", stdin))
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = f.Close() }()
		cmd.Stdin = f
	}
	var out, errb bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errb
	err := cmd.Run()
	var ee *exec.ExitError
	if err != nil && !errors.As(err, &ee) {
		t.Fatal(err)
	}
	return result{out.String(), errb.String(), cmd.ProcessState.ExitCode()}
}

type summaryLine struct {
	Summary struct {
		Read, Emitted, Dropped, Errored, Generated int64
		Stopped                                    string
	}
}

// parseSummary acha a linha de resumo no stderr e confere I1.
func parseSummary(t *testing.T, stderr string) summaryLine {
	t.Helper()
	lines := strings.Split(strings.TrimSpace(stderr), "\n")
	var s summaryLine
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &s); err != nil {
		t.Fatalf("última linha do stderr não é o resumo: %v\n%s", err, stderr)
	}
	x := s.Summary
	if x.Read+x.Generated != x.Emitted+x.Dropped+x.Errored {
		t.Fatalf("I1 violada no resumo: %+v", x)
	}
	return s
}

// normalize troca o resumo de execuções com parada suave (cujo "read"
// depende de quanto o producer leu adiantado) por um marcador estável.
func normalize(t *testing.T, r result) string {
	s := parseSummary(t, r.stderr)
	stderr := r.stderr
	if s.Summary.Stopped != "" {
		lines := strings.Split(strings.TrimSpace(stderr), "\n")
		lines[len(lines)-1] = fmt.Sprintf(`{"summary":<parada suave: %s; emitted=%d>}`, s.Summary.Stopped, s.Summary.Emitted)
		stderr = strings.Join(lines, "\n") + "\n"
	}
	return fmt.Sprintf("exit: %d\n--- stdout\n%s--- stderr\n%s", r.exit, r.stdout, stderr)
}

func golden(t *testing.T, name, got string) {
	t.Helper()
	path := filepath.Join("testdata", "golden", name+".golden")
	if *update {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(got), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("%v (rode com -update)", err)
	}
	if got != string(want) {
		t.Fatalf("difere do golden %s\n--- got\n%s\n--- want\n%s", path, got, want)
	}
}

// TestCLI_EndToEnd: cada caso roda o binário de verdade e compara stdout,
// stderr e código de saída com o golden. Os exemplos do README estão aqui.
func TestCLI_EndToEnd(t *testing.T) {
	tests := []struct {
		name  string
		stdin string
		args  []string
	}{
		{"validar", "examples/logs.jsonl", nil},
		{"filter-map", "examples/logs.jsonl", []string{"--filter", "level==error", "--map", "rename:msg=message"}},
		{"filter-e", "examples/logs.jsonl", []string{"--filter", "level==error", "--filter", "latency_ms>=300"}},
		{"limit", "examples/logs.jsonl", []string{"--limit", "3"}},
		{"workers", "examples/logs.jsonl", []string{"--filter", "level!=info", "--workers", "4", "--buffer", "0"}},
		{"yaml-erros-por-host", "examples/logs.jsonl", []string{"run", "-c", "examples/erros-por-host.yaml"}},
		{"yaml-erros-por-minuto", "examples/logs.jsonl", []string{"run", "-c", "examples/erros-por-minuto.yaml"}},
		{"yaml-limpar", "examples/logs.jsonl", []string{"run", "-c", "examples/limpar.yaml"}},
		{"yaml-quiet", "examples/logs.jsonl", []string{"run", "-c", "examples/erros-por-host.yaml", "--quiet"}},
		{"yaml-industria5", "examples/industria5.jsonl", []string{"run", "-c", "examples/industria5-supervisao.yaml"}},
		{"yaml-reconciliacao", "examples/reconciliacao.jsonl", []string{"run", "-c", "examples/reconciliacao.yaml"}},
		{"yaml-reconciliacao-workers", "examples/reconciliacao.jsonl", []string{"run", "-c", "examples/reconciliacao.yaml", "--workers", "8", "--buffer", "0"}},
		{"yaml-industria5-workers", "examples/industria5.jsonl", []string{"run", "-c", "examples/industria5-supervisao.yaml", "--workers", "8", "--buffer", "0"}},
		{"flag-invalida", "", []string{"--filter", "level"}},
		{"flag-desconhecida", "", []string{"--fitler", "a==b"}},
		{"run-sem-c", "", []string{"run"}},
		{"config-invalida", "", []string{"run", "-c", "internal/config/testdata/config/14-campo-stage-desconhecido.yaml"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := runBin(t, tt.stdin, tt.args...)
			if r.exit == exitUsage {
				golden(t, tt.name, fmt.Sprintf("exit: %d\n--- stdout\n%s--- stderr\n%s", r.exit, r.stdout, r.stderr))
				return
			}
			golden(t, tt.name, normalize(t, r))
		})
	}
}

// TestCLI_Conservation: I1 vale no resumo do binário em toda execução.
func TestCLI_Conservation(t *testing.T) {
	for _, args := range [][]string{
		nil,
		{"--filter", "level==error"},
		{"--limit", "1"},
		{"run", "-c", "examples/erros-por-minuto.yaml"},
		{"run", "-c", "examples/limpar.yaml", "--workers", "8"},
	} {
		r := runBin(t, "examples/logs.jsonl", args...)
		s := parseSummary(t, r.stderr)
		if s.Summary.Errored != 3 && s.Summary.Stopped == "" {
			t.Errorf("%v: errored = %d, quer 3", args, s.Summary.Errored)
		}
	}
}

// TestCLI_SIGINT: com o stdin aberto e parado (como um terminal), o 1º
// Ctrl+C drena e imprime o resumo com I1; o código é 130.
func TestCLI_SIGINT(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("sem SIGINT")
	}
	cmd := exec.Command(bin, "--filter", "level==error")
	stdin, _ := cmd.StdinPipe()
	stdout, _ := cmd.StdoutPipe()
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	for i := range 10 {
		level := "info"
		if i%2 == 0 {
			level = "error"
		}
		fmt.Fprintf(stdin, `{"time":"2026-01-01T00:00:%02dZ","level":%q}`+"\n", i, level)
	}
	// Espera a saída chegar: prova que as linhas foram lidas antes do sinal.
	// A saída é bufferizada até o fim, então esperamos pelo processo ler
	// tudo de forma indireta: um tempo curto, e o resto é drenado no stop.
	time.Sleep(200 * time.Millisecond)
	if err := cmd.Process.Signal(syscall.SIGINT); err != nil {
		t.Fatal(err)
	}
	out, _ := io.ReadAll(bufio.NewReader(stdout))
	err := cmd.Wait()
	var ee *exec.ExitError
	if !errors.As(err, &ee) || ee.ExitCode() != exitInterrupted {
		t.Fatalf("err = %v, quer saída %d\nstderr: %s", err, exitInterrupted, stderr.String())
	}
	s := parseSummary(t, stderr.String())
	if s.Summary.Read != 10 || s.Summary.Emitted != 5 || !strings.Contains(s.Summary.Stopped, "SIGINT") {
		t.Fatalf("resumo = %+v", s.Summary)
	}
	if n := bytes.Count(out, []byte("\n")); n != 5 {
		t.Fatalf("stdout com %d linhas, quer 5", n)
	}
	_ = stdin.Close()
}

// TestCLI_Stable: --help e o schema YAML são a API pública. Mudar qualquer
// um deles muda o golden e aparece no diff do PR.
func TestCLI_Stable(t *testing.T) {
	help := runBin(t, "", "--help")
	golden(t, "help", help.stderr)
	runHelp := runBin(t, "", "run", "--help")
	golden(t, "run-help", runHelp.stderr)
	schema := runBin(t, "", "schema")
	golden(t, "schema", schema.stdout)
}

func TestBinary_Version(t *testing.T) {
	r := runBin(t, "", "version")
	if r.exit != 0 || !strings.HasPrefix(r.stdout, "hermes-pipe ") || !strings.Contains(r.stdout, runtime.Version()) {
		t.Fatalf("version = %q (exit %d)", r.stdout, r.exit)
	}
	if r2 := runBin(t, "", "--version"); r2.stdout != r.stdout {
		t.Fatalf("--version difere de version: %q", r2.stdout)
	}
}

// TestBinary_Reproducible: dois builds com -trimpath, em diretórios de saída
// diferentes, dão o mesmo binário byte a byte.
func TestBinary_Reproducible(t *testing.T) {
	if testing.Short() {
		t.Skip("compila duas vezes")
	}
	hash := func() [32]byte {
		out := filepath.Join(t.TempDir(), "hp")
		cmd := exec.Command("go", "build", "-trimpath", "-o", out, ".")
		cmd.Env = append(os.Environ(), "CGO_ENABLED=0")
		if b, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("%v\n%s", err, b)
		}
		data, err := os.ReadFile(out)
		if err != nil {
			t.Fatal(err)
		}
		return sha256.Sum256(data)
	}
	if a, b := hash(), hash(); a != b {
		t.Fatalf("builds diferentes: %x != %x", a, b)
	}
}
