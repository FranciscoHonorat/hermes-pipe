// Command hermes-pipe processa streams de eventos JSON Lines.
//
//	hermes-pipe [flags] < in.jsonl > ok.jsonl 2> bad.jsonl
//	hermes-pipe run -c pipeline.yaml [flags]
//	hermes-pipe version
//
// stdout recebe os eventos válidos; stderr recebe, em JSON Lines, os eventos
// rejeitados e, no fim, uma linha {"summary":{...}}. Logs operacionais (slog)
// também vão para o stderr, em JSON, só a partir de WARN (ou INFO com -v).
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"runtime"
	"runtime/debug"
	"strings"
	"syscall"
	"time"

	"github.com/FranciscoHonorat/hermes-pipe/internal/config"
	"github.com/FranciscoHonorat/hermes-pipe/internal/pipeline"
)

// Códigos de saída (ADR 0005).
const (
	exitOK          = 0   // tudo certo, nenhum evento rejeitado
	exitEventErrors = 1   // terminou, mas houve eventos rejeitados
	exitUsage       = 2   // flag ou config inválida: nenhum evento foi lido
	exitFatal       = 3   // erro fatal durante a execução
	exitInterrupted = 130 // Ctrl+C (128 + SIGINT)
)

var errInterrupted = errors.New("interrompido (SIGINT)")

func main() {
	sig := make(chan os.Signal, 2)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	os.Exit(run(os.Args[1:], os.Stdin, os.Stdout, os.Stderr, sig))
}

type stringList []string

func (s *stringList) String() string     { return strings.Join(*s, ", ") }
func (s *stringList) Set(v string) error { *s = append(*s, v); return nil }

type common struct {
	stats    bool
	quiet    bool
	verbose  bool
	buffer   int
	workers  int
	maxLine  int
	idle     time.Duration
	setFlags map[string]bool
}

func (c *common) register(fs *flag.FlagSet) {
	fs.BoolVar(&c.stats, "stats", false, "inclui tempo, throughput e configuração no resumo")
	fs.BoolVar(&c.quiet, "quiet", false, "não imprime o resumo no stderr")
	fs.BoolVar(&c.verbose, "v", false, "logs operacionais a partir de INFO")
	fs.IntVar(&c.buffer, "buffer", pipeline.DefaultBuffer, "capacidade dos channels entre stages")
	fs.IntVar(&c.workers, "workers", 1, "goroutines por stage sem estado (filter, map)")
	fs.IntVar(&c.maxLine, "max-line", 0, "tamanho máximo de linha em bytes (0 = 1 MiB)")
	fs.DurationVar(&c.idle, "idle-flush", 0, "fecha janelas após esse tempo sem eventos (0 = desligado)")
}

const usageText = `uso:
  hermes-pipe [flags] < entrada.jsonl > ok.jsonl 2> erros.jsonl
  hermes-pipe run -c pipeline.yaml [flags]
  hermes-pipe schema
  hermes-pipe version

Cada linha da entrada precisa ser um objeto JSON com "time" em RFC 3339.
Linhas válidas vão para o stdout; rejeitadas vão para o stderr, em JSON,
seguidas de uma linha {"summary":{...}}.

Códigos de saída: 0 ok, 1 houve eventos rejeitados, 2 uso ou config
inválida, 3 erro fatal, 130 interrompido.

Ctrl+C uma vez: para de ler, drena e imprime o resumo. Duas vezes: para já.

flags:
`

func run(args []string, stdin io.Reader, stdout, stderr io.Writer, sig <-chan os.Signal) int {
	if len(args) > 0 {
		switch args[0] {
		case "version":
			fmt.Fprintln(stdout, versionString())
			return exitOK
		case "run":
			return runConfig(args[1:], stdin, stdout, stderr, sig)
		case "schema":
			fmt.Fprint(stdout, config.Schema())
			return exitOK
		}
	}

	fs := flag.NewFlagSet("hermes-pipe", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var c common
	c.register(fs)
	var filters, maps stringList
	var limitN int64
	var showVersion bool
	fs.Var(&filters, "filter", "expressão campo OP valor (repetível, E lógico)")
	fs.Var(&maps, "map", "rename:a=b, copy:a=b, set:a=v ou delete:a (repetível)")
	fs.Int64Var(&limitN, "limit", 0, "para depois de N eventos emitidos (0 = sem limite)")
	fs.BoolVar(&showVersion, "version", false, "imprime a versão")
	fs.Usage = func() { fmt.Fprint(fs.Output(), usageText); fs.PrintDefaults() }
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}
		return exitUsage
	}
	if showVersion {
		fmt.Fprintln(stdout, versionString())
		return exitOK
	}
	if fs.NArg() > 0 {
		fmt.Fprintf(stderr, "hermes-pipe: argumento inesperado %q\n", fs.Arg(0))
		return exitUsage
	}
	c.setFlags = visited(fs)
	p, err := config.FromFlags(config.Flags{Filters: filters, Maps: maps, Limit: limitN})
	if err != nil {
		fmt.Fprintf(stderr, "hermes-pipe: %v\n", err)
		return exitUsage
	}
	return execute(p, &c, stdin, stdout, stderr, sig)
}

func runConfig(args []string, stdin io.Reader, stdout, stderr io.Writer, sig <-chan os.Signal) int {
	fs := flag.NewFlagSet("hermes-pipe run", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var c common
	c.register(fs)
	var path string
	fs.StringVar(&path, "c", "", "arquivo YAML do pipeline (obrigatório)")
	fs.Usage = func() { fmt.Fprint(fs.Output(), usageText); fs.PrintDefaults() }
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}
		return exitUsage
	}
	if path == "" || fs.NArg() > 0 {
		fmt.Fprintln(stderr, "hermes-pipe run: use -c pipeline.yaml")
		return exitUsage
	}
	data, err := os.ReadFile(path) //nolint:gosec // o arquivo de config é o que o usuário passou em -c
	if err != nil {
		fmt.Fprintf(stderr, "hermes-pipe: %v\n", err)
		return exitUsage
	}
	p, err := config.Load(path, data)
	if err != nil {
		fmt.Fprintf(stderr, "hermes-pipe: %v\n", err)
		return exitUsage
	}
	c.setFlags = visited(fs)
	// Flags explícitas vencem o YAML.
	if !c.setFlags["buffer"] && p.Buffer >= 0 {
		c.buffer = p.Buffer
	}
	if !c.setFlags["workers"] {
		c.workers = p.Workers
	}
	if !c.setFlags["max-line"] {
		c.maxLine = p.MaxLine
	}
	if !c.setFlags["idle-flush"] {
		c.idle = p.IdleFlush
	}
	return execute(p, &c, stdin, stdout, stderr, sig)
}

func visited(fs *flag.FlagSet) map[string]bool {
	m := map[string]bool{}
	fs.Visit(func(f *flag.Flag) { m[f.Name] = true })
	return m
}

func execute(p *config.Pipeline, c *common, stdin io.Reader, stdout, stderr io.Writer, sig <-chan os.Signal) int {
	level := slog.LevelWarn
	if c.verbose {
		level = slog.LevelInfo
	}
	log := slog.New(slog.NewJSONHandler(stderr, &slog.HandlerOptions{Level: level}))
	if c.buffer < 0 || c.workers < 1 || c.maxLine < 0 || c.idle < 0 {
		fmt.Fprintln(stderr, "hermes-pipe: buffer, max-line e idle-flush >= 0; workers >= 1")
		return exitUsage
	}

	stages, err := p.Stages()
	if err != nil {
		fmt.Fprintf(stderr, "hermes-pipe: %v\n", err)
		return exitUsage
	}

	var src pipeline.Producer
	switch p.Input {
	case "", "-":
		// stdin pode bloquear para sempre (terminal parado): a leitura mora
		// numa goroutine própria para que Ctrl+C funcione (ADR 0005).
		src = pipeline.NewAsyncProducer(pipeline.NewReaderProducer(stdin, c.maxLine))
	default:
		f, err := os.Open(p.Input) //nolint:gosec // abrir o arquivo que o usuário pediu é a função do CLI
		if err != nil {
			fmt.Fprintf(stderr, "hermes-pipe: %v\n", err)
			return exitUsage
		}
		defer func() { _ = f.Close() }() // só leitura: o erro do Close não importa
		src = pipeline.NewReaderProducer(f, c.maxLine)
	}

	// 1º sinal: parada suave. 2º: parada dura.
	soft, softCancel := context.WithCancelCause(context.Background())
	hard, hardCancel := context.WithCancelCause(context.Background())
	defer softCancel(nil)
	defer hardCancel(nil)
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-sig:
			log.Warn("interrompido: drenando; Ctrl+C de novo para parar já")
			softCancel(errInterrupted)
		case <-done:
			return
		}
		select {
		case <-sig:
			hardCancel(errInterrupted)
		case <-done:
		}
	}()

	log.Info("pipeline montado", "stages", strings.Join(p.StageTypes(), ","), "buffer", c.buffer, "workers", c.workers)
	errs := pipeline.NewWriterErrorSink(stderr)
	st, runErr := pipeline.Run(hard, src, stages, pipeline.NewWriterConsumer(stdout),
		pipeline.WithBuffer(c.buffer),
		pipeline.WithWorkers(c.workers),
		pipeline.WithErrors(errs),
		pipeline.WithStop(soft),
		pipeline.WithIdleFlush(c.idle),
	)
	if runErr != nil {
		log.Error("falha", "err", runErr.Error())
	}
	if !c.quiet {
		writeSummary(stderr, st, c, runErr)
	}

	switch {
	case runErr != nil && errors.Is(runErr, errInterrupted):
		return exitInterrupted
	case runErr != nil:
		return exitFatal
	case errors.Is(st.Stopped, errInterrupted):
		return exitInterrupted
	case st.Errored > 0:
		return exitEventErrors
	}
	return exitOK
}

type summary struct {
	Read         int64   `json:"read"`
	Emitted      int64   `json:"emitted"`
	Dropped      int64   `json:"dropped"`
	Errored      int64   `json:"errored"`
	Generated    int64   `json:"generated"`
	Stopped      string  `json:"stopped,omitempty"`
	Error        string  `json:"error,omitempty"`
	ElapsedMS    float64 `json:"elapsed_ms,omitempty"`
	EventsPerSec float64 `json:"events_per_sec,omitempty"`
	Buffer       *int    `json:"buffer,omitempty"`
	Workers      int     `json:"workers,omitempty"`
	GOMAXPROCS   int     `json:"gomaxprocs,omitempty"`
}

func writeSummary(w io.Writer, st pipeline.Stats, c *common, runErr error) {
	s := summary{Read: st.Read, Emitted: st.Emitted, Dropped: st.Dropped, Errored: st.Errored, Generated: st.Generated}
	if st.Stopped != nil {
		s.Stopped = st.Stopped.Error()
	}
	if runErr != nil {
		s.Error = runErr.Error()
	}
	if c.stats {
		s.ElapsedMS = float64(st.Elapsed.Microseconds()) / 1000
		if secs := st.Elapsed.Seconds(); secs > 0 {
			s.EventsPerSec = float64(st.Read) / secs
		}
		s.Buffer = &c.buffer
		s.Workers = c.workers
		s.GOMAXPROCS = runtime.GOMAXPROCS(0)
	}
	b, _ := json.Marshal(map[string]summary{"summary": s})
	fmt.Fprintf(w, "%s\n", b)
}

// versionString usa a versão que o go embute no binário (debug.ReadBuildInfo):
// a tag do módulo em go install, ou a pseudo-versão do VCS no build local.
func versionString() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "hermes-pipe (sem build info)"
	}
	v := info.Main.Version
	if v == "" {
		v = "(devel)"
	}
	var rev, modified string
	for _, s := range info.Settings {
		switch s.Key {
		case "vcs.revision":
			rev = s.Value
		case "vcs.modified":
			modified = s.Value
		}
	}
	out := fmt.Sprintf("hermes-pipe %s %s", v, info.GoVersion)
	if rev != "" && v == "(devel)" {
		out += " rev=" + rev[:min(12, len(rev))]
		if modified == "true" {
			out += "+dirty"
		}
	}
	return out
}
