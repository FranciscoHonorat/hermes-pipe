// Package config transforma um pipeline declarado em YAML (ou em flags) em
// stages prontos para o runner.
//
// É o único pacote com dependência externa (go.yaml.in/yaml/v3, ADR 0002).
// Os stages não conhecem YAML: cada um expõe uma Config em Go puro e um New
// que a valida. O registro abaixo liga o nome do stage ao decodificador
// estrito e ao construtor, e é por isso que config depende de stage e nunca
// o contrário.
package config

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"time"

	"go.yaml.in/yaml/v3"

	"github.com/FranciscoHonorat/hermes-pipe/internal/pipeline"
	"github.com/FranciscoHonorat/hermes-pipe/internal/stage/aggregate"
	"github.com/FranciscoHonorat/hermes-pipe/internal/stage/dedup"
	"github.com/FranciscoHonorat/hermes-pipe/internal/stage/filter"
	"github.com/FranciscoHonorat/hermes-pipe/internal/stage/limit"
	"github.com/FranciscoHonorat/hermes-pipe/internal/stage/mapper"
	"github.com/FranciscoHonorat/hermes-pipe/internal/stage/window"
)

// Version é a única versão de schema aceita.
const Version = 1

// Pipeline é um pipeline validado. Stages devolve instâncias novas a cada
// chamada (stages com estado não podem ser reusados).
type Pipeline struct {
	Input     string        // caminho do arquivo; "" ou "-" = stdin
	Buffer    int           // capacidade dos channels; -1 = padrão do runner
	Workers   int           // >= 1
	IdleFlush time.Duration // 0 = desligado
	MaxLine   int           // 0 = padrão
	makers    []maker
}

type maker struct {
	typ  string
	make func() (pipeline.Stage, error)
}

// Stages constrói os stages.
func (p *Pipeline) Stages() ([]pipeline.Stage, error) {
	out := make([]pipeline.Stage, 0, len(p.makers))
	for _, m := range p.makers {
		s, err := m.make()
		if err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, nil
}

// StageTypes devolve os tipos dos stages, em ordem.
func (p *Pipeline) StageTypes() []string {
	out := make([]string, len(p.makers))
	for i, m := range p.makers {
		out[i] = m.typ
	}
	return out
}

// Error é um erro de config com posição no arquivo.
type Error struct {
	File      string
	Line, Col int
	Msg       string
}

func (e *Error) Error() string {
	if e.Line > 0 {
		return fmt.Sprintf("%s:%d:%d: %s", e.File, e.Line, e.Col, e.Msg)
	}
	return fmt.Sprintf("%s: %s", e.File, e.Msg)
}

type file struct {
	Version   int           `yaml:"version"`
	Input     string        `yaml:"input"`
	Buffer    *int          `yaml:"buffer"`
	Workers   int           `yaml:"workers"`
	IdleFlush time.Duration `yaml:"idle_flush"`
	MaxLine   int           `yaml:"max_line"`
	Stages    []yaml.Node   `yaml:"stages"`
}

// Load lê e valida um pipeline em YAML. name aparece nas mensagens de erro.
// Toda validação acontece aqui, antes do primeiro evento.
func Load(name string, data []byte) (*Pipeline, error) {
	errAt := func(n *yaml.Node, format string, args ...any) error {
		e := &Error{File: name, Msg: fmt.Sprintf(format, args...)}
		if n != nil {
			e.Line, e.Col = n.Line, n.Column
		}
		return e
	}

	var doc yaml.Node
	dec := yaml.NewDecoder(bytes.NewReader(data))
	if err := dec.Decode(&doc); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, &Error{File: name, Msg: "arquivo vazio"}
		}
		line, msg := splitYAMLLine(strings.TrimPrefix(err.Error(), "yaml: "))
		return nil, &Error{File: name, Line: line, Col: 1, Msg: msg}
	}
	if len(doc.Content) != 1 || doc.Content[0].Kind != yaml.MappingNode {
		return nil, errAt(&doc, "o documento precisa ser um mapa (version, stages, ...)")
	}
	root := doc.Content[0]
	var f file
	if err := decodeStrict(root, &f, nil, errAt); err != nil {
		return nil, err
	}
	if f.Version != Version {
		return nil, errAt(valueNode(root, "version", root), "version precisa ser %d", Version)
	}
	if len(f.Stages) == 0 {
		return nil, errAt(valueNode(root, "stages", root), "stages precisa ter pelo menos um stage")
	}
	p := &Pipeline{Input: f.Input, Buffer: -1, Workers: max(f.Workers, 1), IdleFlush: f.IdleFlush, MaxLine: f.MaxLine}
	if f.Buffer != nil {
		if *f.Buffer < 0 {
			return nil, errAt(valueNode(root, "buffer", root), "buffer não pode ser negativo")
		}
		p.Buffer = *f.Buffer
	}
	if f.Workers < 0 || f.MaxLine < 0 || f.IdleFlush < 0 {
		return nil, errAt(root, "workers, max_line e idle_flush não podem ser negativos")
	}

	for i := range f.Stages {
		n := &f.Stages[i]
		if n.Kind != yaml.MappingNode {
			return nil, errAt(n, "cada stage precisa ser um mapa com type")
		}
		typNode := valueNode(n, "type", nil)
		if typNode == nil {
			return nil, errAt(n, "stage sem type")
		}
		reg, ok := registry[typNode.Value]
		if !ok {
			return nil, errAt(typNode, "stage desconhecido %q (conhecidos: %s)", typNode.Value, strings.Join(Types(), ", "))
		}
		mk, err := reg(n, errAt)
		if err != nil {
			return nil, err
		}
		p.makers = append(p.makers, maker{typNode.Value, mk})
	}
	return p, nil
}

type errAtFunc func(n *yaml.Node, format string, args ...any) error

type registration func(n *yaml.Node, errAt errAtFunc) (func() (pipeline.Stage, error), error)

// register cria a entrada do registro para um stage com config C. O
// construtor roda uma vez aqui para validar; depois, uma vez por Stages().
func register[C any, S pipeline.Stage](newStage func(C) (S, error)) registration {
	return func(n *yaml.Node, errAt errAtFunc) (func() (pipeline.Stage, error), error) {
		var c C
		if err := decodeStrict(n, &c, []string{"type"}, errAt); err != nil {
			return nil, err
		}
		if _, err := newStage(c); err != nil {
			return nil, errAt(n, "%v", err)
		}
		return func() (pipeline.Stage, error) { return newStage(c) }, nil
	}
}

var registry = map[string]registration{
	"filter":    register(filter.New),
	"map":       register(mapper.New),
	"limit":     register(limit.New),
	"aggregate": register(aggregate.New),
	"window":    register(window.New),
	"dedup":     register(dedup.New),
}

// schemaTypes liga cada stage ao tipo da Config dele, para Schema.
var schemaTypes = map[string]reflect.Type{
	"filter":    reflect.TypeFor[filter.Config](),
	"map":       reflect.TypeFor[mapper.Config](),
	"limit":     reflect.TypeFor[limit.Config](),
	"aggregate": reflect.TypeFor[aggregate.Config](),
	"window":    reflect.TypeFor[window.Config](),
	"dedup":     reflect.TypeFor[dedup.Config](),
}

// Schema descreve o schema YAML aceito: é parte da API pública (a CLI e o
// YAML), e TestCLI_Stable trava a forma dele num golden.
func Schema() string {
	var b strings.Builder
	describe := func(indent string, t reflect.Type) {
		for f := range t.Fields() {
			name, _, _ := strings.Cut(f.Tag.Get("yaml"), ",")
			if name == "" || name == "-" {
				continue
			}
			typ := f.Type.String()
			if f.Type == reflect.TypeFor[[]yaml.Node]() {
				typ = "[]stage"
			}
			fmt.Fprintf(&b, "%s%s: %s\n", indent, name, strings.TrimPrefix(typ, "*"))
		}
	}
	describe("", reflect.TypeFor[file]())
	for _, typ := range Types() {
		fmt.Fprintf(&b, "stage %s:\n", typ)
		describe("  ", schemaTypes[typ])
	}
	return b.String()
}

// Types devolve os tipos de stage registrados, em ordem.
func Types() []string {
	out := make([]string, 0, len(registry))
	for k := range registry {
		out = append(out, k)
	}
	slices.Sort(out)
	return out
}

// valueNode devolve o nó de valor da chave key num mapa, ou def.
func valueNode(m *yaml.Node, key string, def *yaml.Node) *yaml.Node {
	for i := 0; i+1 < len(m.Content); i += 2 {
		if m.Content[i].Value == key {
			return m.Content[i+1]
		}
	}
	return def
}

// decodeStrict recusa chaves desconhecidas ou repetidas (com posição) e
// depois decodifica. yaml.Node.Decode não tem KnownFields, por isso a
// verificação é feita aqui, comparando com as tags do struct.
func decodeStrict(n *yaml.Node, v any, skip []string, errAt errAtFunc) error {
	known := yamlFields(reflect.TypeOf(v).Elem())
	seen := map[string]bool{}
	for i := 0; i+1 < len(n.Content); i += 2 {
		k := n.Content[i]
		if seen[k.Value] {
			return errAt(k, "campo %q repetido", k.Value)
		}
		seen[k.Value] = true
		if slices.Contains(skip, k.Value) {
			continue
		}
		if !slices.Contains(known, k.Value) {
			return errAt(k, "campo desconhecido %q (campos: %s)", k.Value, strings.Join(known, ", "))
		}
	}
	// Decodifica uma cópia sem as chaves puladas.
	cp := *n
	cp.Content = nil
	for i := 0; i+1 < len(n.Content); i += 2 {
		if !slices.Contains(skip, n.Content[i].Value) {
			cp.Content = append(cp.Content, n.Content[i], n.Content[i+1])
		}
	}
	if err := cp.Decode(v); err != nil {
		var te *yaml.TypeError
		if errors.As(err, &te) && len(te.Errors) > 0 {
			e := errAt(n, "")
			var ce *Error
			if errors.As(e, &ce) {
				line, msg := splitYAMLLine(te.Errors[0])
				if line > 0 {
					ce.Line, ce.Col = line, 1
				}
				ce.Msg = msg
			}
			return e
		}
		return errAt(n, "%v", err)
	}
	return nil
}

// splitYAMLLine separa o "line N: " que a lib YAML põe nas mensagens.
func splitYAMLLine(msg string) (int, string) {
	rest, ok := strings.CutPrefix(msg, "line ")
	if !ok {
		return 0, msg
	}
	num, text, ok := strings.Cut(rest, ": ")
	n, err := strconv.Atoi(num)
	if !ok || err != nil {
		return 0, msg
	}
	return n, text
}

func yamlFields(t reflect.Type) []string {
	var out []string
	for f := range t.Fields() {
		name, _, _ := strings.Cut(f.Tag.Get("yaml"), ",")
		if name != "" && name != "-" {
			out = append(out, name)
		}
	}
	return out
}

// Flags é o pipeline montado pelas flags (atalho da semana 2).
type Flags struct {
	Filters []string
	Maps    []string
	Limit   int64
}

// FromFlags monta o pipeline das flags: filter, depois map, depois limit.
func FromFlags(f Flags) (*Pipeline, error) {
	p := &Pipeline{Buffer: -1, Workers: 1}
	if len(f.Filters) > 0 {
		cfg := filter.Config{Where: f.Filters}
		if _, err := filter.New(cfg); err != nil {
			return nil, err
		}
		p.makers = append(p.makers, maker{"filter", func() (pipeline.Stage, error) { return filter.New(cfg) }})
	}
	if len(f.Maps) > 0 {
		cfg := mapper.Config{Ops: f.Maps}
		if _, err := mapper.New(cfg); err != nil {
			return nil, err
		}
		p.makers = append(p.makers, maker{"map", func() (pipeline.Stage, error) { return mapper.New(cfg) }})
	}
	if f.Limit != 0 {
		cfg := limit.Config{N: f.Limit}
		if _, err := limit.New(cfg); err != nil {
			return nil, err
		}
		p.makers = append(p.makers, maker{"limit", func() (pipeline.Stage, error) { return limit.New(cfg) }})
	}
	return p, nil
}
