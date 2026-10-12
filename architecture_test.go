package hermespipe

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"slices"
	"strings"
	"testing"
)

const module = "github.com/FranciscoHonorat/hermes-pipe"

type pkg struct {
	ImportPath string
	Standard   bool
	Imports    []string
}

var allowed = map[string][]string{
	"cmd":      {"config", "pipeline", "stage", "event"},
	"config":   {"pipeline", "stage", "event"},
	"pipeline": {"event"},
	"stage":    {"stage", "event"},
	"event":    {},
	"root":     {},
}

var allowedExternal = map[string][]string{
	"config": {"go.yaml.in/yaml/v3"},
}

func layerOf(path string) (string, bool) {
	if path == module {
		return "root", true
	}
	rel, ok := strings.CutPrefix(path, module+"/")
	if !ok {
		return "", false
	}
	switch {
	case rel == "cmd/hermes-pipe":
		return "cmd", true
	case rel == "internal/config":
		return "config", true
	case rel == "internal/pipeline":
		return "pipeline", true
	case rel == "internal/event":
		return "event", true
	case rel == "internal/stage", strings.HasPrefix(rel, "internal/stage/"):
		return "stage", true
	}
	return "", false
}

func check(pkgs []pkg) []string {
	std := map[string]bool{}
	for _, p := range pkgs {
		std[p.ImportPath] = p.Standard
	}
	var out []string
	for _, p := range pkgs {
		if p.Standard || !strings.HasPrefix(p.ImportPath, module) {
			continue
		}
		from, ok := layerOf(p.ImportPath)
		if !ok {
			out = append(out, fmt.Sprintf("%s: pacote fora de qualquer camada; classifique em layerOf", p.ImportPath))
			continue
		}
		for _, imp := range p.Imports {
			switch to, internal := layerOf(imp); {
			case std[imp]:
				// stdlib é sempre permitida
			case !internal && strings.HasPrefix(imp, module):
				out = append(out, fmt.Sprintf("%s → %s: destino fora de qualquer camada", p.ImportPath, imp))
			case !internal:
				if !slices.Contains(allowedExternal[from], imp) {
					out = append(out, fmt.Sprintf("%s → %s: dependência externa proibida em %q", p.ImportPath, imp, from))
				}
			case !slices.Contains(allowed[from], to):
				out = append(out, fmt.Sprintf("%s → %s: %q não pode importar %q", p.ImportPath, imp, from, to))
			case from == "stage" && to == "stage" && !strings.HasPrefix(p.ImportPath, imp+"/"):
				out = append(out, fmt.Sprintf("%s → %s: stages não importam uns aos outros", p.ImportPath, imp))
			}
		}
	}
	return out
}

func goList(t *testing.T) []pkg {
	t.Helper()
	out, err := exec.Command("go", "list", "-deps", "-json", "./...").Output()
	if err != nil {
		var ee *exec.ExitError
		if errors.As(err, &ee) {
			t.Fatalf("go list failed: %s\n%s", err, ee.Stderr)
		}
		t.Fatalf("go list failed: %s", err)
	}
	dec := json.NewDecoder(bytes.NewReader(out))
	var pkgs []pkg
	for {
		var p pkg
		if err := dec.Decode(&p); errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			t.Fatalf("failed to decode go list output: %s", err)
		}
		pkgs = append(pkgs, p)
	}
	return pkgs
}

func TestArchitecture_Layers(t *testing.T) {
	pkgs := goList(t)
	if !slices.ContainsFunc(pkgs, func(p pkg) bool { return strings.HasPrefix(p.ImportPath, module) }) {
		t.Fatalf("nenhum pacote do módulo %q encontrado; a constante module bate com o go.mod?", module)
	}
	for _, v := range check(pkgs) {
		t.Errorf("layer violation: %s", v)
	}
}

func TestArchitecture_DetectsViolations(t *testing.T) {
	const (
		event    = module + "/internal/event"
		pipeline = module + "/internal/pipeline"
		config   = module + "/internal/config"
		stage    = module + "/internal/stage"
		filter   = module + "/internal/stage/filter"
		mapper   = module + "/internal/stage/mapper"
	)
	tests := []struct {
		name string
		pkgs []pkg
		want string
	}{
		{"camadas corretas", []pkg{
			{ImportPath: "fmt", Standard: true},
			{ImportPath: event, Imports: []string{"fmt"}},
			{ImportPath: pipeline, Imports: []string{event}},
			{ImportPath: filter, Imports: []string{stage, event}},
			{ImportPath: config, Imports: []string{pipeline, filter, "go.yaml.in/yaml/v3"}},
		}, ""},
		{"seta ao contrário", []pkg{{ImportPath: event, Imports: []string{pipeline}}}, `"event" não pode importar "pipeline"`},
		{"stage importa pipeline", []pkg{{ImportPath: filter, Imports: []string{pipeline}}}, `"stage" não pode importar "pipeline"`},
		{"stage importa config", []pkg{{ImportPath: filter, Imports: []string{config}}}, `"stage" não pode importar "config"`},
		{"stage importa irmão", []pkg{{ImportPath: filter, Imports: []string{mapper}}}, "stages não importam uns aos outros"},
		{"lib externa em event", []pkg{{ImportPath: event, Imports: []string{"github.com/x/y"}}}, "dependência externa proibida"},
		{"pacote sem camada", []pkg{{ImportPath: module + "/internal/novo"}}, "fora de qualquer camada"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := check(tt.pkgs)
			if tt.want == "" {
				if len(got) > 0 {
					t.Fatalf("violações inesperadas: %v", got)
				}
				return
			}
			if !slices.ContainsFunc(got, func(s string) bool { return strings.Contains(s, tt.want) }) {
				t.Fatalf("esperava violação %q, mas não foi encontrada. Violations: %v", tt.want, got)
			}
		})
	}
}
