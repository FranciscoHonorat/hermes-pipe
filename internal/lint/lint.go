// Package lint é o analisador estático do hermes-pipe: impõe sobre os stages
// as regras que o go list não enxerga (I4, I5). Usa só a stdlib: go/parser,
// go/ast, go/types e go/importer.
//
// Regras:
//
//	clock: não chama relógio de parede nem aleatoriedade (time.Now, time.Since,
//	       time.After..., math/rand, math/rand/v2).
//	io:    não declara nem recebe chan, context.Context, *os.File ou io.Writer.
//	order: range sobre map só se a mesma função ordena depois (sort.*,
//	       slices.Sort*), ou com //hermespipe:unordered-ok <motivo>.
//
// As regras usam tipos, não só sintaxe: import t "time" seguido de t.Now()
// é pego, e uma variável local chamada time não confunde o analisador.
package lint

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"go/ast"
	"go/importer"
	"go/parser"
	"go/token"
	"go/types"
	"io"
	"os/exec"
	"path/filepath"
	"slices"
	"sort"
	"strings"
)

// Directive é o comentário que libera um range sobre map.
const Directive = "//hermespipe:unordered-ok"

// Diagnostic é uma violação encontrada.
type Diagnostic struct {
	Pos  token.Position
	Rule string
	Msg  string
}

func (d Diagnostic) String() string {
	return fmt.Sprintf("%s: %s: %s", d.Pos, d.Rule, d.Msg)
}

// Package é um pacote parseado e com tipos.
type Package struct {
	Path  string
	Fset  *token.FileSet
	Files []*ast.File
	Info  *types.Info
}

type listed struct {
	ImportPath string
	Dir        string
	GoFiles    []string
	Export     string
	Standard   bool
	Error      *struct{ Err string }
}

func goList(dir string, args ...string) ([]listed, error) {
	listArgs := append([]string{"list", "-json=ImportPath,Dir,GoFiles,Export,Standard,Error"}, args...)
	cmd := exec.Command("go", listArgs...) //nolint:gosec // os padrões vêm da linha de comando de quem roda o lint
	cmd.Dir = dir
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("go list: %w\n%s", err, stderr.String())
	}
	var pkgs []listed
	dec := json.NewDecoder(bytes.NewReader(out))
	for {
		var p listed
		if err := dec.Decode(&p); errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return nil, err
		}
		pkgs = append(pkgs, p)
	}
	return pkgs, nil
}

// Load carrega os pacotes dos padrões (como em go list), a partir de dir.
// Os pacotes são parseados do código-fonte; as dependências vêm dos dados de
// export que o go list -export produz.
func Load(dir string, patterns ...string) ([]*Package, error) {
	targets, err := goList(dir, patterns...)
	if err != nil {
		return nil, err
	}
	deps, err := goList(dir, append([]string{"-export", "-deps"}, patterns...)...)
	if err != nil {
		return nil, err
	}
	exports := map[string]string{}
	for _, d := range deps {
		if d.Export != "" {
			exports[d.ImportPath] = d.Export
		}
	}
	fset := token.NewFileSet()
	imp := importer.ForCompiler(fset, "gc", func(path string) (io.ReadCloser, error) {
		f, ok := exports[path]
		if !ok {
			return nil, fmt.Errorf("sem dados de export para %q", path)
		}
		return openFile(f)
	})

	var out []*Package
	for _, t := range targets {
		if t.Error != nil {
			return nil, fmt.Errorf("%s: %s", t.ImportPath, t.Error.Err)
		}
		p := &Package{Path: t.ImportPath, Fset: fset, Info: &types.Info{
			Types: map[ast.Expr]types.TypeAndValue{},
			Defs:  map[*ast.Ident]types.Object{},
			Uses:  map[*ast.Ident]types.Object{},
		}}
		for _, name := range t.GoFiles {
			f, err := parser.ParseFile(fset, filepath.Join(t.Dir, name), nil, parser.ParseComments)
			if err != nil {
				return nil, err
			}
			p.Files = append(p.Files, f)
		}
		conf := types.Config{Importer: imp}
		if _, err := conf.Check(t.ImportPath, fset, p.Files, p.Info); err != nil {
			return nil, fmt.Errorf("type check %s: %w", t.ImportPath, err)
		}
		out = append(out, p)
	}
	return out, nil
}

// Check aplica as três regras a um pacote.
func Check(p *Package) []Diagnostic {
	var ds []Diagnostic
	report := func(pos token.Pos, rule, format string, args ...any) {
		ds = append(ds, Diagnostic{Pos: p.Fset.Position(pos), Rule: rule, Msg: fmt.Sprintf(format, args...)})
	}
	for _, f := range p.Files {
		checkClock(p, f, report)
		checkIO(p, f, report)
		checkOrder(p, f, report)
	}
	sort.SliceStable(ds, func(i, j int) bool {
		a, b := ds[i].Pos, ds[j].Pos
		if a.Filename != b.Filename {
			return a.Filename < b.Filename
		}
		if a.Line != b.Line {
			return a.Line < b.Line
		}
		return a.Column < b.Column
	})
	return ds
}

type reporter func(pos token.Pos, rule, format string, args ...any)

var clockFuncs = []string{"Now", "Since", "Until", "After", "AfterFunc", "Tick", "NewTimer", "NewTicker", "Sleep"}

// checkClock: o objeto usado vem do pacote time (e é uma das funções de
// relógio) ou de math/rand. Olhar o objeto (types.Info.Uses), e não o nome
// do identificador, é o que pega aliases de import.
func checkClock(p *Package, f *ast.File, report reporter) {
	ast.Inspect(f, func(n ast.Node) bool {
		sel, ok := n.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		obj := p.Info.Uses[sel.Sel]
		if obj == nil || obj.Pkg() == nil {
			return true
		}
		switch path := obj.Pkg().Path(); {
		case path == "time" && slices.Contains(clockFuncs, obj.Name()):
			// Só funções do pacote: t.After(u) é um método de time.Time, também
			// *types.Func, mas com receptor, e é permitido.
			if fn, isFunc := obj.(*types.Func); isFunc && fn.Signature().Recv() == nil {
				report(sel.Pos(), "clock", "stage não pode usar o relógio de parede (time.%s): use o tempo do evento", obj.Name())
			}
		case path == "math/rand" || path == "math/rand/v2":
			report(sel.Pos(), "clock", "stage não pode usar %s: a saída deixa de ser determinística", path)
		}
		return true
	})
}

// forbiddenType diz se t é (ou contém diretamente) um tipo de I/O ou
// concorrência proibido em stages.
func forbiddenType(t types.Type) string {
	for range 8 { // desembrulha ponteiros, slices, arrays e maps
		switch u := t.(type) {
		case *types.Chan:
			return "chan"
		case *types.Pointer:
			if named(u.Elem(), "os", "File") {
				return "*os.File"
			}
			t = u.Elem()
			continue
		case *types.Slice:
			t = u.Elem()
			continue
		case *types.Array:
			t = u.Elem()
			continue
		case *types.Map:
			if s := forbiddenType(u.Key()); s != "" {
				return s
			}
			t = u.Elem()
			continue
		}
		break
	}
	switch {
	case named(t, "context", "Context"):
		return "context.Context"
	case named(t, "io", "Writer"):
		return "io.Writer"
	case named(t, "os", "File"):
		return "os.File"
	}
	if _, ok := t.Underlying().(*types.Chan); ok {
		return "chan"
	}
	return ""
}

func named(t types.Type, pkg, name string) bool {
	n, ok := types.Unalias(t).(*types.Named)
	if !ok || n.Obj().Pkg() == nil {
		return false
	}
	return n.Obj().Pkg().Path() == pkg && n.Obj().Name() == name
}

// checkIO: toda variável declarada (parâmetro, resultado, campo, local) com
// tipo proibido, e todo make(chan ...).
func checkIO(p *Package, f *ast.File, report reporter) {
	ast.Inspect(f, func(n ast.Node) bool {
		switch n := n.(type) {
		case *ast.Ident:
			v, ok := p.Info.Defs[n].(*types.Var)
			if !ok {
				return true
			}
			if s := forbiddenType(v.Type()); s != "" {
				report(n.Pos(), "io", "stage não pode declarar nem receber %s (%s)", s, n.Name)
			}
		case *ast.FuncType:
			// Parâmetros e resultados sem nome não aparecem em Info.Defs.
			for _, fl := range []*ast.FieldList{n.Params, n.Results} {
				if fl == nil {
					continue
				}
				for _, field := range fl.List {
					if len(field.Names) > 0 {
						continue
					}
					if tv, ok := p.Info.Types[field.Type]; ok {
						if s := forbiddenType(tv.Type); s != "" {
							report(field.Pos(), "io", "stage não pode declarar nem receber %s", s)
						}
					}
				}
			}
		case *ast.CallExpr:
			if id, ok := n.Fun.(*ast.Ident); ok && id.Name == "make" && len(n.Args) > 0 {
				if tv, ok := p.Info.Types[n.Args[0]]; ok {
					if _, isChan := tv.Type.Underlying().(*types.Chan); isChan {
						report(n.Pos(), "io", "stage não pode criar channels")
					}
				}
			}
		}
		return true
	})
}

// sortFuncs são as chamadas que contam como "ordenou as chaves".
var sortFuncs = map[string][]string{
	"sort":   {"Strings", "Ints", "Float64s", "Slice", "SliceStable", "Sort", "Stable"},
	"slices": {"Sort", "SortFunc", "SortStableFunc"},
}

// checkOrder: range sobre map sem ordenação depois, na mesma função. É uma
// heurística (ADR 0008): garante que alguém ordenou algo depois do laço, não
// que ordenou as chaves certas.
func checkOrder(p *Package, f *ast.File, report reporter) {
	directives := map[int]string{} // linha → motivo
	for _, cg := range f.Comments {
		for _, c := range cg.List {
			if rest, ok := strings.CutPrefix(c.Text, Directive); ok {
				line := p.Fset.Position(c.Pos()).Line
				reason := strings.TrimSpace(rest)
				if strings.HasPrefix(reason, "//") {
					reason = "" // outro comentário não é motivo
				}
				directives[line] = reason
				if reason == "" {
					report(c.Pos(), "order", "%s exige um motivo", Directive)
				}
			}
		}
	}
	for _, decl := range f.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Body == nil {
			continue
		}
		var sorts []token.Pos
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			if obj := p.Info.Uses[sel.Sel]; obj != nil && obj.Pkg() != nil && slices.Contains(sortFuncs[obj.Pkg().Path()], obj.Name()) {
				sorts = append(sorts, call.Pos())
			}
			return true
		})
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			rs, ok := n.(*ast.RangeStmt)
			if !ok {
				return true
			}
			tv, ok := p.Info.Types[rs.X]
			if !ok {
				return true
			}
			if _, isMap := tv.Type.Underlying().(*types.Map); !isMap {
				return true
			}
			line := p.Fset.Position(rs.Pos()).Line
			if _, ok := directives[line]; ok {
				return true
			}
			if _, ok := directives[line-1]; ok {
				return true
			}
			if slices.ContainsFunc(sorts, func(s token.Pos) bool { return s > rs.End() }) {
				return true
			}
			report(rs.Pos(), "order", "range sobre map sem ordenar as chaves depois; ordene ou use %s <motivo>", Directive)
			return true
		})
	}
}
