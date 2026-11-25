package lint

import (
	"fmt"
	"go/ast"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
)

type want struct {
	file string
	line int
	re   *regexp.Regexp
}

var wantRE = regexp.MustCompile(`// want((?:\s+"(?:[^"\\]|\\.)*")+)`)
var quotedRE = regexp.MustCompile(`"(?:[^"\\]|\\.)*"`)

func commentsOf(p *Package) []*ast.Comment {
	var out []*ast.Comment
	for _, f := range p.Files {
		for _, cg := range f.Comments {
			out = append(out, cg.List...)
		}
	}
	return out
}

// analyze roda o analisador num pacote de testdata/src e confere cada
// diagnóstico contra os // want.
func analyze(t *testing.T, name string) {
	t.Helper()
	pkgs, err := Load(".", "./testdata/src/"+name)
	if err != nil {
		t.Fatal(err)
	}
	p := pkgs[0]
	var ws []want
	for _, c := range commentsOf(p) {
		m := wantRE.FindStringSubmatch(c.Text)
		if m == nil {
			continue
		}
		pos := p.Fset.Position(c.Pos())
		for _, q := range quotedRE.FindAllString(m[1], -1) {
			s, err := strconv.Unquote(q)
			if err != nil {
				t.Fatalf("%s: want inválido %s", pos, q)
			}
			ws = append(ws, want{pos.Filename, pos.Line, regexp.MustCompile(s)})
		}
	}
	ds := Check(p)
	used := make([]bool, len(ds))
	for _, w := range ws {
		i := slices.IndexFunc(ds, func(d Diagnostic) bool {
			return d.Pos.Filename == w.file && d.Pos.Line == w.line && w.re.MatchString(d.Msg)
		})
		for i >= 0 && used[i] {
			j := slices.IndexFunc(ds[i+1:], func(d Diagnostic) bool {
				return d.Pos.Filename == w.file && d.Pos.Line == w.line && w.re.MatchString(d.Msg)
			})
			if j < 0 {
				i = -1
				break
			}
			i += 1 + j
		}
		if i < 0 {
			t.Errorf("%s:%d: esperava diagnóstico casando %q", w.file, w.line, w.re)
			continue
		}
		used[i] = true
	}
	for i, d := range ds {
		if !used[i] {
			t.Errorf("diagnóstico inesperado: %s", d)
		}
	}
}

func TestAnalyzer(t *testing.T) {
	for _, name := range []string{"clockbad", "clockgood", "iobad", "iogood", "orderbad", "ordergood"} {
		t.Run(name, func(t *testing.T) { analyze(t, name) })
	}
}

// TestAnalyzer_Aliases: import t "time" e t.Now() só são pegos com tipos.
func TestAnalyzer_Aliases(t *testing.T) { analyze(t, "alias") }

// TestArchitecture_Rules: o código real dos stages passa nas três regras.
func TestArchitecture_Rules(t *testing.T) {
	pkgs, err := Load(".", "../stage/...")
	if err != nil {
		t.Fatal(err)
	}
	if len(pkgs) < 7 {
		t.Fatalf("carregou só %d pacotes de stage", len(pkgs))
	}
	var all []string
	for _, p := range pkgs {
		for _, d := range Check(p) {
			all = append(all, d.String())
		}
	}
	if len(all) > 0 {
		t.Fatalf("violações nos stages:\n%s", strings.Join(all, "\n"))
	}
}

func TestDiagnostic_String(t *testing.T) {
	d := Diagnostic{Rule: "clock", Msg: "x"}
	d.Pos.Filename, d.Pos.Line, d.Pos.Column = "a.go", 3, 4
	if got := fmt.Sprint(d); got != "a.go:3:4: clock: x" {
		t.Fatal(got)
	}
}
