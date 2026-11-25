package hermespipe

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strings"
	"testing"
)

// invariants é a tabela I1–I9 do roadmap: cada invariante aponta para os
// testes que falham quando ela é violada.
var invariants = []struct {
	id, desc string
	tests    []string
}{
	{"I1", "conservação: lidos + gerados = emitidos + descartados + com_erro", []string{"TestCLI_Conservation", "TestPipeline_Conservation", "TestProperty_Conservation"}},
	{"I2", "sem vazamento: quando Run retorna, nenhuma goroutine vive", []string{"TestPipeline_NoLeak", "TestProperty_NoLeak"}},
	{"I3", "posse: cada channel é fechado uma vez, só por quem envia", []string{"TestPipeline_CloseOnce"}},
	{"I4", "isolamento: stage sem channel, contexto, relógio, I/O; não modifica o Event", []string{"TestStage_NoSideEffects", "TestArchitecture_Rules"}},
	{"I5", "determinismo: mesma entrada → mesma saída, byte a byte", []string{"TestPipeline_Determinism", "TestWindow_Determinism"}},
	{"I6", "memória limitada: agregação, janela e dedup têm teto", []string{"TestAggregate_Bounded", "TestDedup_Bounded"}},
	{"I7", "ordem: eventos saem na ordem de entrada", []string{"TestPipeline_DifferentialVsSequential"}},
	{"I8", "backpressure: consumer lento desacelera o producer", []string{"TestPipeline_Backpressure"}},
	{"I9", "shutdown ordenado: dreno producer → stages → consumer", []string{"TestPipeline_ShutdownGraceful"}},
}

// TestInvariants falha se algum teste da tabela deixar de existir (renomeado
// ou apagado): nenhuma invariante pode ficar sem teste.
func TestInvariants(t *testing.T) {
	defined := map[string]string{}
	fset := token.NewFileSet()
	err := filepath.WalkDir(".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() && (d.Name() == "testdata" || strings.HasPrefix(d.Name(), ".")) && path != "." {
			return filepath.SkipDir
		}
		if d.IsDir() || !strings.HasSuffix(path, "_test.go") {
			return nil
		}
		f, err := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
		if err != nil {
			return err
		}
		for _, decl := range f.Decls {
			if fn, ok := declName(decl); ok && strings.HasPrefix(fn, "Test") {
				defined[fn] = path
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, inv := range invariants {
		if len(inv.tests) == 0 {
			t.Errorf("%s (%s) sem teste", inv.id, inv.desc)
		}
		for _, name := range inv.tests {
			if _, ok := defined[name]; !ok {
				t.Errorf("%s (%s): teste %s não existe", inv.id, inv.desc, name)
			}
		}
	}
}

func declName(d ast.Decl) (string, bool) {
	fn, ok := d.(*ast.FuncDecl)
	if !ok || fn.Recv != nil {
		return "", false
	}
	return fn.Name.Name, true
}
