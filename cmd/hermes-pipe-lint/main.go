// Command hermes-pipe-lint aplica as regras de arquitetura dos stages
// (internal/lint) aos pacotes dados.
//
//	hermes-pipe-lint ./internal/stage/...
//
// Sai com 1 se encontrar violações, 2 se não conseguir carregar os pacotes.
package main

import (
	"fmt"
	"io"
	"os"

	"github.com/FranciscoHonorat/hermes-pipe/internal/lint"
)

func main() { os.Exit(run(os.Args[1:], os.Stdout, os.Stderr)) }

func run(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		args = []string{"./internal/stage/..."}
	}
	pkgs, err := lint.Load(".", args...)
	if err != nil {
		fmt.Fprintf(stderr, "hermes-pipe-lint: %v\n", err)
		return 2
	}
	n := 0
	for _, p := range pkgs {
		for _, d := range lint.Check(p) {
			fmt.Fprintln(stdout, d)
			n++
		}
	}
	if n > 0 {
		fmt.Fprintf(stderr, "hermes-pipe-lint: %d violação(ões)\n", n)
		return 1
	}
	return 0
}
