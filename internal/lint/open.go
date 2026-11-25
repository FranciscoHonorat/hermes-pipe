package lint

import (
	"io"
	"os"
)

// openFile abre um arquivo de export que o próprio go list indicou.
func openFile(path string) (io.ReadCloser, error) {
	return os.Open(path) //nolint:gosec // caminho vem do go list -export, não de entrada externa
}
