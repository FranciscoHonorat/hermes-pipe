package event

import (
	"bufio"
	"errors"
	"io"
)

// DefaultMaxLine é o tamanho máximo padrão de uma linha (1 MiB).
const DefaultMaxLine = 1 << 20

// LineReader lê linhas de um io.Reader com tamanho máximo.
//
// Diferente de bufio.Scanner, uma linha grande demais não é fatal: Next
// devolve ErrLineTooLong para aquela linha, descarta o resto dela e segue.
type LineReader struct {
	r   *bufio.Reader
	max int
	seq uint64
	buf []byte
}

// NewLineReader cria um LineReader. max <= 0 usa DefaultMaxLine.
func NewLineReader(r io.Reader, maxLine int) *LineReader {
	if maxLine <= 0 {
		maxLine = DefaultMaxLine
	}
	return &LineReader{r: bufio.NewReaderSize(r, min(maxLine, 64<<10)), max: maxLine}
}

// Next devolve o número da próxima linha (a partir de 1) e o conteúdo dela,
// sem o "\n". O slice só vale até a próxima chamada. No fim do stream devolve
// io.EOF. Uma última linha sem "\n" é devolvida normalmente.
func (lr *LineReader) Next() (seq uint64, line []byte, err error) {
	lr.buf = lr.buf[:0]
	tooLong := false
	for {
		chunk, err := lr.r.ReadSlice('\n')
		if !tooLong {
			if len(lr.buf)+len(chunk) > lr.max+1 { // +1 pelo '\n'
				tooLong = true
				lr.buf = lr.buf[:0]
			} else {
				lr.buf = append(lr.buf, chunk...)
			}
		}
		switch {
		case err == nil:
			lr.seq++
			if tooLong {
				return lr.seq, nil, ErrLineTooLong
			}
			return lr.seq, lr.buf[:len(lr.buf)-1], nil
		case errors.Is(err, bufio.ErrBufferFull):
			continue
		case errors.Is(err, io.EOF):
			if len(lr.buf) == 0 && !tooLong {
				return 0, nil, io.EOF
			}
			lr.seq++
			if tooLong || len(lr.buf) > lr.max {
				return lr.seq, nil, ErrLineTooLong
			}
			return lr.seq, lr.buf, nil
		default:
			return 0, nil, err
		}
	}
}
