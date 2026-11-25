// Package iogood emite por função, sem I/O.
package iogood

import "strings"

type Stage struct{ seen int }

func (s *Stage) Process(e string, emit func(string)) error {
	s.seen++
	var b strings.Builder // strings.Builder não é io.Writer declarado
	b.WriteString(e)
	emit(b.String())
	return nil
}
