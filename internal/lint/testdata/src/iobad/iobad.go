// Package iobad viola a regra io.
package iobad

import (
	"context"
	"io"
	"os"
)

type Stage struct {
	out io.Writer // want "io.Writer"
	f   *os.File  // want "\\*os.File"
}

func (s *Stage) Process(ctx context.Context, ch chan<- int) { // want "context.Context" "chan"
	done := make(chan struct{}) // want "chan" "criar channels"
	close(done)
	_ = s
}

func Many(ws []io.Writer) {} // want "io.Writer"
