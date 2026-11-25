package pipeline

import (
	"bufio"
	"bytes"
	"context"
	"io"

	"github.com/FranciscoHonorat/hermes-pipe/internal/event"
)

// ReaderProducer lê JSON Lines de um io.Reader, de forma síncrona.
//
// Next só verifica ctx entre linhas: uma leitura bloqueada não é
// interrompida. Para stdin, envolva com NewAsyncProducer.
type ReaderProducer struct {
	lr *event.LineReader
}

// NewReaderProducer cria um ReaderProducer. maxLine <= 0 usa
// event.DefaultMaxLine.
func NewReaderProducer(r io.Reader, maxLine int) *ReaderProducer {
	return &ReaderProducer{lr: event.NewLineReader(r, maxLine)}
}

// Next implementa Producer.
func (p *ReaderProducer) Next(ctx context.Context) (Record, error) {
	if err := ctx.Err(); err != nil {
		return Record{}, err
	}
	seq, line, err := p.lr.Next()
	switch {
	case err == nil:
		return Record{Seq: seq, Line: line}, nil
	case seq > 0: // erro de uma linha, não do stream
		return Record{Seq: seq, Err: err}, nil
	default:
		return Record{}, err
	}
}

// AsyncProducer lê de outro Producer numa goroutine própria, para que Next
// possa desistir quando ctx é cancelado mesmo com a leitura bloqueada.
//
// Posse: a goroutine de leitura pertence ao AsyncProducer, não ao runner.
// Ela termina no fim do stream (EOF ou erro). Se o stream nunca termina (um
// terminal parado), ela vive até o processo sair: fechar os.Stdin não
// desbloqueia um read(2) em andamento. É a única goroutine que pode
// sobreviver a Run, e por isso só o cmd usa AsyncProducer.
type AsyncProducer struct {
	ch   chan asyncItem
	done bool
	last error
}

type asyncItem struct {
	rec Record
	err error
}

// NewAsyncProducer começa a ler de p imediatamente.
func NewAsyncProducer(p Producer) *AsyncProducer {
	a := &AsyncProducer{ch: make(chan asyncItem, 64)}
	go func() {
		defer close(a.ch)
		for {
			rec, err := p.Next(context.Background())
			rec.Line = bytes.Clone(rec.Line) // o buffer de p é reusado
			a.ch <- asyncItem{rec, err}
			if err != nil {
				return
			}
		}
	}()
	return a
}

// Next implementa Producer.
func (a *AsyncProducer) Next(ctx context.Context) (Record, error) {
	if a.done {
		return Record{}, a.last
	}
	select {
	case it, ok := <-a.ch:
		if !ok {
			a.done, a.last = true, io.EOF
			return Record{}, io.EOF
		}
		if it.err != nil {
			a.done, a.last = true, it.err
		}
		return it.rec, it.err
	case <-ctx.Done():
		return Record{}, context.Cause(ctx)
	}
}

// WriterConsumer escreve cada evento como uma linha JSON.
type WriterConsumer struct {
	w   *bufio.Writer
	buf []byte
}

// NewWriterConsumer cria um WriterConsumer com buffer. Close faz o Flush.
func NewWriterConsumer(w io.Writer) *WriterConsumer {
	return &WriterConsumer{w: bufio.NewWriterSize(w, 64<<10)}
}

// Consume implementa Consumer.
func (c *WriterConsumer) Consume(e event.Event) error {
	var err error
	c.buf, err = e.AppendJSON(c.buf[:0])
	if err != nil {
		return err
	}
	c.buf = append(c.buf, '\n')
	_, err = c.w.Write(c.buf)
	return err
}

// Close implementa Consumer: sem ele, o fim da saída fica no buffer.
func (c *WriterConsumer) Close() error { return c.w.Flush() }

// WriterErrorSink escreve cada ErrorRecord como uma linha JSON.
type WriterErrorSink struct {
	w   *bufio.Writer
	buf []byte
}

// NewWriterErrorSink cria um WriterErrorSink com buffer. Close faz o Flush.
func NewWriterErrorSink(w io.Writer) *WriterErrorSink {
	return &WriterErrorSink{w: bufio.NewWriter(w)}
}

// Report implementa ErrorSink.
func (s *WriterErrorSink) Report(r event.ErrorRecord) error {
	var err error
	s.buf, err = r.AppendJSON(s.buf[:0])
	if err != nil {
		return err
	}
	s.buf = append(s.buf, '\n')
	_, err = s.w.Write(s.buf)
	return err
}

// Close implementa ErrorSink.
func (s *WriterErrorSink) Close() error { return s.w.Flush() }
