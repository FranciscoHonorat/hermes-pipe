package pipeline_test

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/FranciscoHonorat/hermes-pipe/internal/event"
	"github.com/FranciscoHonorat/hermes-pipe/internal/pipeline"
)

// TestAsyncProducer_CancelWhileBlocked: com a leitura bloqueada (pipe sem
// escritor, como um terminal parado), Next desiste quando ctx é cancelado.
func TestAsyncProducer_CancelWhileBlocked(t *testing.T) {
	pr, pw := io.Pipe()
	defer func() { _ = pw.Close() }() // termina a goroutine de leitura no fim do teste
	a := pipeline.NewAsyncProducer(pipeline.NewReaderProducer(pr, 0))
	go func() { _, _ = pw.Write([]byte(`{"time":"2026-01-01T00:00:00Z"}` + "\n")) }()

	rec, err := a.Next(context.Background())
	if err != nil || rec.Seq != 1 {
		t.Fatalf("rec=%+v err=%v", rec, err)
	}
	ctx, cancel := context.WithCancelCause(context.Background())
	cause := errors.New("ctrl+c")
	time.AfterFunc(10*time.Millisecond, func() { cancel(cause) })
	if _, err := a.Next(ctx); !errors.Is(err, cause) {
		t.Fatalf("err = %v, quer a causa do cancelamento", err)
	}
}

func TestAsyncProducer_EOFAndLineErrors(t *testing.T) {
	in := "a\n" + strings.Repeat("x", 100) + "\nb"
	a := pipeline.NewAsyncProducer(pipeline.NewReaderProducer(strings.NewReader(in), 10))
	var got []string
	for {
		rec, err := a.Next(context.Background())
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if rec.Err != nil {
			got = append(got, "erro:"+rec.Err.Error())
			continue
		}
		got = append(got, string(rec.Line))
	}
	if strings.Join(got, "|") != "a|erro:"+event.ErrLineTooLong.Error()+"|b" {
		t.Fatalf("got %q", got)
	}
	if _, err := a.Next(context.Background()); !errors.Is(err, io.EOF) {
		t.Fatal("EOF não é estável")
	}
}

func TestReaderProducer_CanceledContext(t *testing.T) {
	p := pipeline.NewReaderProducer(strings.NewReader("x\n"), 0)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := p.Next(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v", err)
	}
}

// fakeClock dispara quando o teste manda.
type fakeClock struct{ tick chan time.Time }

func (c fakeClock) After(time.Duration) <-chan time.Time { return c.tick }

func TestWithClock_IdleUsesInjectedClock(t *testing.T) {
	clk := fakeClock{tick: make(chan time.Time)}
	p := &stalled{lines: lines(1, 0), release: make(chan struct{})}
	c := &bufConsumer{}
	counters := &pipeline.Counters{}
	done := make(chan error)
	go func() {
		_, err := pipeline.Run(context.Background(), p, []pipeline.Stage{&idler{}}, c,
			pipeline.WithClock(clk), pipeline.WithIdleFlush(time.Hour), pipeline.WithCounters(counters))
		done <- err
	}()
	clk.tick <- time.Time{} // um "tick" sem esperar uma hora
	clk.tick <- time.Time{}
	close(p.release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if got := counters.Snapshot().Generated; got < 1 {
		t.Fatalf("idle não rodou com o relógio injetado: generated=%d", got)
	}
}

func TestPanicError_Message(t *testing.T) {
	e := &pipeline.PanicError{Stage: "x", Value: "boom"}
	if e.Error() != "pânico no stage x: boom" || !e.Fatal() {
		t.Fatal(e.Error())
	}
}
