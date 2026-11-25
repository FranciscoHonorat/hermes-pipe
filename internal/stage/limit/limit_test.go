package limit

import (
	"errors"
	"testing"

	"github.com/FranciscoHonorat/hermes-pipe/internal/event"
	"github.com/FranciscoHonorat/hermes-pipe/internal/stage"
	"github.com/FranciscoHonorat/hermes-pipe/internal/stage/stagetest"
)

func TestStageContract(t *testing.T) {
	stagetest.Run(t, func() stage.Stage {
		l, _ := New(Config{N: 2})
		return l
	})
}

func TestLimit(t *testing.T) {
	l, err := New(Config{N: 2})
	if err != nil {
		t.Fatal(err)
	}
	var n int
	emit := func(event.Event) { n++ }
	in := stagetest.Inputs(t)
	if err := l.Process(in[0], emit); err != nil {
		t.Fatalf("1º: %v", err)
	}
	if err := l.Process(in[1], emit); !errors.Is(err, ErrLimitReached) {
		t.Fatalf("2º: err = %v, quer ErrLimitReached", err)
	}
	var s interface{ Stop() bool }
	if !errors.As(ErrLimitReached, &s) || !s.Stop() {
		t.Fatal("ErrLimitReached precisa ser parada suave")
	}
	for _, e := range in[2:] {
		if err := l.Process(e, emit); err != nil {
			t.Fatalf("depois do limite: %v", err)
		}
	}
	if n != 2 {
		t.Fatalf("emitiu %d, quer 2", n)
	}
}

func TestNew_Invalid(t *testing.T) {
	for _, n := range []int64{0, -1} {
		if _, err := New(Config{N: n}); err == nil {
			t.Errorf("n=%d: esperava erro", n)
		}
	}
}
