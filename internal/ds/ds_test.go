package ds

import (
	"fmt"
	"slices"
	"testing"
)

func TestBounded_LRU(t *testing.T) {
	b := NewBounded[string, int](2)
	b.Put("a", 1)
	b.Put("b", 2)
	b.Get("a") // a fica quente
	k, v, ev := b.Put("c", 3)
	if !ev || k != "b" || v != 2 {
		t.Fatalf("despejou (%q,%d,%v), quer b", k, v, ev)
	}
	if _, ok := b.Peek("a"); !ok {
		t.Fatal("chave quente foi despejada")
	}
	if b.Len() != 2 {
		t.Fatalf("len = %d", b.Len())
	}
	var keys []string
	for k := range b.All() {
		keys = append(keys, k)
	}
	if !slices.Equal(keys, []string{"a", "c"}) {
		t.Fatalf("ordem = %v", keys)
	}
}

func TestBounded_PeekDoesNotTouch(t *testing.T) {
	b := NewBounded[int, int](2)
	b.Put(1, 1)
	b.Put(2, 2)
	b.Peek(1)
	if k, _, _ := b.Put(3, 3); k != 1 {
		t.Fatalf("despejou %d, quer 1", k)
	}
}

func TestBounded_UpdateAndDelete(t *testing.T) {
	b := NewBounded[int, string](3)
	b.Put(1, "x")
	b.Put(1, "y")
	if v, _ := b.Get(1); v != "y" || b.Len() != 1 {
		t.Fatalf("v=%q len=%d", v, b.Len())
	}
	b.Delete(1)
	b.Delete(42)
	if _, _, ok := b.Oldest(); ok {
		t.Fatal("deveria estar vazio")
	}
	if _, _, ok := b.RemoveOldest(); ok {
		t.Fatal("deveria estar vazio")
	}
}

func TestBounded_NeverExceedsCap(t *testing.T) {
	b := NewBounded[int, int](100)
	for i := range 10_000 {
		b.Put(i%377, i)
		if b.Len() > 100 {
			t.Fatalf("len = %d", b.Len())
		}
	}
}

// testRingContract vale para qualquer T.
func testRingContract[T any](t *testing.T, gen func(int) T, eq func(a, b T) bool) {
	t.Helper()
	r := NewRing[T](3)
	if _, ok := r.PopFront(); ok {
		t.Fatal("pop em vazio")
	}
	for i := range 3 {
		if !r.PushBack(gen(i)) {
			t.Fatalf("push %d", i)
		}
	}
	if r.PushBack(gen(9)) || !r.Full() {
		t.Fatal("push em cheio")
	}
	v, _ := r.PopFront()
	if !eq(v, gen(0)) {
		t.Fatal("FIFO")
	}
	r.PushBack(gen(3)) // dá a volta no slice
	var got []T
	for x := range r.All() {
		got = append(got, x)
	}
	want := []T{gen(1), gen(2), gen(3)}
	if !slices.EqualFunc(got, want, eq) {
		t.Fatal("All fora de ordem")
	}
	if !eq(r.At(2), gen(3)) || r.Len() != 3 || r.Cap() != 3 {
		t.Fatal("At/Len/Cap")
	}
	// break no range: o iterador respeita o false do yield.
	n := 0
	for range r.All() {
		n++
		break
	}
	if n != 1 {
		t.Fatalf("break ignorado: %d", n)
	}
}

type ev struct{ s string }

func TestRingContract(t *testing.T) {
	t.Run("int", func(t *testing.T) {
		testRingContract(t, func(i int) int { return i }, func(a, b int) bool { return a == b })
	})
	t.Run("string", func(t *testing.T) {
		testRingContract(t, func(i int) string { return fmt.Sprint(i) }, func(a, b string) bool { return a == b })
	})
	t.Run("*ev", func(t *testing.T) {
		ps := map[int]*ev{}
		gen := func(i int) *ev {
			if ps[i] == nil {
				ps[i] = &ev{fmt.Sprint(i)}
			}
			return ps[i]
		}
		testRingContract(t, gen, func(a, b *ev) bool { return a == b })
	})
}

func TestBounded_AllBreak(t *testing.T) {
	b := NewBounded[int, int](10)
	for i := range 5 {
		b.Put(i, i)
	}
	n := 0
	for range b.All() {
		n++
		if n == 2 {
			break
		}
	}
	if n != 2 {
		t.Fatalf("n = %d", n)
	}
}

// RingOfInt é a versão concreta, para comparar com Ring[int] (ADR 0011).
type RingOfInt struct {
	buf        []int
	head, size int
}

func (r *RingOfInt) PushBack(v int) bool {
	if r.size == len(r.buf) {
		return false
	}
	r.buf[(r.head+r.size)%len(r.buf)] = v
	r.size++
	return true
}

func (r *RingOfInt) PopFront() (int, bool) {
	if r.size == 0 {
		return 0, false
	}
	v := r.buf[r.head]
	r.head = (r.head + 1) % len(r.buf)
	r.size--
	return v, true
}

var sinkInt int

func BenchmarkRing(b *testing.B) {
	b.Run("generic-int", func(b *testing.B) {
		r := NewRing[int](1024)
		b.ReportAllocs()
		for b.Loop() {
			r.PushBack(1)
			sinkInt, _ = r.PopFront()
		}
	})
	b.Run("concrete-int", func(b *testing.B) {
		r := &RingOfInt{buf: make([]int, 1024)}
		b.ReportAllocs()
		for b.Loop() {
			r.PushBack(1)
			sinkInt, _ = r.PopFront()
		}
	})
	b.Run("generic-ptr", func(b *testing.B) {
		r := NewRing[*ev](1024)
		p := &ev{}
		b.ReportAllocs()
		for b.Loop() {
			r.PushBack(p)
			p, _ = r.PopFront()
		}
	})
}

func BenchmarkBounded(b *testing.B) {
	for _, n := range []int{10, 1000, 100_000} {
		b.Run(fmt.Sprint(n), func(b *testing.B) {
			bd := NewBounded[int, int](n)
			i := 0
			b.ReportAllocs()
			for b.Loop() {
				if _, ok := bd.Get(i % (2 * n)); !ok {
					bd.Put(i%(2*n), i)
				}
				i++
			}
		})
	}
}
