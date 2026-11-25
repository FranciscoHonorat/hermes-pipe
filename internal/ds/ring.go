package ds

import "iter"

// Ring é uma fila de capacidade fixa sobre um slice circular: PushBack e
// PopFront em O(1), sem alocar depois de criada.
type Ring[T any] struct {
	buf        []T
	head, size int
}

// NewRing cria um Ring com capacidade n (> 0).
func NewRing[T any](n int) *Ring[T] {
	if n <= 0 {
		panic("ds: capacidade precisa ser > 0")
	}
	return &Ring[T]{buf: make([]T, n)}
}

// Len devolve o número de elementos.
func (r *Ring[T]) Len() int { return r.size }

// Cap devolve a capacidade.
func (r *Ring[T]) Cap() int { return len(r.buf) }

// Full diz se o Ring está cheio.
func (r *Ring[T]) Full() bool { return r.size == len(r.buf) }

// PushBack acrescenta v no fim. Devolve false (e não faz nada) se está cheio.
func (r *Ring[T]) PushBack(v T) bool {
	if r.Full() {
		return false
	}
	r.buf[(r.head+r.size)%len(r.buf)] = v
	r.size++
	return true
}

// PopFront remove e devolve o primeiro elemento.
func (r *Ring[T]) PopFront() (v T, ok bool) {
	if r.size == 0 {
		return v, false
	}
	v = r.buf[r.head]
	var zero T
	r.buf[r.head] = zero // não segura ponteiros para o GC
	r.head = (r.head + 1) % len(r.buf)
	r.size--
	return v, true
}

// At devolve o i-ésimo elemento a partir do início (0 <= i < Len).
func (r *Ring[T]) At(i int) T {
	if i < 0 || i >= r.size {
		panic("ds: índice fora do Ring")
	}
	return r.buf[(r.head+i)%len(r.buf)]
}

// All percorre do primeiro ao último elemento.
func (r *Ring[T]) All() iter.Seq[T] {
	return func(yield func(T) bool) {
		for i := range r.size {
			if !yield(r.buf[(r.head+i)%len(r.buf)]) {
				return
			}
		}
	}
}
