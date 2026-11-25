// Package ds tem as estruturas genéricas com memória limitada usadas pelos
// stages com estado (aggregate, window, dedup). Só importa a stdlib.
package ds

import "iter"

// Bounded é um map com no máximo Cap entradas e despejo LRU.
//
// Get marca a entrada como usada; Peek não. Put numa chave nova com o map
// cheio despeja a entrada menos usada e a devolve. Não é seguro para uso
// concorrente: o estado de um stage mora numa goroutine só.
type Bounded[K comparable, V any] struct {
	limit int
	m     map[K]*entry[K, V]
	root  entry[K, V] // sentinela: root.next é a mais antiga, root.prev a mais nova
}

type entry[K comparable, V any] struct {
	key        K
	val        V
	prev, next *entry[K, V]
}

// NewBounded cria um Bounded com capacidade limit (> 0).
func NewBounded[K comparable, V any](limit int) *Bounded[K, V] {
	if limit <= 0 {
		panic("ds: capacidade precisa ser > 0")
	}
	b := &Bounded[K, V]{limit: limit, m: make(map[K]*entry[K, V])}
	b.root.prev, b.root.next = &b.root, &b.root
	return b
}

// Len devolve o número de entradas.
func (b *Bounded[K, V]) Len() int { return len(b.m) }

// Cap devolve a capacidade.
func (b *Bounded[K, V]) Cap() int { return b.limit }

func (b *Bounded[K, V]) unlink(e *entry[K, V]) {
	e.prev.next, e.next.prev = e.next, e.prev
}

func (b *Bounded[K, V]) pushNewest(e *entry[K, V]) {
	e.prev, e.next = b.root.prev, &b.root
	b.root.prev.next = e
	b.root.prev = e
}

// Get devolve o valor e o marca como o mais recente.
func (b *Bounded[K, V]) Get(k K) (V, bool) {
	e, ok := b.m[k]
	if !ok {
		var zero V
		return zero, false
	}
	b.unlink(e)
	b.pushNewest(e)
	return e.val, true
}

// Peek devolve o valor sem mudar a ordem.
func (b *Bounded[K, V]) Peek(k K) (V, bool) {
	e, ok := b.m[k]
	if !ok {
		var zero V
		return zero, false
	}
	return e.val, true
}

// Put insere ou atualiza k como o mais recente. Se o map estava cheio e k é
// nova, a entrada mais antiga é removida e devolvida com evicted == true.
func (b *Bounded[K, V]) Put(k K, v V) (oldK K, oldV V, evicted bool) {
	if e, ok := b.m[k]; ok {
		e.val = v
		b.unlink(e)
		b.pushNewest(e)
		return oldK, oldV, false
	}
	if len(b.m) >= b.limit {
		oldK, oldV, evicted = b.RemoveOldest()
	}
	e := &entry[K, V]{key: k, val: v}
	b.m[k] = e
	b.pushNewest(e)
	return oldK, oldV, evicted
}

// Oldest devolve a entrada menos recente sem removê-la.
func (b *Bounded[K, V]) Oldest() (k K, v V, ok bool) {
	if len(b.m) == 0 {
		return k, v, false
	}
	e := b.root.next
	return e.key, e.val, true
}

// RemoveOldest remove e devolve a entrada menos recente.
func (b *Bounded[K, V]) RemoveOldest() (k K, v V, ok bool) {
	if len(b.m) == 0 {
		return k, v, false
	}
	e := b.root.next
	b.unlink(e)
	delete(b.m, e.key)
	return e.key, e.val, true
}

// Delete remove k, se existir.
func (b *Bounded[K, V]) Delete(k K) {
	if e, ok := b.m[k]; ok {
		b.unlink(e)
		delete(b.m, k)
	}
}

// All percorre as entradas da mais antiga para a mais nova. A ordem é
// determinística (não depende da iteração do map). Não modifique o Bounded
// durante a iteração.
func (b *Bounded[K, V]) All() iter.Seq2[K, V] {
	return func(yield func(K, V) bool) {
		for e := b.root.next; e != &b.root; e = e.next {
			if !yield(e.key, e.val) {
				return
			}
		}
	}
}
