// Package reconcile implementa o stage que casa exceções com a decisão humana
// que as resolve, com idempotência por decision_id e memória limitada
// (ADR 0016).
//
// Três tipos de evento, pelo campo "kind":
//
//   - exceção: kind == "exception" ou triage.requires_ack == true. Abre uma
//     pendência pela chave correlate_by e fica retida até o desfecho.
//   - decisão: kind == "human_decision", com decision_id, target_key,
//     operator_id, action e reason. Resolve a pendência de target_key.
//   - qualquer outro: passa sem mudança.
//
// Cada exceção sai exatamente uma vez, com "reconcile": {"status": ...}:
// resolved, escalated_timeout, quarantined_timeout, evicted, unresolved (no
// Flush) ou duplicate_pending (já havia pendência para a chave). Uma decisão
// aplicada é absorvida na exceção; uma decisão sem pendência sai como
// orphan_decision; um decision_id repetido é descartado (ou sai como
// duplicate_ignored).
//
// O tempo é o do evento: o prazo de uma pendência é time + window, e ela
// expira quando o maior tempo visto passa do prazo. Nada usa relógio de
// parede (I4, I5).
package reconcile

import (
	"container/heap"
	"errors"
	"fmt"
	"maps"
	"strings"
	"time"

	"github.com/FranciscoHonorat/hermes-pipe/internal/ds"
	"github.com/FranciscoHonorat/hermes-pipe/internal/event"
	"github.com/FranciscoHonorat/hermes-pipe/internal/stage"
)

// Campos do contrato de eventos.
const (
	Field          = "reconcile"
	KindField      = "kind"
	KindException  = "exception"
	KindDecision   = "human_decision"
	DefaultMaxKeys = 10_000
	// DefaultMaxDecisions é o teto padrão de decision_id lembrados.
	DefaultMaxDecisions = 100_000
)

// Status de saída em reconcile.status.
const (
	StatusResolved           = "resolved"
	StatusEscalatedTimeout   = "escalated_timeout"
	StatusQuarantinedTimeout = "quarantined_timeout"
	StatusEvicted            = "evicted"
	StatusUnresolved         = "unresolved"
	StatusDuplicatePending   = "duplicate_pending"
	StatusOrphanDecision     = "orphan_decision"
	StatusDuplicateIgnored   = "duplicate_ignored"
)

// Erros de evento: vão para o stderr e contam como "com erro" (I1).
var (
	ErrNoKey           = errors.New("exceção sem a chave de correlação")
	ErrInvalidDecision = errors.New("human_decision inválida")
)

// TimeoutPolicy é o que fazer com uma pendência que passou do prazo.
type TimeoutPolicy string

// Políticas de timeout.
const (
	TimeoutEscalate   TimeoutPolicy = "escalate"
	TimeoutQuarantine TimeoutPolicy = "quarantine"
	TimeoutDrop       TimeoutPolicy = "drop"
)

// DuplicatePolicy é o que fazer com um decision_id já visto.
type DuplicatePolicy string

// Políticas de duplicata.
const (
	DuplicateDrop DuplicatePolicy = "drop"
	DuplicateMark DuplicatePolicy = "mark"
)

// Config é a configuração do reconcile.
type Config struct {
	CorrelateBy  string          `yaml:"correlate_by"`
	Window       time.Duration   `yaml:"window"`
	MaxKeys      int             `yaml:"max_keys"`
	MaxDecisions int             `yaml:"max_decisions"`
	OnTimeout    TimeoutPolicy   `yaml:"on_timeout"`
	OnDuplicate  DuplicatePolicy `yaml:"on_duplicate"`
}

// Reconcile é um stage com estado: não é Concurrent.
//
// Memória limitada (I6): no máximo MaxKeys pendências e MaxDecisions
// decision_id lembrados. Com as pendências no teto, a de prazo mais próximo
// sai na hora com "evicted": true. Um decision_id esquecido pelo teto, se
// reenviado, não encontra mais a pendência (ela já foi resolvida) e sai como
// órfão: o efeito nunca é aplicado duas vezes.
type Reconcile struct {
	by         string
	window     time.Duration
	maxKeys    int
	onTimeout  TimeoutPolicy
	onDup      DuplicatePolicy
	pending    map[string]*pend
	byDeadline deadlineHeap
	seen       *ds.Bounded[string, struct{}]
	arrivals   uint64
	maxSeen    time.Time
	started    bool
	duplicates int64
	evicted    int64
	name       string
}

type pend struct {
	key      string
	ev       event.Event
	deadline time.Time
	arrival  uint64 // desempate determinístico para o mesmo prazo
	index    int    // posição no heap
}

// New valida a config.
func New(cfg Config) (*Reconcile, error) {
	switch {
	case strings.TrimSpace(cfg.CorrelateBy) == "":
		return nil, errors.New("reconcile: correlate_by é obrigatório")
	case cfg.Window <= 0:
		return nil, errors.New("reconcile: window precisa ser > 0")
	case cfg.MaxKeys < 0:
		return nil, errors.New("reconcile: max_keys não pode ser negativo")
	case cfg.MaxDecisions < 0:
		return nil, errors.New("reconcile: max_decisions não pode ser negativo")
	}
	if cfg.MaxKeys == 0 {
		cfg.MaxKeys = DefaultMaxKeys
	}
	if cfg.MaxDecisions == 0 {
		cfg.MaxDecisions = DefaultMaxDecisions
	}
	switch cfg.OnTimeout {
	case "":
		cfg.OnTimeout = TimeoutEscalate
	case TimeoutEscalate, TimeoutQuarantine, TimeoutDrop:
	default:
		return nil, fmt.Errorf("reconcile: on_timeout %q inválido (use escalate, quarantine ou drop)", cfg.OnTimeout)
	}
	switch cfg.OnDuplicate {
	case "":
		cfg.OnDuplicate = DuplicateDrop
	case DuplicateDrop, DuplicateMark:
	default:
		return nil, fmt.Errorf("reconcile: on_duplicate %q inválido (use drop ou mark)", cfg.OnDuplicate)
	}
	return &Reconcile{
		by: cfg.CorrelateBy, window: cfg.Window, maxKeys: cfg.MaxKeys,
		onTimeout: cfg.OnTimeout, onDup: cfg.OnDuplicate,
		pending: make(map[string]*pend),
		seen:    ds.NewBounded[string, struct{}](cfg.MaxDecisions),
		name:    fmt.Sprintf("reconcile(by %s within %s)", cfg.CorrelateBy, cfg.Window),
	}, nil
}

// Name implementa pipeline.Namer.
func (r *Reconcile) Name() string { return r.name }

// Len devolve quantas pendências estão abertas.
func (r *Reconcile) Len() int { return len(r.pending) }

// Duplicates devolve quantos decision_id repetidos foram ignorados.
func (r *Reconcile) Duplicates() int64 { return r.duplicates }

// Evicted devolve quantas pendências saíram pelo teto max_keys.
func (r *Reconcile) Evicted() int64 { return r.evicted }

// Process classifica e. Primeiro expira o que passou do prazo (o tempo de e
// pode ter avançado o relógio do stream), depois trata e.
func (r *Reconcile) Process(e event.Event, emit func(event.Event)) error {
	kind, _ := e.Get(KindField)
	switch {
	case kind == KindDecision:
		d, err := parseDecision(e)
		if err != nil {
			return err
		}
		r.advance(e.Time, emit)
		r.decide(e, d, emit)
	case kind == KindException || requiresAck(e):
		v, ok := e.Get(r.by)
		if !ok || v == nil {
			return fmt.Errorf("%w %q", ErrNoKey, r.by)
		}
		r.advance(e.Time, emit)
		r.open(stage.KeyString(v, true), e, emit)
	default:
		r.advance(e.Time, emit)
		emit(e)
	}
	return nil
}

func requiresAck(e event.Event) bool {
	v, _ := e.Get("triage.requires_ack")
	return v == true
}

type decision struct {
	id, target, operator, action, reason string
}

func parseDecision(e event.Event) (decision, error) {
	str := func(f string) (string, error) {
		v, _ := e.Get(f)
		s, ok := v.(string)
		if !ok || strings.TrimSpace(s) == "" {
			return "", fmt.Errorf("%w: %q precisa ser texto não vazio", ErrInvalidDecision, f)
		}
		return s, nil
	}
	var d decision
	var err error
	if d.id, err = str("decision_id"); err != nil {
		return d, err
	}
	t, ok := e.Get("target_key")
	if !ok || t == nil {
		return d, fmt.Errorf("%w: %q ausente", ErrInvalidDecision, "target_key")
	}
	d.target = stage.KeyString(t, true)
	if d.operator, err = str("operator_id"); err != nil {
		return d, err
	}
	if d.action, err = str("action"); err != nil {
		return d, err
	}
	d.reason, err = str("reason")
	return d, err
}

// advance move o maior tempo visto e fecha o que passou do prazo, em ordem de
// prazo e, no empate, de chegada. O prazo é inclusivo: uma decisão com tempo
// igual ao prazo ainda resolve.
func (r *Reconcile) advance(t time.Time, emit func(event.Event)) {
	if !r.started || t.After(r.maxSeen) {
		r.started, r.maxSeen = true, t
	}
	for len(r.byDeadline) > 0 && r.byDeadline[0].deadline.Before(r.maxSeen) {
		r.timeout(r.remove(r.byDeadline[0]), emit)
	}
}

func (r *Reconcile) timeout(p *pend, emit func(event.Event)) {
	switch r.onTimeout {
	case TimeoutDrop:
	case TimeoutQuarantine:
		emit(r.annotate(p.ev, deadlineAnn(StatusQuarantinedTimeout, p), false))
	default:
		emit(r.annotate(p.ev, deadlineAnn(StatusEscalatedTimeout, p), false))
	}
}

func (r *Reconcile) open(key string, e event.Event, emit func(event.Event)) {
	if _, ok := r.pending[key]; ok {
		emit(r.annotate(e, map[string]any{"status": StatusDuplicatePending}, false))
		return
	}
	p := &pend{key: key, ev: e, deadline: e.Time.Add(r.window)}
	// Uma exceção que já chega depois do prazo fecha aqui mesmo, sem ocupar
	// (nem despejar) lugar de pendência viva.
	if p.deadline.Before(r.maxSeen) {
		r.timeout(p, emit)
		return
	}
	if len(r.pending) >= r.maxKeys {
		old := r.remove(r.byDeadline[0])
		r.evicted++
		emit(r.annotate(old.ev, deadlineAnn(StatusEvicted, old), true))
	}
	r.arrivals++
	p.arrival = r.arrivals
	r.pending[key] = p
	heap.Push(&r.byDeadline, p)
}

func (r *Reconcile) decide(e event.Event, d decision, emit func(event.Event)) {
	if _, dup := r.seen.Peek(d.id); dup {
		r.duplicates++
		if r.onDup == DuplicateMark {
			emit(r.annotate(e, map[string]any{"status": StatusDuplicateIgnored, "decision_id": d.id}, false))
		}
		return
	}
	r.seen.Put(d.id, struct{}{})
	p, ok := r.pending[d.target]
	if !ok {
		emit(r.annotate(e, map[string]any{"status": StatusOrphanDecision}, false))
		return
	}
	r.remove(p)
	emit(r.annotate(p.ev, map[string]any{
		"status":      StatusResolved,
		"decision_id": d.id,
		"operator_id": d.operator,
		"action":      d.action,
		"reason":      d.reason,
		"decided_at":  e.Time.UTC().Format(time.RFC3339Nano),
	}, false))
}

func (r *Reconcile) remove(p *pend) *pend {
	heap.Remove(&r.byDeadline, p.index)
	delete(r.pending, p.key)
	return p
}

func deadlineAnn(status string, p *pend) map[string]any {
	return map[string]any{"status": status, "deadline": p.deadline.UTC().Format(time.RFC3339Nano)}
}

// annotate devolve uma cópia rasa de e com o objeto reconcile: e não é
// modificado (I4).
func (*Reconcile) annotate(e event.Event, ann map[string]any, evicted bool) event.Event {
	out := e
	out.Fields = maps.Clone(e.Fields)
	out.Fields[Field] = ann
	if evicted {
		out.Fields["evicted"] = true
	}
	return out
}

// Flush emite as pendências abertas como unresolved, em ordem de prazo.
func (r *Reconcile) Flush(emit func(event.Event)) error {
	for len(r.byDeadline) > 0 {
		p := r.remove(r.byDeadline[0])
		emit(r.annotate(p.ev, deadlineAnn(StatusUnresolved, p), false))
	}
	return nil
}

// deadlineHeap é um heap mínimo por (prazo, chegada) com índice, para remover
// uma pendência resolvida em O(log n) sem deixar lixo para trás.
type deadlineHeap []*pend

func (h deadlineHeap) Len() int { return len(h) }

func (h deadlineHeap) Less(i, j int) bool {
	if !h[i].deadline.Equal(h[j].deadline) {
		return h[i].deadline.Before(h[j].deadline)
	}
	return h[i].arrival < h[j].arrival
}

func (h deadlineHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index, h[j].index = i, j
}

func (h *deadlineHeap) Push(x any) {
	p := x.(*pend) //nolint:forcetypeassert // só Push(&pend) chega aqui
	p.index = len(*h)
	*h = append(*h, p)
}

func (h *deadlineHeap) Pop() any {
	old := *h
	n := len(old)
	p := old[n-1]
	old[n-1] = nil
	*h = old[:n-1]
	return p
}
