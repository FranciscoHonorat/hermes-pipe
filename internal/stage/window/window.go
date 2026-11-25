// Package window implementa a agregação por janela de tempo do evento.
package window

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/FranciscoHonorat/hermes-pipe/internal/ds"
	"github.com/FranciscoHonorat/hermes-pipe/internal/event"
	"github.com/FranciscoHonorat/hermes-pipe/internal/stage"
)

// ErrLate é o erro de um evento que chegou depois que a janela dele fechou.
// É erro de evento: vai para o stderr e fecha I1 como "com erro".
var ErrLate = errors.New("evento atrasado: a janela dele já fechou")

// Config é a configuração do window.
type Config struct {
	Size     time.Duration `yaml:"size"`
	Lateness time.Duration `yaml:"lateness"`
	By       []string      `yaml:"by"`
	Metrics  []string      `yaml:"metrics"`
	MaxOpen  int           `yaml:"max_open"`
	MaxKeys  int           `yaml:"max_keys"`
}

// Window agrega por chave dentro de janelas fixas [início, fim) do tempo do
// evento.
//
// Watermark = maior tempo visto − Lateness; nunca recua. Uma janela fecha
// (é emitida) quando o watermark passa do fim dela, quando o teto MaxOpen
// obriga, no Idle ou no Flush. Nada aqui usa relógio de parede: reprocessar
// o mesmo arquivo dá a mesma saída (I5).
//
// As janelas abertas são contíguas e moram num ds.Ring: a da frente é a mais
// antiga, de índice base.
type Window struct {
	size, lateness time.Duration
	by             []string
	metrics        []stage.Metric
	maxKeys        int
	open           *ds.Ring[*win]
	base           int64 // índice da janela na frente do ring
	maxSeen        time.Time
	watermark      time.Time
	started        bool
	name           string
}

type win struct {
	idx    int64
	groups *ds.Bounded[string, *stage.Group]
}

// New valida a config.
func New(cfg Config) (*Window, error) {
	if cfg.Size <= 0 {
		return nil, errors.New("window: size precisa ser > 0")
	}
	if cfg.Lateness < 0 {
		return nil, errors.New("window: lateness não pode ser negativo")
	}
	if len(cfg.By) == 0 {
		return nil, errors.New("window: by precisa de pelo menos um campo")
	}
	ms, err := stage.ParseMetrics(cfg.Metrics)
	if err != nil {
		return nil, fmt.Errorf("window: %w", err)
	}
	if cfg.MaxOpen < 0 || cfg.MaxKeys < 0 {
		return nil, errors.New("window: max_open e max_keys não podem ser negativos")
	}
	if cfg.MaxOpen == 0 {
		cfg.MaxOpen = 64
	}
	if cfg.MaxKeys == 0 {
		cfg.MaxKeys = 10_000
	}
	return &Window{
		size: cfg.Size, lateness: cfg.Lateness, by: cfg.By, metrics: ms,
		maxKeys: cfg.MaxKeys, open: ds.NewRing[*win](cfg.MaxOpen),
		name: fmt.Sprintf("window(%s by %s)", cfg.Size, strings.Join(cfg.By, ",")),
	}, nil
}

// Name implementa pipeline.Namer.
func (w *Window) Name() string { return w.name }

// Watermark devolve o watermark atual.
func (w *Window) Watermark() time.Time { return w.watermark }

func (w *Window) index(t time.Time) int64 {
	n, s := t.UnixNano(), int64(w.size)
	q := n / s
	if n%s != 0 && n < 0 { // divisão com piso também para antes de 1970
		q--
	}
	return q
}

func (w *Window) start(idx int64) time.Time { return time.Unix(0, idx*int64(w.size)).UTC() }
func (w *Window) end(idx int64) time.Time   { return w.start(idx + 1) }

// Process põe e na janela dela.
func (w *Window) Process(e event.Event, emit func(event.Event)) error {
	idx := w.index(e.Time)
	// Atrasado: a janela já fechou pelo watermark ou já saiu do ring.
	if w.started && (!w.end(idx).After(w.watermark) || idx < w.base) {
		return ErrLate
	}
	first := !w.started
	// O watermark só muda quando chega um novo máximo; como maxSeen só
	// cresce, maxSeen − lateness também só cresce: ele nunca recua.
	if first || e.Time.After(w.maxSeen) {
		w.started = true
		w.maxSeen = e.Time
		w.watermark = e.Time.Add(-w.lateness)
	}
	// A janela aberta mais antiga possível é a que contém o watermark: com o
	// ring vazio, ele recomeça dali (ou de onde parou, se for depois).
	if w.open.Len() == 0 {
		if wmIdx := w.index(w.watermark); first || wmIdx > w.base {
			w.base = wmIdx
		}
	}
	w.ensure(idx, emit)
	win := w.open.At(int(idx - w.base))
	g, ok := win.groups.Get(stage.GroupKey(e, w.by))
	if !ok {
		key := stage.GroupKey(e, w.by)
		g = stage.NewGroup(key, e, w.by, w.metrics)
		if _, old, ev := win.groups.Put(key, g); ev {
			emit(w.result(win, old, true))
		}
	}
	g.Add(e, w.metrics)
	w.closeUpTo(w.watermark, emit)
	return nil
}

// ensure garante que o ring cobre idx, fechando as janelas mais antigas se o
// teto MaxOpen obrigar.
func (w *Window) ensure(idx int64, emit func(event.Event)) {
	limit := int64(w.open.Cap())
	for idx >= w.base+limit {
		if w.open.Len() == 0 {
			w.base = idx - limit + 1
			break
		}
		w.closeFront(emit)
	}
	for next := w.base + int64(w.open.Len()); next <= idx; next++ {
		w.open.PushBack(&win{idx: next, groups: ds.NewBounded[string, *stage.Group](w.maxKeys)})
	}
}

// closeUpTo fecha as janelas cujo fim é <= wm.
func (w *Window) closeUpTo(wm time.Time, emit func(event.Event)) {
	for w.open.Len() > 0 && !w.end(w.base).After(wm) {
		w.closeFront(emit)
	}
}

func (w *Window) closeFront(emit func(event.Event)) {
	win, _ := w.open.PopFront()
	w.base++
	gs := make([]*stage.Group, 0, win.groups.Len())
	for _, g := range win.groups.All() {
		gs = append(gs, g)
	}
	stage.SortGroups(gs, w.metrics, false)
	for _, g := range gs {
		emit(w.result(win, g, false))
	}
}

func (w *Window) result(win *win, g *stage.Group, evicted bool) event.Event {
	extra := map[string]any{
		"window_start": w.start(win.idx).Format(time.RFC3339Nano),
		"window_end":   w.end(win.idx).Format(time.RFC3339Nano),
	}
	if evicted {
		extra["evicted"] = true
	}
	return g.Event(w.by, w.metrics, w.start(win.idx), extra)
}

// Flush fecha todas as janelas abertas, em ordem.
func (w *Window) Flush(emit func(event.Event)) error {
	for w.open.Len() > 0 {
		w.closeFront(emit)
	}
	return nil
}

// Idle fecha todas as janelas abertas: a fonte ficou em silêncio. Só roda com
// idle-flush ligado no runner, nunca em replay.
func (w *Window) Idle(emit func(event.Event)) error { return w.Flush(emit) }
