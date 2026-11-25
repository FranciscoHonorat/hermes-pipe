// Package aggregate implementa a contagem por chave com memória limitada.
package aggregate

import (
	"errors"
	"fmt"
	"strings"

	"github.com/FranciscoHonorat/hermes-pipe/internal/ds"
	"github.com/FranciscoHonorat/hermes-pipe/internal/event"
	"github.com/FranciscoHonorat/hermes-pipe/internal/stage"
)

// DefaultMaxKeys é o teto padrão de chaves em memória.
const DefaultMaxKeys = 100_000

// Config é a configuração do aggregate.
type Config struct {
	By      []string `yaml:"by"`
	Metrics []string `yaml:"metrics"`
	Top     int      `yaml:"top"`
	MaxKeys int      `yaml:"max_keys"`
}

// Aggregate agrupa eventos por chave e emite um evento por grupo no Flush.
//
// Teto de memória (I6): no máximo MaxKeys grupos. Uma chave nova com o teto
// cheio despeja a menos usada recentemente (LRU), que é emitida na hora com
// "evicted": true. O despejo depende só da ordem de entrada: é determinístico.
type Aggregate struct {
	by      []string
	metrics []stage.Metric
	top     int
	groups  *ds.Bounded[string, *stage.Group]
	evicted int64
	name    string
}

// New valida a config.
func New(cfg Config) (*Aggregate, error) {
	if len(cfg.By) == 0 {
		return nil, errors.New("aggregate: by precisa de pelo menos um campo")
	}
	ms, err := stage.ParseMetrics(cfg.Metrics)
	if err != nil {
		return nil, fmt.Errorf("aggregate: %w", err)
	}
	if cfg.Top < 0 || cfg.MaxKeys < 0 {
		return nil, errors.New("aggregate: top e max_keys não podem ser negativos")
	}
	if cfg.MaxKeys == 0 {
		cfg.MaxKeys = DefaultMaxKeys
	}
	return &Aggregate{
		by:      cfg.By,
		metrics: ms,
		top:     cfg.Top,
		groups:  ds.NewBounded[string, *stage.Group](cfg.MaxKeys),
		name:    "aggregate(by " + strings.Join(cfg.By, ",") + ")",
	}, nil
}

// Name implementa pipeline.Namer.
func (a *Aggregate) Name() string { return a.name }

// Evicted devolve quantos grupos foram despejados pelo teto.
func (a *Aggregate) Evicted() int64 { return a.evicted }

// Len devolve quantos grupos estão em memória.
func (a *Aggregate) Len() int { return a.groups.Len() }

// Process acumula e. Só emite quando um grupo é despejado.
func (a *Aggregate) Process(e event.Event, emit func(event.Event)) error {
	key := stage.GroupKey(e, a.by)
	g, ok := a.groups.Get(key)
	if !ok {
		g = stage.NewGroup(key, e, a.by, a.metrics)
		if _, old, ev := a.groups.Put(key, g); ev {
			a.evicted++
			emit(old.Event(a.by, a.metrics, old.Time(), map[string]any{"evicted": true}))
		}
	}
	g.Add(e, a.metrics)
	return nil
}

// Flush emite os grupos em ordem determinística: pela primeira métrica
// (decrescente) com top > 0, senão pela chave. Com top, só os top primeiros.
func (a *Aggregate) Flush(emit func(event.Event)) error {
	gs := make([]*stage.Group, 0, a.groups.Len())
	for _, g := range a.groups.All() {
		gs = append(gs, g)
	}
	stage.SortGroups(gs, a.metrics, a.top > 0)
	if a.top > 0 && len(gs) > a.top {
		gs = gs[:a.top]
	}
	for _, g := range gs {
		emit(g.Event(a.by, a.metrics, g.Time(), nil))
	}
	return nil
}
