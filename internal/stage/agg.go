package stage

import (
	"cmp"
	"errors"
	"fmt"
	"maps"
	"math"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/FranciscoHonorat/hermes-pipe/internal/event"
)

// MaxSamples é quanto cada grupo guarda para estimar percentis. Acima disso,
// a amostra é um reservatório determinístico: a memória por chave tem teto.
const MaxSamples = 512

// Metric é uma métrica de agregação: count, sum:f, min:f, max:f, avg:f,
// pNN:f (percentil NN, ex.: p95:latency_ms) ou ratio:num:den (razão das
// somas, ex.: ratio:energy_wh:units_ok; ADR 0015).
type Metric struct {
	Kind  string  // count, sum, min, max, avg, p, ratio
	Field string  // para ratio, o numerador
	Den   string  // só para Kind == "ratio": o denominador
	P     float64 // só para Kind == "p", em (0, 100]
	Name  string  // nome do campo de saída
}

// StatusUndefinedDenominator é o valor de "<métrica>_status" quando a soma
// dos denominadores de um ratio é zero: o valor sai null, nunca NaN ou Inf.
const StatusUndefinedDenominator = "undefined_denominator"

// ParseMetrics valida a lista de métricas. Lista vazia vira ["count"].
func ParseMetrics(src []string) ([]Metric, error) {
	if len(src) == 0 {
		src = []string{"count"}
	}
	out := make([]Metric, 0, len(src))
	seen := map[string]bool{}
	for _, s := range src {
		m, err := parseMetric(s)
		if err != nil {
			return nil, fmt.Errorf("métrica %q: %w", s, err)
		}
		if seen[m.Name] {
			return nil, fmt.Errorf("métrica %q repetida", s)
		}
		seen[m.Name] = true
		out = append(out, m)
	}
	return out, nil
}

func parseMetric(s string) (Metric, error) {
	kind, field, hasField := strings.Cut(strings.TrimSpace(s), ":")
	if kind == "count" {
		if hasField {
			return Metric{}, errors.New("count não leva campo")
		}
		return Metric{Kind: "count", Name: "count"}, nil
	}
	if !hasField || field == "" {
		return Metric{}, errors.New("formato é tipo:campo")
	}
	if kind == "ratio" {
		num, den, ok := strings.Cut(field, ":")
		if !ok || num == "" || den == "" || strings.Contains(den, ":") {
			return Metric{}, errors.New("formato é ratio:numerador:denominador")
		}
		return Metric{Kind: "ratio", Field: num, Den: den, Name: "ratio_" + num + "_" + den}, nil
	}
	m := Metric{Kind: kind, Field: field, Name: kind + "_" + field}
	switch kind {
	case "sum", "min", "max", "avg":
	default:
		p, ok := strings.CutPrefix(kind, "p")
		v, err := strconv.ParseFloat(p, 64)
		if !ok || err != nil || v <= 0 || v > 100 {
			return Metric{}, errors.New("tipo desconhecido (use count, sum, min, max, avg, pNN ou ratio)")
		}
		m.Kind, m.P = "p", v
	}
	return m, nil
}

// GroupKey monta a chave de agrupamento de e pelos campos by.
func GroupKey(e event.Event, by []string) string {
	if len(by) == 1 {
		v, ok := e.Get(by[0])
		return KeyString(v, ok)
	}
	var b strings.Builder
	for i, f := range by {
		if i > 0 {
			b.WriteByte(0)
		}
		v, ok := e.Get(f)
		b.WriteString(KeyString(v, ok))
	}
	return b.String()
}

// Group acumula as métricas de uma chave. Memória por Group é limitada por
// MaxSamples por métrica de percentil.
type Group struct {
	Key     string
	keyVals []any
	keyOK   []bool
	last    time.Time
	count   int64
	states  []metricState
}

type metricState struct {
	n             int64
	sum, min, max float64
	den           float64 // só ratio: soma dos denominadores
	samples       []float64
	rng           uint64
}

// NewGroup cria o grupo da chave de e.
func NewGroup(key string, e event.Event, by []string, metrics []Metric) *Group {
	g := &Group{Key: key, keyVals: make([]any, len(by)), keyOK: make([]bool, len(by)), states: make([]metricState, len(metrics))}
	for i, f := range by {
		g.keyVals[i], g.keyOK[i] = e.Get(f)
	}
	for i := range g.states {
		g.states[i].rng = 0x9E3779B97F4A7C15 // semente fixa: determinístico
	}
	return g
}

// Time devolve o maior tempo de evento visto no grupo.
func (g *Group) Time() time.Time { return g.last }

// Count devolve quantos eventos o grupo viu.
func (g *Group) Count() int64 { return g.count }

// Add acumula e no grupo. Valores não numéricos são ignorados pelas métricas
// numéricas.
func (g *Group) Add(e event.Event, metrics []Metric) {
	g.count++
	if e.Time.After(g.last) {
		g.last = e.Time
	}
	for i, m := range metrics {
		if m.Kind == "count" {
			continue
		}
		if m.Kind == "ratio" {
			g.states[i].addRatio(e, m)
			continue
		}
		raw, ok := e.Get(m.Field)
		if !ok {
			continue
		}
		v, ok := ToFloat(raw)
		if !ok || math.IsNaN(v) {
			continue
		}
		g.states[i].add(v, m.Kind == "p")
	}
}

// addRatio soma numerador e denominador só quando os dois são números: um
// evento sem par não puxa a razão para nenhum lado.
func (s *metricState) addRatio(e event.Event, m Metric) {
	num, ok := numberAt(e, m.Field)
	if !ok {
		return
	}
	den, ok := numberAt(e, m.Den)
	if !ok {
		return
	}
	s.n++
	s.sum += num
	s.den += den
}

func numberAt(e event.Event, path string) (float64, bool) {
	raw, ok := e.Get(path)
	if !ok {
		return 0, false
	}
	v, ok := ToFloat(raw)
	return v, ok && !math.IsNaN(v)
}

func (s *metricState) add(v float64, keepSamples bool) {
	if s.n == 0 || v < s.min {
		s.min = v
	}
	if s.n == 0 || v > s.max {
		s.max = v
	}
	s.n++
	s.sum += v
	if !keepSamples {
		return
	}
	if len(s.samples) < MaxSamples {
		s.samples = append(s.samples, v)
		return
	}
	// Reservatório (algoritmo R) com xorshift de semente fixa.
	s.rng ^= s.rng << 13
	s.rng ^= s.rng >> 7
	s.rng ^= s.rng << 17
	if j := s.rng % uint64(s.n); j < MaxSamples { //nolint:gosec // s.n > 0: acabou de ser incrementado
		s.samples[j] = v
	}
}

// Value devolve o valor da métrica i (nil se não houve nenhum número).
func (g *Group) Value(i int, m Metric) any {
	if m.Kind == "count" {
		return g.count
	}
	s := &g.states[i]
	if s.n == 0 {
		return nil
	}
	switch m.Kind {
	case "ratio":
		if s.den == 0 {
			return nil
		}
		return s.sum / s.den
	case "sum":
		return s.sum
	case "min":
		return s.min
	case "max":
		return s.max
	case "avg":
		return s.sum / float64(s.n)
	}
	sorted := slices.Clone(s.samples)
	slices.Sort(sorted)
	rank := int(math.Ceil(m.P/100*float64(len(sorted)))) - 1 // nearest-rank
	return sorted[max(rank, 0)]
}

// Event monta o evento de saída do grupo, com os campos by, as métricas e
// extra. O instante é o maior tempo de evento visto no grupo.
func (g *Group) Event(by []string, metrics []Metric, at time.Time, extra map[string]any) event.Event {
	fields := make(map[string]any, len(by)+len(metrics)+len(extra)+1)
	for i, f := range by {
		if g.keyOK[i] {
			fields[f] = g.keyVals[i]
		} else {
			fields[f] = nil
		}
	}
	for i, m := range metrics {
		v := jsonValue(g.Value(i, m))
		fields[m.Name] = v
		if m.Kind == "ratio" && v == nil {
			fields[m.Name+"_status"] = StatusUndefinedDenominator
		}
	}
	maps.Copy(fields, extra)
	if at.IsZero() {
		at = g.last
	}
	return event.New(at, fields)
}

// jsonValue evita float64 com Inf (inválido em JSON) e ints como float.
func jsonValue(v any) any {
	f, ok := v.(float64)
	if !ok {
		return v
	}
	if math.IsInf(f, 0) {
		return nil
	}
	return f
}

// SortGroups ordena para emissão determinística: pela primeira métrica
// (decrescente) se byMetric, desempate (e ordem padrão) pela chave.
func SortGroups(gs []*Group, metrics []Metric, byMetric bool) {
	slices.SortStableFunc(gs, func(a, b *Group) int {
		if byMetric && len(metrics) > 0 {
			va, _ := ToFloat(toNumber(a.Value(0, metrics[0])))
			vb, _ := ToFloat(toNumber(b.Value(0, metrics[0])))
			if c := cmp.Compare(vb, va); c != 0 {
				return c
			}
		}
		return strings.Compare(a.Key, b.Key)
	})
}

func toNumber(v any) any {
	if n, ok := v.(int64); ok {
		return float64(n)
	}
	return v
}
