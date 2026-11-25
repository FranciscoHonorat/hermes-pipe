package stage

import (
	"strings"
	"testing"

	"github.com/FranciscoHonorat/hermes-pipe/internal/event"
)

func TestParseMetrics_Ratio(t *testing.T) {
	ms, err := ParseMetrics([]string{"ratio:energy_wh:units_ok"})
	if err != nil {
		t.Fatal(err)
	}
	m := ms[0]
	if m.Kind != "ratio" || m.Field != "energy_wh" || m.Den != "units_ok" || m.Name != "ratio_energy_wh_units_ok" {
		t.Fatalf("%+v", m)
	}
	for _, bad := range []string{"ratio:a", "ratio:a:", "ratio::b", "ratio:a:b:c", "ratio"} {
		if _, err := ParseMetrics([]string{bad}); err == nil {
			t.Errorf("%q: esperava erro", bad)
		}
	}
}

func groupOut(t *testing.T, metrics []string, lines ...string) string {
	t.Helper()
	ms, err := ParseMetrics(metrics)
	if err != nil {
		t.Fatal(err)
	}
	by := []string{"k"}
	var g *Group
	for _, l := range lines {
		e, err := event.Parse([]byte(l))
		if err != nil {
			t.Fatal(err)
		}
		if g == nil {
			g = NewGroup(GroupKey(e, by), e, by, ms)
		}
		g.Add(e, ms)
	}
	b, err := g.Event(by, ms, g.Time(), nil).AppendJSON(nil)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// TestRatio: razão das somas, só com eventos que têm os dois campos
// numéricos; denominador zero nunca vira NaN/Inf: valor null e status.
func TestRatio(t *testing.T) {
	const (
		ev = `{"time":"2026-01-01T00:00:00Z","k":"a"`
		r  = "ratio:e:u"
	)
	tests := []struct {
		name  string
		lines []string
		want  string
	}{
		{"razão das somas", []string{ev + `,"e":100,"u":2}`, ev + `,"e":50,"u":1}`}, `"ratio_e_u":50,`},
		{"evento sem par não conta", []string{ev + `,"e":100,"u":4}`, ev + `,"e":999}`, ev + `,"u":999}`, ev + `,"e":1,"u":"x"}`}, `"ratio_e_u":25,`},
		{"denominador zero", []string{ev + `,"e":10,"u":0}`}, `"ratio_e_u":null,"ratio_e_u_status":"undefined_denominator",`},
		{"sem nenhum par", []string{ev + `}`}, `"ratio_e_u":null,"ratio_e_u_status":"undefined_denominator",`},
		{"numerador zero é definido", []string{ev + `,"e":0,"u":3}`}, `"ratio_e_u":0,`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := groupOut(t, []string{"count", r}, tt.lines...)
			if !strings.Contains(got, tt.want) {
				t.Fatalf("got %s\nwant contendo %s", got, tt.want)
			}
			if strings.Contains(got, "NaN") || strings.Contains(got, "Inf") {
				t.Fatalf("JSON inválido: %s", got)
			}
			if !strings.Contains(tt.want, "_status") && strings.Contains(got, "_status") {
				t.Fatalf("status inesperado: %s", got)
			}
		})
	}
}
