// Package triage implementa o stage que classifica cada evento por regras
// ordenadas e anota a decisão no próprio evento (ADR 0015).
//
// O triage não retém, não descarta e não faz I/O: todo evento sai, uma vez, na
// ordem em que entrou, com o objeto "triage" anexado. Rotear "human_review"
// para uma fila é trabalho de um filter depois dele ou de quem lê o stdout.
package triage

import (
	"errors"
	"fmt"
	"maps"
	"strings"

	"github.com/FranciscoHonorat/hermes-pipe/internal/event"
	"github.com/FranciscoHonorat/hermes-pipe/internal/stage"
)

// Field é o campo de saída com a anotação. Os subcampos são lidos com caminho
// com ponto: triage.decision, triage.rule, triage.reason, triage.requires_ack.
const Field = "triage"

// Decision é o resultado da triagem.
type Decision string

// As decisões aceitas.
const (
	AutoOK      Decision = "auto_ok"
	HumanReview Decision = "human_review"
	Quarantine  Decision = "quarantine"
)

func (d Decision) valid() bool { return d == AutoOK || d == HumanReview || d == Quarantine }

// Rule é uma regra: se todas as expressões de Where forem verdadeiras (mesma
// linguagem do filter), o evento recebe Decision, Reason e RequiresAck.
type Rule struct {
	Name        string   `yaml:"name"`
	Where       []string `yaml:"where"`
	Decision    Decision `yaml:"decision"`
	Reason      string   `yaml:"reason"`
	RequiresAck bool     `yaml:"requires_ack"`
}

// Config é a configuração do triage. As regras são avaliadas de cima para
// baixo e a primeira que casa vence. Sem regra casando, vale DefaultDecision
// (auto_ok se vazio), com rule e reason null.
type Config struct {
	DefaultDecision Decision `yaml:"default_decision"`
	Rules           []Rule   `yaml:"rules"`
}

type rule struct {
	Rule
	conds []stage.Cond
}

// Triage é um stage sem estado: seguro para Process concorrente.
type Triage struct {
	rules []rule
	def   Decision
	name  string
}

// New valida a config e compila as expressões.
func New(cfg Config) (*Triage, error) {
	if len(cfg.Rules) == 0 {
		return nil, errors.New("triage: precisa de pelo menos uma regra em rules")
	}
	if cfg.DefaultDecision == "" {
		cfg.DefaultDecision = AutoOK
	}
	if !cfg.DefaultDecision.valid() {
		return nil, fmt.Errorf("triage: default_decision %q inválido (use auto_ok, human_review ou quarantine)", cfg.DefaultDecision)
	}
	t := &Triage{def: cfg.DefaultDecision, rules: make([]rule, 0, len(cfg.Rules))}
	seen := make(map[string]bool, len(cfg.Rules))
	names := make([]string, 0, len(cfg.Rules))
	for i, r := range cfg.Rules {
		if err := validate(i, r, seen); err != nil {
			return nil, fmt.Errorf("triage: %w", err)
		}
		conds, err := stage.ParseConds(r.Where)
		if err != nil {
			return nil, fmt.Errorf("triage: regra %q: %w", r.Name, err)
		}
		t.rules = append(t.rules, rule{Rule: r, conds: conds})
		names = append(names, r.Name)
	}
	t.name = "triage(" + strings.Join(names, ",") + ")"
	return t, nil
}

func validate(i int, r Rule, seen map[string]bool) error {
	switch {
	case strings.TrimSpace(r.Name) == "":
		return fmt.Errorf("regra %d sem name", i+1)
	case seen[r.Name]:
		return fmt.Errorf("regra %q repetida", r.Name)
	case len(r.Where) == 0:
		return fmt.Errorf("regra %q: precisa de pelo menos uma expressão em where", r.Name)
	case !r.Decision.valid():
		return fmt.Errorf("regra %q: decision %q inválido (use auto_ok, human_review ou quarantine)", r.Name, r.Decision)
	case strings.TrimSpace(r.Reason) == "":
		return fmt.Errorf("regra %q: reason é obrigatório (é o que o operador lê)", r.Name)
	}
	seen[r.Name] = true
	return nil
}

// Name implementa pipeline.Namer.
func (t *Triage) Name() string { return t.name }

// Concurrent implementa pipeline.Concurrent.
func (*Triage) Concurrent() bool { return true }

// Process emite e, uma vez, com a anotação de triagem.
//
// e não é modificado (I4): o mapa de topo é copiado raso, o que basta porque
// só a chave "triage" é escrita; os valores aninhados são compartilhados e
// ninguém os modifica.
func (t *Triage) Process(e event.Event, emit func(event.Event)) error {
	ann := map[string]any{"decision": string(t.def), "rule": nil, "reason": nil, "requires_ack": false}
	for i := range t.rules {
		r := &t.rules[i]
		if stage.All(r.conds, e) {
			ann["decision"], ann["rule"], ann["reason"], ann["requires_ack"] = string(r.Decision), r.Name, r.Reason, r.RequiresAck
			break
		}
	}
	out := e
	out.Fields = maps.Clone(e.Fields)
	out.Fields[Field] = ann
	emit(out)
	return nil
}
