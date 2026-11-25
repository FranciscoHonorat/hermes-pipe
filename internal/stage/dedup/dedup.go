// Package dedup implementa o stage que remove eventos repetidos dentro de uma
// janela de tempo do evento. A primeira ocorrência passa.
package dedup

import (
	"errors"
	"fmt"
	"hash/fnv"
	"strings"
	"time"

	"github.com/FranciscoHonorat/hermes-pipe/internal/ds"
	"github.com/FranciscoHonorat/hermes-pipe/internal/event"
	"github.com/FranciscoHonorat/hermes-pipe/internal/stage"
)

// DefaultMaxKeys é o teto padrão de chaves lembradas.
const DefaultMaxKeys = 100_000

// Config é a configuração do dedup.
type Config struct {
	By      []string      `yaml:"by"`
	Window  time.Duration `yaml:"window"`
	MaxKeys int           `yaml:"max_keys"`
}

// Hasher calcula o hash de 64 bits de uma chave. Injetável para testes de
// colisão.
type Hasher func(key string) uint64

// FNV64a é o Hasher padrão.
func FNV64a(key string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(key))
	return h.Sum64()
}

// Dedup lembra o hash da chave de cada evento visto e o tempo da primeira
// ocorrência. Um evento é duplicado se o mesmo hash foi visto há menos de
// Window (no tempo do evento).
//
// Memória limitada (I6): no máximo MaxKeys hashes. Com o teto cheio, o mais
// antigo é esquecido: um duplicado dele passa a ser tratado como novo.
//
// Guarda o hash, não a chave: 8 bytes por entrada em vez da chave inteira.
// O preço é que duas chaves diferentes com o mesmo hash são tratadas como a
// mesma (ADR 0011: com FNV-64 e 1e5 chaves, a chance é ~3e-10).
type Dedup struct {
	by      []string
	window  time.Duration
	hash    Hasher
	seen    *ds.Bounded[uint64, time.Time]
	dropped int64
	name    string
}

// New valida a config.
func New(cfg Config) (*Dedup, error) { return NewWithHasher(cfg, FNV64a) }

// NewWithHasher é New com um Hasher próprio.
func NewWithHasher(cfg Config, h Hasher) (*Dedup, error) {
	if len(cfg.By) == 0 {
		return nil, errors.New("dedup: by precisa de pelo menos um campo")
	}
	if cfg.Window <= 0 {
		return nil, errors.New("dedup: window precisa ser > 0")
	}
	if cfg.MaxKeys < 0 {
		return nil, errors.New("dedup: max_keys não pode ser negativo")
	}
	if cfg.MaxKeys == 0 {
		cfg.MaxKeys = DefaultMaxKeys
	}
	return &Dedup{
		by:     cfg.By,
		window: cfg.Window,
		hash:   h,
		seen:   ds.NewBounded[uint64, time.Time](cfg.MaxKeys),
		name:   fmt.Sprintf("dedup(by %s within %s)", strings.Join(cfg.By, ","), cfg.Window),
	}, nil
}

// Name implementa pipeline.Namer.
func (d *Dedup) Name() string { return d.name }

// Len devolve quantas chaves estão lembradas.
func (d *Dedup) Len() int { return d.seen.Len() }

// Process emite e se ele não é duplicado.
func (d *Dedup) Process(e event.Event, emit func(event.Event)) error {
	h := d.hash(stage.GroupKey(e, d.by))
	// Peek: um duplicado não renova a lembrança. A janela conta a partir da
	// primeira ocorrência.
	if first, ok := d.seen.Peek(h); ok {
		// Distância absoluta: um repetido fora de ordem (tempo anterior ao da
		// primeira ocorrência) também é duplicado.
		if age := e.Time.Sub(first).Abs(); age < d.window {
			d.dropped++
			return nil
		}
	}
	d.seen.Put(h, e.Time)
	d.expire(e.Time)
	emit(e)
	return nil
}

// expire esquece, a partir do mais antigo inserido, o que já saiu da janela.
// Como a ordem é de inserção, uma entrada velha atrás de uma nova espera a
// vez; ela só ocupa memória, não muda o resultado (a idade é conferida no
// Peek).
func (d *Dedup) expire(now time.Time) {
	for {
		_, t, ok := d.seen.Oldest()
		if !ok || now.Sub(t) < d.window {
			return
		}
		d.seen.RemoveOldest()
	}
}
