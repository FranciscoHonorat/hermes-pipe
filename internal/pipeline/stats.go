package pipeline

import (
	"sync/atomic"
	"time"
)

// Stats é o resumo de uma execução.
//
// Invariante I1 (conservação), válida sempre que não houve hard cancel:
//
//	Read + Generated == Emitted + Dropped + Errored
//
// Read conta linhas lidas; Generated conta eventos criados por stages além de
// um por entrada (fan-out e Flush); Dropped conta entradas que um stage não
// repassou (filtradas, absorvidas por agregação, além do limite).
type Stats struct {
	Read      int64
	Emitted   int64
	Dropped   int64
	Errored   int64
	Generated int64
	// Stopped é a causa da parada suave (limite, Ctrl+C), ou nil se o
	// producer chegou ao fim.
	Stopped error
	Elapsed time.Duration
}

// Conserved diz se I1 vale.
func (s Stats) Conserved() bool {
	return s.Read+s.Generated == s.Emitted+s.Dropped+s.Errored
}

// Counters são os contadores ao vivo de uma execução. Os stages não os veem:
// só o runner incrementa. São atômicos porque cada contador é escrito por
// goroutines diferentes e lido por quem mostra --stats.
type Counters struct {
	read, emitted, dropped, errored, generated atomic.Int64
}

// Snapshot lê os contadores. Cada campo é lido atomicamente, mas o conjunto
// não é uma fotografia consistente enquanto o pipeline roda.
func (c *Counters) Snapshot() Stats {
	return Stats{
		Read:      c.read.Load(),
		Emitted:   c.emitted.Load(),
		Dropped:   c.dropped.Load(),
		Errored:   c.errored.Load(),
		Generated: c.generated.Load(),
	}
}

// account registra o resultado de uma chamada de Process que não falhou.
func (c *Counters) account(n int) {
	switch {
	case n == 0:
		c.dropped.Add(1)
	case n > 1:
		c.generated.Add(int64(n - 1))
	}
}
