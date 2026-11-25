// Package pipeline é o runner do hermes-pipe: o único lugar com concorrência.
//
// O runner cria e fecha os channels, propaga cancelamento, conta eventos e
// drena na ordem producer → stages → consumer. Ele só importa internal/event:
// os stages chegam por interface, e quem liga tudo é cmd/hermes-pipe.
package pipeline

import (
	"context"
	"errors"
	"fmt"

	"github.com/FranciscoHonorat/hermes-pipe/internal/event"
)

// Record é uma linha crua vinda de um Producer.
type Record struct {
	Seq  uint64 // número da linha, a partir de 1
	Line []byte // conteúdo, sem "\n"; vale até a próxima chamada de Next
	Err  error  // erro de leitura daquela linha (ex.: event.ErrLineTooLong)
}

// Producer entrega linhas ao runner.
//
// Contrato:
//   - Next devolve io.EOF quando acabou, e continua devolvendo io.EOF depois.
//   - Record.Err != nil é um erro daquela linha: o runner conta e segue.
//   - Qualquer outro erro de Next é fatal.
//   - Se ctx é cancelado, Next deve retornar assim que puder (um Producer
//     sobre leitura bloqueante, como stdin, usa AsyncProducer).
type Producer interface {
	Next(ctx context.Context) (Record, error)
}

// Stage processa um evento e emite zero ou mais eventos.
//
// Contrato (testado por internal/stage/stagetest):
//   - Não modifica o Event recebido (use Clone).
//   - Não guarda emit depois de retornar.
//   - Erro comum é erro do evento: o runner conta, reporta e segue; os
//     eventos emitidos na mesma chamada são descartados.
//   - Erro com método Fatal() bool == true cancela o pipeline.
//   - Erro com método Stop() bool == true pede parada suave: o producer para
//     de ler, o que está em trânsito é drenado e os eventos emitidos na
//     chamada seguem normalmente.
type Stage interface {
	Process(e event.Event, emit func(event.Event)) error
}

// Flusher é implementado por stages com estado: Flush é chamado exatamente
// uma vez, no fim do stream (EOF ou parada suave), nunca após hard cancel.
type Flusher interface {
	Flush(emit func(event.Event)) error
}

// Idler é implementado por stages que podem emitir quando a entrada fica
// parada (ex.: fechar janelas). Só é chamado com WithIdleFlush > 0.
type Idler interface {
	Idle(emit func(event.Event)) error
}

// Concurrent é implementado por stages sem estado mutável, que aceitam
// Process concorrente. Só esses rodam com WithWorkers > 1.
type Concurrent interface {
	Concurrent() bool
}

// Namer dá o nome usado em erros e logs. Sem ele, o runner usa o tipo.
type Namer interface {
	Name() string
}

// Consumer recebe os eventos que saem do último stage.
//
// Contrato: Consume é chamado por uma goroutine só, em ordem; Close é chamado
// exatamente uma vez quando Run termina, mesmo após erro. Erro em Consume é
// fatal.
type Consumer interface {
	Consume(e event.Event) error
	Close() error
}

// ErrorSink recebe os eventos rejeitados. Mesmo contrato de Consumer, mas
// Report pode ser chamado de várias goroutines: o runner serializa as chamadas.
type ErrorSink interface {
	Report(r event.ErrorRecord) error
	Close() error
}

// PanicError é o erro fatal produzido quando um Stage entra em pânico.
type PanicError struct {
	Stage string
	Value any
	Stack []byte
}

func (e *PanicError) Error() string {
	return fmt.Sprintf("pânico no stage %s: %v", e.Stage, e.Value)
}

// Fatal marca o pânico como fatal: o estado do Stage é desconhecido.
func (*PanicError) Fatal() bool { return true }

func isFatal(err error) bool {
	var f interface{ Fatal() bool }
	return errors.As(err, &f) && f.Fatal()
}

func isStop(err error) bool {
	var s interface{ Stop() bool }
	return errors.As(err, &s) && s.Stop()
}

func stageName(s Stage) string {
	if n, ok := s.(Namer); ok {
		return n.Name()
	}
	return fmt.Sprintf("%T", s)
}

// discardErrors é o ErrorSink padrão.
type discardErrors struct{}

func (discardErrors) Report(event.ErrorRecord) error { return nil }
func (discardErrors) Close() error                   { return nil }
