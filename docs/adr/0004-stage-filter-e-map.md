# ADR 0004: Assinatura do Stage, filter e map

- **Status:** aceito
- **Data:** 2026-11-25

## Contexto

A semana 2 cria a interface `Stage` e os dois primeiros stages: `filter`
(`--filter 'level==error'`) e `map` (`--map 'rename:msg=message'`). A
interface vale para todos os stages que vêm depois, então a assinatura precisa
servir para 1→0 (filtrar), 1→1 (transformar), 1→N (fan-out) e N→1 (agregar,
emitindo no fim do stream).

Números de `go test -bench . -benchmem ./internal/stage/filter` (i5-1135G7):

| Benchmark | ns/op | B/op | allocs/op |
|---|---|---|---|
| `Filter` (2 condições, tabela de operadores) | ~130 | 0 | 0 |
| `FilterAlt_DeepEqual` (só a comparação) | ~45 | 0 | 0 |
| `FilterAlt_RegexPerEvent` (`regexp.MustCompile` por evento) | ~1900 | 2112 | 18 |
| `FilterAlt_RegexPrecompiled` | ~130 | 0 | 0 |
| `Map` (`rename`, com `Clone`) | ~400 | 336 | 2 |

## Decisão

### Assinatura: `Process(e, emit) error`

```go
type Stage interface {
    Process(e event.Event, emit func(event.Event)) error
}
type Flusher interface { Flush(emit func(event.Event)) error } // opcional
```

- **`emit` em vez de `[]Event`.** Devolver um slice obriga a alocar um slice
  por chamada (ou a criar uma regra de posse para reusar). Com `emit`, o
  runner passa uma função criada **uma vez** por stage, que faz append num
  buffer reusado (`collector` em `internal/pipeline/step.go`). O `Filter`
  fica com zero alocações por evento, e o teste trava isso.
- **`Flush` é uma interface opcional.** Stages sem estado não precisam
  implementar nada. O runner verifica com type assertion (`s.(Flusher)`).
  O mesmo vale para `Idler`, `Concurrent` e `Namer`.
- **O Stage não conhece o runner.** A interface é declarada duas vezes, em
  `internal/pipeline` e em `internal/stage`, com o mesmo method set. Como
  interfaces em Go são estruturais, um `*filter.Filter` satisfaz as duas sem
  que `stage` importe `pipeline` (ADR 0001).

### O tipo do erro diz o que fazer

| Erro devolvido | O runner faz |
|---|---|
| `nil` | conta e envia o que foi emitido |
| erro comum | **erro do evento**: reporta no stderr, descarta o emitido, segue |
| erro com `Fatal() bool == true` (`stage.Fatal(err)`) | cancela o pipeline |
| erro com `Stop() bool == true` (`*stage.StopError`) | parada suave (ADR 0005) |

A classificação usa interfaces de comportamento via `errors.As`, não
sentinelas: o runner não precisa importar nenhum pacote de stage para saber o
que um erro significa.

### `filter`: tabela de operadores, não `reflect.DeepEqual`

- A expressão é `campo OP valor`, com `==`, `!=`, `<`, `<=`, `>`, `>=`,
  `=~` e `!~`. Vários `--filter` fazem E lógico. O parse acontece no `New`.
- A comparação é `stage.Compare`, que compara por tipo: números como
  `float64` (`n==10` casa com `10` e `10.0`), strings por ordem
  lexicográfica, bool e null. Tipos diferentes nunca são iguais: `s==10` é
  falso se `s` é a string `"10"`. Campo ausente `== null` é verdadeiro.
- `reflect.DeepEqual` é ~3× mais rápido **só na comparação**, mas dá a
  resposta errada: `json.Number("10")` e `json.Number("10.0")` são diferentes
  para ele, e ele não ordena. O custo de verdade do filtro está no `Get` do
  campo e no parse do número, não no `==`.

### `map`: cria um Event novo

- `Process` faz `e.Clone()` e aplica as operações na cópia. O evento recebido
  nunca muda (I4). O custo é medido: 2 alocações, 336 B, ~400 ns.
- Operações: `rename:a=b`, `copy:a=b`, `set:a=valor`, `delete:a`. O campo
  `time` não pode ser alterado (`copy:time=ts` pode): mudar `time` quebraria a
  invariante do ADR 0003 sem passar pelo parser.

### `regexp` compilada no `New`

Compilar a regex por evento é ~15× mais lento e aloca 18 vezes por evento
(`FilterAlt_RegexPerEvent`). A regex é compilada uma vez, no `New`, e fica no
`cond`. `*regexp.Regexp` é seguro para uso concorrente, então isso não impede
`--workers` (ADR 0012).

## Alternativas consideradas

- **`Process(e) ([]Event, error)`.** Rejeitada: aloca por chamada; o fan-out
  e o caso 1→1 pagariam o mesmo preço.
- **Stage com channels de entrada e saída.** Rejeitada: espalha concorrência
  por todos os stages e torna cada um responsável por fechar channels (I3).
- **Linguagem de expressão completa (OR, parênteses).** Adiada: o E lógico
  com flags repetidas cobre o uso do dia a dia; o parser fica pequeno.

## Consequências

- `TestStageContract` (em `internal/stage/stagetest`) roda para todo stage:
  não modifica a entrada, só emite eventos válidos, é determinístico e não usa
  erro fatal para entrada válida. `TestStage_NoSideEffects` (em
  `internal/config`) roda o mesmo contrato em **todo stage registrado**.
- `TestFilter_ZeroAllocs` falha se o `Filter` passar a alocar.
- Mutantes verificados: `!=` virando `==` em `filter` e `map` sem `Clone` são
  pegos (`scripts/mutate.sh`, ADR 0013).
- Escape analysis (`go build -gcflags=-m`): em `filter.go` e `mapper.go`,
  `emit does not escape`. O `Clone` é inlined no `Map.Process`, e o map novo
  vai para o heap porque é emitido e sobrevive à chamada (são as 2 alocações
  do benchmark).
