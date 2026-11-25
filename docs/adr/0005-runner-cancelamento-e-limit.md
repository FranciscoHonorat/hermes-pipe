# ADR 0005: Runner concorrente, cancelamento, shutdown e limit

- **Status:** aceito
- **Data:** 2026-11-25

## Contexto

Na semana 3 o laço sequencial vira um pipeline concorrente, sem mudar nenhum
Stage:

```go
func Run(ctx context.Context, p Producer, stages []Stage, c Consumer, opts ...Option) (Stats, error)
```

A estrutura é uma goroutine para o producer, uma por stage (ou N, ADR 0012) e
o consumer na goroutine de quem chamou. Cada par vizinho é ligado por um
channel. O runner sequencial (`RunSequential`) continua existindo e é o
**oráculo**: `Run` precisa produzir a mesma saída, byte a byte.

Perguntas: como distinguir parada suave e dura, erro de evento e erro fatal,
o que fazer com pânico, quem é dono da leitura bloqueante do stdin e quais são
os códigos de saída.

## Decisão

### Dois níveis de cancelamento

| | Parada **suave** | Parada **dura** |
|---|---|---|
| Causa | `WithStop(ctx)` cancelado (1º Ctrl+C) ou Stage devolve `Stop()` (`limit`) | `ctx` de `Run` cancelado (2º Ctrl+C) ou erro fatal |
| Producer | para de ler | para |
| Em trânsito | drenado até o consumer | descartado |
| `Flush` | roda, em ordem | não roda |
| Retorno | `nil`, `Stats.Stopped` = causa | a causa (`context.Cause`) |

Implementação: `hard, hardCancel := context.WithCancelCause(ctx)` e
`soft := context.WithCancelCause(hard)`. Só o producer olha `soft`; todo
envio faz `select` em `hard.Done()`. Um `context.AfterFunc(stopCtx, ...)` liga
o `WithStop` externo ao `soft` interno sem uma goroutine extra.

### O `limit` não entra em pânico nem cancela nada

`limit` emite o N-ésimo evento e devolve `limit.ErrLimitReached`, um
`*stage.StopError`. O runner traduz isso em parada suave **com causa**. O stage
não conhece contexto (I4); só devolve um erro que diz "chega".

O que o producer leu adiantado chega ao `limit` depois e é descartado (conta
como `dropped`, I1 continua valendo). O stage que pediu a parada também
descarta os erros de linhas lidas adiantadas (ver "erros em banda").

### Erros de evento andam no stream, em ordem

Os eventos rejeitados (de parse ou de stage) **não** vão direto ao stderr:
viram um `item` com `rej != nil` e seguem pelos mesmos channels que os
eventos, até o consumer, que os entrega ao `ErrorSink`.

A primeira versão reportava direto. O `TestCLI_EndToEnd` pegou o problema:
com `--limit`, as linhas inválidas que o producer lia adiantado eram
reportadas ou não, dependendo do scheduler. A saída de erros não era
determinística. Com os erros em banda:

- o stderr sai na ordem da entrada, igual ao `RunSequential`;
- o stage que pediu a parada descarta tudo o que chega depois dele, inclusive
  rejeitados. O diferencial compara **também** o stderr, sem ordenar.

### Pânico num Stage vira erro fatal

`step.process` tem `defer recover()` e transforma o pânico em
`*pipeline.PanicError{Stage, Value, Stack}`, que tem `Fatal() == true`. Depois
de um pânico, o estado do stage é desconhecido; continuar daria saída errada
em silêncio. O processo não morre: o runner cancela, fecha tudo, imprime o
resumo e sai com código 3.

### stdin: a leitura mora no `AsyncProducer`

`read(2)` num terminal ou pipe parado não é cancelável: fechar o `os.Stdin`
não desbloqueia uma leitura em andamento. Se o producer do runner lesse direto,
o Ctrl+C esperaria a próxima linha.

O `cmd` envolve o stdin num `AsyncProducer`. A goroutine dele faz a leitura
bloqueante e entrega por um channel; `Next(ctx)` faz `select` entre o channel
e `ctx.Done()`. Quando `Run` retorna, essa goroutine **pode continuar viva**,
bloqueada no `read`. Ela pertence ao `AsyncProducer`, não ao runner, e morre
com o processo. É a única exceção a I2, e por isso só o `cmd` usa
`AsyncProducer`. Os testes de vazamento usam producers em memória.

### Códigos de saída

| Código | Quando |
|---|---|
| 0 | terminou, nenhum evento rejeitado (inclui `--limit` atingido) |
| 1 | terminou, mas houve eventos rejeitados |
| 2 | uso ou config inválida: nenhum evento foi lido |
| 3 | erro fatal na execução (stage, consumer, pânico) |
| 130 | interrompido por SIGINT (128 + 2), suave ou duro |

`--limit` atingido é sucesso, como no `head`.

### Ferramentas usadas

`sync.WaitGroup.Go` (1.25) para lançar e esperar as goroutines;
`errgroup` foi lido, mas não é dependência (ADR 0002): ele cancela no
primeiro erro, e aqui um erro de evento **não** pode cancelar.

## Alternativas consideradas

- **Um contexto só.** Rejeitada: não há como pedir "pare de ler, mas termine
  o que começou".
- **`limit` com `panic` + `recover` no runner.** Rejeitada: usa pânico como
  controle de fluxo, e o recover passaria a ter de diferenciar pânico bom e
  ruim.
- **Recuperar o pânico e seguir.** Rejeitada pelo motivo acima.
- **Fechar o stdin no Ctrl+C.** Rejeitada: não desbloqueia o `read`.

## Consequências

- Testes: `TestPipeline_DifferentialVsSequential` (8 cenários × 4 buffers × 4
  números de workers, stdout **e** stderr iguais ao oráculo),
  `TestPipeline_ShutdownGraceful`, `TestPipeline_NoLeak` e
  `TestPipeline_CancelAtEveryStep` dentro de `synctest.Test`,
  `TestPipeline_FlushOnEOF`, `TestPipeline_FatalAndPanic`, `TestCLI_SIGINT`.
- O `synctest` detecta goroutine presa porque, dentro da bolha, o runtime sabe
  quando **todas** as goroutines estão *durably blocked* (bloqueadas em
  channel, `sync.Cond`, `time.Sleep` etc. criados na bolha). Se `synctest.Test`
  termina com alguma ainda bloqueada, o teste falha.
- Um bug real foi pego pelos testes na primeira execução: a closure de cada
  goroutine de stage capturava a variável `in`, reatribuída no laço, e todos
  os stages liam do último channel. A correção (copiar `in` antes do
  `wg.Go`) está comentada em `run.go`.
