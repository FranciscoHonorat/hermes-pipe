# ADR 0006: Buffers, ordem e contadores

- **Status:** aceito
- **Data:** 2026-11-25

## Contexto

A semana 4 pergunta o que acontece por dentro do runner: qual capacidade de
channel usar, por que a ordem é preservada, como contar sem que os stages
saibam, e quando o concorrente ganha do sequencial.

Medições de `BenchmarkPipeline_Buffer` e `BenchmarkPipeline_StageCost`
(`internal/pipeline/bench_test.go`): 10.000 eventos, 3 stages, i5-1135G7
(4 núcleos, 8 threads), `-count=5`. Os valores são a faixa típica; a variação
entre execuções foi de ±10–15%.

| Configuração (3 stages baratos) | eventos/s |
|---|---|
| `RunSequential` | ~225 mil |
| `Run`, buffer 0 | ~130 mil |
| `Run`, buffer 1 | ~145 mil |
| `Run`, buffer 16 | ~180 mil |
| `Run`, buffer 64 | ~205 mil |
| `Run`, buffer 256 | ~215 mil |
| `Run`, buffer 1024 | ~215 mil |

| Custo de cada stage (rodadas de SHA-256) | sequencial | concorrente |
|---|---|---|
| 0 | ~230 mil/s | ~205 mil/s |
| 1 | ~215 mil/s | ~185 mil/s |
| 10 | ~140 mil/s | ~150 mil/s |
| 100 | ~27 mil/s | ~50 mil/s |

## Decisão

### Buffer padrão: 64

- Com buffer 0, cada envio é um *rendezvous*: o emissor só segue quando o
  receptor pega o valor, e quase todo envio passa pelo caminho lento do
  `chan.go` (estacionar a goroutine num `sudog`, acordar a outra).
- A partir de ~64 o ganho satura (256 e 1024 ficam dentro do ruído). 64
  pega quase todo o ganho e limita a memória em trânsito.
- `--buffer` e `buffer:` no YAML mudam o valor.

**Pior caso de eventos em trânsito** no shutdown, com S stages, buffer B e W
workers:

```
(S + 1)·B          eventos nos channels (S+1 channels)
+ S + 2            um evento "na mão" de cada goroutine (producer, stages, consumer)
+ S·(2W + 1)       só com W > 1: buffer de reordenação (2W) + dispatcher, por stage
```

`TestPipeline_Backpressure` verifica essa fórmula: com o consumer parado, o
producer nunca lê mais do que ela.

### Ordem (I7) em termos de happens-before

Para um stage com uma goroutine só:

1. O producer envia os itens na ordem de leitura num channel. Um channel é
   FIFO: se o envio de `a` acontece antes do envio de `b` na mesma goroutine,
   o recebimento de `a` acontece antes do de `b`.
2. A goroutine do stage recebe `a`, chama `Process(a)` e envia as saídas de
   `a` **antes** de receber `b` (ordem de programa).
3. Pelo modelo de memória, o envio num channel *happens-before* a conclusão do
   recebimento correspondente. Encadeando as etapas, todas as saídas de `a`
   chegam ao consumer antes de qualquer saída de `b`.
4. O `close(out)` de cada goroutine acontece depois do último envio dela
   (ordem de programa). O `for range` do vizinho só termina depois de receber
   o último valor e depois o fechamento, e é aí que ele chama `Flush`. Por
   isso o `Flush` de um stage vê todos os eventos anteriores, e as saídas
   dele vêm depois de todas as outras.

Com W > 1 workers a ordem é restaurada explicitamente (ADR 0012).

### Contadores: atômicos e invisíveis aos stages

- `Counters` tem cinco `atomic.Int64` (`read`, `emitted`, `dropped`,
  `errored`, `generated`). Cada um é escrito por goroutines diferentes
  (producer, stages, consumer). Um mutex serializaria todas elas num lock só;
  o `atomic.Add` não bloqueia ninguém.
- Só o runner incrementa: depois de cada `Process`, ele conta pelo número de
  eventos emitidos (0 → `dropped`, N > 1 → `generated += N−1`). O stage não
  sabe que é contado.
- `Snapshot()` lê cada campo atomicamente, mas o conjunto não é uma foto
  consistente com o pipeline rodando. I1 só é verificado depois do fim.
- Com os contadores trocados por `int64` comum (experimento feito e
  desfeito), `go test -race -count=1 -run DifferentialVsSequential` já
  reporta `DATA RACE`: essa suíte faz o producer e os stages escreverem ao
  mesmo tempo em todos os cenários. O race detector só vê o que executou, e
  aqui a escrita concorrente executa em todo teste.

### Concorrente vs. sequencial

O concorrente só ganha quando o trabalho por stage é maior que o custo de
passar um evento por um channel (algumas centenas de ns, incluindo acordar a
goroutine). Pela tabela, o ponto de virada está entre 1 e 10 rodadas de
SHA-256, ou seja, alguns microssegundos por stage. Para filtros e maps
simples, o sequencial é mais rápido.

O runner concorrente continua sendo o padrão porque dá o que o sequencial não
dá: Ctrl+C com dreno (ADR 0005), idle-flush (ADR 0010), `--workers` (ADR 0012)
e sobreposição de I/O com processamento.

## Alternativas consideradas

- **Buffer 0 por "pureza".** Rejeitada pelos números.
- **Buffer 1024.** Rejeitada: o mesmo throughput que 64, com 16× mais memória
  em trânsito no pior caso.
- **Contadores com mutex.** Rejeitada: serializa goroutines que não têm outra
  relação entre si.
- **Contadores por stage passados ao Stage.** Rejeitada: viola I4.

## Consequências

- `make stress` (`-race -count=50`) passa.
- `FuzzPipeline` gera entrada, stages, buffer, workers e ponto de cancelamento
  a partir de uma seed, dentro do `synctest`.
- CPU profile (`go test -bench 'Pipeline_Buffer/buf=0$' -cpuprofile`, depois
  `go tool pprof -top`): com buffer 0, o topo é `runtime.futex` (~8%),
  `runtime.selectgo` (~7%), `lock2` e `sellock`. São goroutines estacionando e
  acordando a cada envio. O `selectgo` aparece porque todo envio do runner é
  um `select` entre o channel e `hard.Done()`. Com buffer 64, o `futex` sai do
  topo e sobem a alocação (`mallocgc*`) e o decode de JSON (no 1.27, o
  `encoding/json` roda sobre `encoding/json/jsontext`). O `selectgo` continua
  no topo, com metade da fatia: é o preço do cancelamento em todo envio.
