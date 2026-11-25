# hermes-pipe

Processador de stream local para logs em JSON Lines, escrito em Go 1.27 só
com a stdlib (mais uma lib YAML). Valida, filtra, transforma, deduplica e
agrega eventos, com saída **determinística**: o mesmo arquivo dá a mesma saída,
byte a byte, com 1 ou 8 workers.

O projeto segue o [roadmap.md](roadmap.md): o pipeline é a desculpa, e o
objetivo é entender Go por baixo. Cada decisão está num ADR em
[`docs/adr/`](docs/adr/).

## Instalação

```sh
go install github.com/FranciscoHonorat/hermes-pipe/cmd/hermes-pipe@latest
# ou, do repositório:
make build      # bin/hermes-pipe
make release    # build reproduzível: -trimpath, CGO_ENABLED=0
```

## Uso

Cada linha da entrada precisa ser um objeto JSON com `"time"` em RFC 3339.

```sh
hermes-pipe [flags] < entrada.jsonl > ok.jsonl 2> erros.jsonl
hermes-pipe run -c pipeline.yaml [flags]
hermes-pipe schema      # o schema YAML aceito
hermes-pipe version
```

- **stdout**: os eventos válidos que saíram do pipeline, um JSON por linha,
  com as chaves ordenadas.
- **stderr**: os eventos rejeitados, em JSON, na ordem da entrada, e no fim
  uma linha `{"summary":{...}}` com os contadores.

Os exemplos abaixo usam [`examples/logs.jsonl`](examples/logs.jsonl): 43
linhas, 3 delas inválidas de propósito. Todos rodam no teste end-to-end
(`cmd/hermes-pipe/main_test.go`), e as saídas completas estão em
[`cmd/hermes-pipe/testdata/golden/`](cmd/hermes-pipe/testdata/golden/).

### Validar: separar linhas boas das ruins

```sh
hermes-pipe < examples/logs.jsonl > ok.jsonl 2> bad.jsonl
```

`bad.jsonl` recebe uma linha por rejeição, com o número da linha e o motivo:

```json
{"line":6,"stage":"parse","error":"campo \"time\" ausente","raw":"{\"level\":\"error\",\"host\":\"web-01\",\"msg\":\"sem time\"}"}
{"line":13,"stage":"parse","error":"JSON inválido: invalid character 'i' looking for beginning of value","raw":"isto não é json"}
{"line":21,"stage":"parse","error":"campo \"time\" inválido: \"ontem\"","raw":"{\"time\":\"ontem\",\"level\":\"info\"}"}
{"summary":{"read":43,"emitted":40,"dropped":0,"errored":3,"generated":0}}
```

O resumo sempre fecha a conta: `read + generated = emitted + dropped + errored`.

### Filtrar e transformar

```sh
hermes-pipe --filter 'level==error' --map 'rename:msg=message' < examples/logs.jsonl
```

```json
{"host":"web-03","latency_ms":84,"level":"error","message":"slow query","req_id":"r2","time":"2026-03-01T12:00:14Z"}
{"host":"db-01","latency_ms":195,"level":"error","message":"upstream timeout","req_id":"r2","time":"2026-03-01T12:00:35Z"}
...
```

- `--filter 'campo OP valor'`: `==`, `!=`, `<`, `<=`, `>`, `>=`, `=~`
  (regex), `!~`. Repetir a flag faz E lógico. Números comparam como números,
  strings em ordem lexicográfica, e tipos diferentes nunca são iguais.
  Campos aninhados com ponto: `http.status>=500`.
- `--map`: `rename:a=b`, `copy:a=b`, `set:a=valor`, `delete:a`. O campo
  `time` não pode ser alterado.
- `--limit N`: para depois de N eventos, como o `head`.

### Pipelines em YAML

Top 3 hosts com mais erros, com a latência p95
([`examples/erros-por-host.yaml`](examples/erros-por-host.yaml)):

```yaml
version: 1
stages:
  - type: filter
    where: ["level == error"]
  - type: aggregate
    by: [host]
    metrics: [count, "p95:latency_ms"]
    top: 3
```

```sh
hermes-pipe run -c examples/erros-por-host.yaml < examples/logs.jsonl
```

```json
{"count":4,"host":"web-03","p95_latency_ms":472,"time":"2026-03-01T12:04:26Z"}
{"count":3,"host":"db-01","p95_latency_ms":195,"time":"2026-03-01T12:03:23Z"}
{"count":3,"host":"web-01","p95_latency_ms":306,"time":"2026-03-01T12:03:44Z"}
```

Outros exemplos:
[`erros-por-minuto.yaml`](examples/erros-por-minuto.yaml) (contagem por host
em janelas de 1 minuto) e [`limpar.yaml`](examples/limpar.yaml) (remove
retries duplicados, renomeia e corta).

Uma config errada falha **antes do primeiro evento**, com linha e coluna:

```
hermes-pipe: pipeline.yaml:4:5: campo desconhecido "wher" (campos: where)
```

## Stages

| Stage | Faz | Campos |
|---|---|---|
| `filter` | deixa passar o que satisfaz todas as expressões | `where: [...]` |
| `map` | transforma campos | `ops: [...]` |
| `limit` | para depois de N eventos | `n` |
| `aggregate` | agrupa por chave e emite no fim | `by`, `metrics`, `top`, `max_keys` |
| `window` | agrega por chave em janelas `[início, fim)` do tempo do evento | `size`, `lateness`, `by`, `metrics`, `max_open`, `max_keys` |
| `dedup` | remove repetidos dentro de uma janela de tempo | `by`, `window`, `max_keys` |

Métricas: `count`, `sum:campo`, `min:campo`, `max:campo`, `avg:campo`,
`pNN:campo` (percentil; aproximado acima de 512 valores por chave).

Todo stage com estado tem **memória limitada**: `max_keys` e `max_open`. Um
grupo despejado pelo teto é emitido na hora com `"evicted": true`, nunca
perdido em silêncio. Na `window`, um evento que chega depois que a janela dele
fechou (watermark = maior tempo visto − `lateness`) vai para o stderr como
erro.

O schema completo sai de `hermes-pipe schema`.

## Flags comuns

| Flag | Padrão | |
|---|---|---|
| `--workers N` | 1 | goroutines por stage sem estado (`filter`, `map`); a saída não muda |
| `--buffer N` | 64 | capacidade dos channels entre stages |
| `--max-line N` | 1 MiB | linha maior que isso é rejeitada (o resto continua) |
| `--idle-flush D` | 0 | fecha janelas depois de D sem eventos (para stdin ao vivo; desligado em replay) |
| `--stats` | | inclui tempo, eventos/s, buffer, workers e `GOMAXPROCS` no resumo |
| `--quiet` | | não imprime o resumo |
| `-v` | | logs operacionais (JSON, stderr) a partir de INFO |

`--workers` só acelera quando os stages paralelos são caros: o parse de JSON
(~6 µs por linha) é serial e vira o gargalo. Os números estão no
[ADR 0012](docs/adr/0012-workers-e-backpressure.md).

## Códigos de saída e Ctrl+C

| Código | Quando |
|---|---|
| 0 | terminou sem eventos rejeitados (inclui `--limit` atingido) |
| 1 | terminou, mas houve eventos rejeitados |
| 2 | flag ou config inválida |
| 3 | erro fatal durante a execução |
| 130 | interrompido |

Ctrl+C uma vez: para de ler, drena o que estava em trânsito, fecha as janelas
e imprime o resumo. Duas vezes: para na hora.

## Arquitetura

```
stdin ──▶ Producer ──chan──▶ Stage ──chan──▶ … ──chan──▶ Consumer ──▶ stdout
                                                              └──────▶ stderr (rejeitados + resumo)
```

```
cmd/hermes-pipe  →  internal/config  →  internal/pipeline, internal/stage/*  →  internal/event
                                                           internal/stage/* →  internal/ds
cmd/hermes-pipe-lint  →  internal/lint
```

- **Stage é uma função pura com estado opcional**: `Process(e, emit) error`.
  Não conhece channels, contexto, relógio nem I/O.
- **O runner (`internal/pipeline`) é o único lugar com concorrência.** Ele cria
  e fecha os channels, conta os eventos, propaga cancelamento e preserva a
  ordem. O runner sequencial (`RunSequential`) é o oráculo dos testes.
- As camadas são verificadas por `architecture_test.go` (`go list -deps`) e
  pelo analisador próprio `hermes-pipe-lint`, que proíbe nos stages relógio de
  parede, aleatoriedade, channels, `context.Context`, `io.Writer` e `range`
  sobre map sem ordenar.

### Invariantes

Cada uma tem testes que falham quando ela é violada; `TestInvariants` falha
se algum desses testes sumir.

| | Invariante |
|---|---|
| I1 | conservação: `lidos + gerados = emitidos + descartados + com_erro` |
| I2 | sem vazamento: quando `Run` retorna, nenhuma goroutine dele vive |
| I3 | cada channel é fechado uma vez, só por quem envia |
| I4 | stage sem channel, contexto, relógio, I/O; não modifica o evento recebido |
| I5 | mesma entrada → mesma saída, byte a byte |
| I6 | agregação, janela e dedup têm teto de memória |
| I7 | eventos saem na ordem de entrada |
| I8 | consumer lento desacelera o producer |
| I9 | shutdown ordenado: producer → stages → consumer |

## Desenvolvimento

```sh
make test         # go test -race ./...
make lint         # golangci-lint
make lint-arch    # hermes-pipe-lint ./internal/stage/...
make cover-check  # cobertura de internal/ >= 80%
make bench        # benchmarks -count=10 em bench.txt
make fuzz-all     # todos os alvos de fuzz por FUZZTIME (padrão 30s)
make stress       # go test -race -count=50 ./...
make mutate       # 10 mutantes: cada um precisa derrubar algum teste
```

Os goldens são atualizados com `go test ./... -update` e revisados no diff do
PR.

O CI (`.github/workflows/ci.yml`) roda em push e PR: testes com `-race` e
`go mod tidy`, golangci-lint, `govulncheck`, o analisador de arquitetura,
cobertura mínima e, nos PRs, `benchstat` contra a base.

## Documentação

| ADR | Tema |
|---|---|
| [0001](docs/adr/0001-nome-modulo-e-layout.md) | nome, módulo, layout e camadas |
| [0002](docs/adr/0002-politica-de-dependencias.md) | dependências e CI |
| [0003](docs/adr/0003-event-parser-e-saida-de-erros.md) | `Event`, parser e saída de erros |
| [0004](docs/adr/0004-stage-filter-e-map.md) | assinatura do Stage, `filter` e `map` |
| [0005](docs/adr/0005-runner-cancelamento-e-limit.md) | runner, cancelamento, shutdown e `limit` |
| [0006](docs/adr/0006-buffers-ordem-e-contadores.md) | buffers, ordem e contadores |
| [0007](docs/adr/0007-config-yaml-e-cli.md) | config YAML e CLI |
| [0008](docs/adr/0008-analisador.md) | o analisador `hermes-pipe-lint` |
| [0009](docs/adr/0009-agregacao-e-teto-de-memoria.md) | agregação e teto de memória |
| [0010](docs/adr/0010-janela-e-watermark.md) | janela e watermark |
| [0011](docs/adr/0011-dedup-e-generics.md) | dedup e generics |
| [0012](docs/adr/0012-workers-e-backpressure.md) | workers e backpressure |
| [0013](docs/adr/0013-determinismo-e-testes-adversariais.md) | determinismo e testes adversariais |
| [0014](docs/adr/0014-build-e-release.md) | build reproduzível e release |
