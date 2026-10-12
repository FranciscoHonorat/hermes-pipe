# hermes-pipe: roadmap de 14 semanas

**Um processador de stream local em Go 1.27. Escopo pequeno, profundidade máxima.**

O pipeline é a desculpa; o objetivo é **entender Go por baixo**: runtime, scheduler, channels, alocador, GC, maps, interfaces, generics. O gate de cada semana não é "funciona", é "sei explicar o que aconteceu na memória, no scheduler e no heap".

**Cada semana vale sozinha.** Se você parar no fim de qualquer semana, tem (1) um binário que funciona e faz algo útil que não fazia antes, (2) um tópico de Go que explica sem consulta, (3) testes e ADR que fecham o que foi feito. Nenhuma semana é "preparação para a próxima".

**Calendário:** semanas 0 a 13. A semana 8 é folga.
**Fonte da verdade:** todo código do runtime e da stdlib é lido **na tag `go1.27.2`** (`$(go env GOROOT)/src`). Se um caminho citado mudou, ache o novo e anote no ADR.

---

## A arquitetura

```
            ┌──────────────────────── runner (internal/pipeline) ───────────────────────┐
            │  dono dos channels, do contexto, dos contadores, do Clock e do shutdown   │
            │                                                                           │
 stdin ──▶ Producer ──chan──▶ Stage ──chan──▶ Stage ──chan──▶ … ──chan──▶ Consumer ──▶ stdout
            │                    │                                                    │
            │                    └──── eventos com erro ──────────────────────────────┼──▶ stderr
            └───────────────────────────────────────────────────────────────────────────┘
```

**Camadas** (nenhuma seta ao contrário):

```
cmd/hermes-pipe  →  internal/config  →  internal/pipeline, internal/stage/*  →  internal/event
```

- **Stage é uma função pura com estado opcional.** Recebe um `Event`, emite zero ou mais, ou devolve erro. Não conhece channels, contexto, config, relógio nem I/O. Stages com estado têm `Flush` no fim do stream.
- **O runner é o único lugar com concorrência.** Ele cria e fecha os channels, propaga cancelamento, conta eventos e drena na ordem producer → stages → consumer.
- **Stages não importam `pipeline` nem uns aos outros.** `cmd/hermes-pipe` é o único lugar que liga tudo.

A arquitetura cresce de dentro para fora: semana 1 é `event` + `cmd`; semana 2 adiciona `stage` e um runner sequencial; semana 3 troca o runner por um concorrente **sem mudar nenhum Stage**. O runner sequencial nunca é apagado: vira o oráculo dos testes diferenciais.

---

## Regras do jogo

1. **Toda semana termina com `main` verde, o binário funcionando e um exemplo novo no README** que roda no teste end-to-end.
2. **Teste vermelho primeiro.** Table-driven por padrão; golden em `testdata/golden/` (flag `-update`); `FuzzXxx` para toda função que lê bytes.
3. **Contrato antes de implementação.** Cada interface (`Producer`, `Stage`, `Consumer`, …) tem um teste que *qualquer* implementação precisa passar.
4. **Arquitetura testada.** `go list -deps` garante as camadas; o analisador da semana 6 garante o que `go list` não vê.
5. **Quebre de propósito.** Toda semana, introduza 3 bugs à mão. Se nenhum teste falha, falta teste.
6. **Medir antes de prometer.** Caminho quente tem `BenchmarkXxx` com `b.Loop()` e `b.ReportAllocs()`. Números vão para `PERFORMANCE.md`; o CI compara com `main` via `benchstat`.
7. **Uma decisão, um ADR.** As perguntas de cada semana são respondidas em `docs/adr/` antes do código que depende delas.

---

## Invariantes

Cada uma nasce na semana indicada e, dali em diante, tem um teste que falha quando ela é violada.

| ID | Invariante | Semana | Teste |
|---|---|---|---|
| I1 | **Conservação:** `lidos = emitidos + descartados + com_erro` | 1 | `TestCLI_Conservation`, `TestPipeline_Conservation` |
| I2 | **Sem vazamento:** quando `Run` retorna, nenhuma goroutine vive | 3 | `TestPipeline_NoLeak` |
| I3 | **Posse:** cada channel é fechado uma vez, só por quem envia | 3 | `TestPipeline_CloseOnce` |
| I4 | **Isolamento:** Stage sem channel, contexto, relógio, I/O; não modifica o Event recebido | 2, 6 | `TestStage_NoSideEffects`, `TestArchitecture_Rules` |
| I5 | **Determinismo:** mesma entrada → mesma saída, byte a byte | 7, 12 | `TestPipeline_Determinism` |
| I6 | **Memória limitada:** estado de agregação, janela e dedup tem teto | 7, 9, 10 | `TestAggregate_Bounded`, `TestDedup_Bounded` |
| I7 | **Ordem:** eventos saem na ordem de entrada (quando configurado) | 3, 11 | `TestPipeline_DifferentialVsSequential` |
| I8 | **Backpressure:** consumer lento desacelera o producer | 11 | `TestPipeline_Backpressure` |
| I9 | **Shutdown ordenado:** dreno producer → stages → consumer | 3 | `TestPipeline_ShutdownGraceful` |

---

## Semana 0 — Fundação

**Valor isolado:** um template de repositório Go com CI que pega erro de verdade, reutilizável em qualquer projeto.

**Construir:** `go.mod` (`go 1.27`), `cmd/hermes-pipe` com `main` vazio, `Makefile` (`test`, `lint`, `bench`, `fuzz`, `stress`), `.golangci.yml`, CI com jobs separados: teste com `-race`, lint, `govulncheck`, benchmark com `benchstat`.

**Por baixo:** diretivas `go` e `toolchain`, `GOTOOLCHAIN=local|auto`; o que o compilador faz com `internal/`; `go help test`, `go help testflag`. Release notes do 1.24 ao 1.27.

**Decidir (ADR 0001, 0002):** nome e módulo; `internal/` vs. `pkg/`; política de dependências (uma lib YAML e mais nada); por que lint e teste são jobs separados.

**Provar:** `architecture_test.go` roda `go list -deps` e falha com import proibido.

**Gate:** CI verde; um PR com dependência proibida falha pelo motivo certo; `go version` no CI imprime 1.27.x.

**Explicar:** o que o `govulncheck` faz que o Dependabot não faz? Quem baixou a toolchain 1.27, e para onde?

---

## Semana 1 — Validador de JSON Lines

**Valor isolado:** `hermes-pipe < in.jsonl > ok.jsonl 2> bad.jsonl` separa linhas válidas das inválidas, com o motivo, e imprime um resumo. Já é uma ferramenta útil para limpar logs.

**Construir:** o tipo `Event`, `ParseEvent([]byte) (Event, error)`, `Validate() error`, e um `main` sequencial: lê linha, valida, escreve. Invariante no godoc: `Time` sempre preenchido; sem timestamp é erro, não zero.

**Por baixo:** `encoding/json/decode.go` (`float64` vs. `json.Number`, reuso de buffer); status do `encoding/json/v2` no 1.27; `time.Time` com relógio monotônico e de parede; `bufio.Reader`/`Scanner` e o limite de 64 KB; `io/io.go` (contratos de `Reader`/`Writer`: leitura curta, `n > 0` com erro); `bufio.Writer` e o `Flush` esquecido.

**Decidir (ADR 0003):**
1. `map[string]any` ou `map[string]json.RawMessage`? Meça.
2. `Time` como `time.Time` ou `int64`? Meça.
3. `Event` por valor ou ponteiro? O que muda na alocação?
4. Tamanho máximo de linha e o que acontece acima dele: erro do evento ou fatal?
5. Formato da saída de erros: linha original + motivo em JSON?

**Provar:** I1 desde já. `TestParseEvent` com 20 linhas (vazia, `{}`, `{"time":null}`, UTF-8 inválido, 1 MB, `\r\n`…); `FuzzParseEvent`; `BenchmarkParseEvent` comparando `Unmarshal`, `Decoder`, `Decoder.Token` (e v2); `TestCLI_EndToEnd` (binário compilado no `TestMain`, `os/exec`, golden). `internal/event` só importa stdlib.

**Quebrar:** off-by-one no slice; campo obrigatório ignorado; esquecer o `Flush` do `bufio.Writer`.

**Gate:** 20 casos; fuzz 5 min limpo; end-to-end com golden; benchmark em `PERFORMANCE.md`.

**Explicar:** onde exatamente o `ParseEvent` aloca, e como você mediu? O que o contrato de `io.Reader` permite que surpreende quem não leu o godoc?

---

## Semana 2 — `filter` e `map`: um jq mínimo

**Valor isolado:** `hermes-pipe --filter 'level==error' --map 'rename:msg=message'` filtra e transforma logs. Útil no dia a dia, ainda sem concorrência.

**Construir:** a interface `Stage` e seu contrato, `filter`, `map`, e `RunSequential` (um laço `for` que passa cada evento pelos stages). Esse laço é guardado para sempre como oráculo.

**Por baixo:** escape analysis com `-gcflags=-m`; `runtime/iface.go` (itab, cache, custo do dispatch, devirtualização e PGO); method sets; `sync.Pool`; `strings.Builder` vs. `bytes.Buffer`. Russ Cox, *Go Data Structures: Interfaces*.

**Decidir (ADR 0004):**
1. Assinatura do Stage: `Process(Event, emit func(Event)) error` ou `[]Event`? Qual aloca menos? Como fica `Flush`?
2. `Filter` compara com `==`, `reflect.DeepEqual` ou tabela de operadores? Meça.
3. `Map` cria um Event novo ou modifica o recebido? Como isso respeita I4?
4. `regexp` compilada onde, e por quê?

**Provar:** I4. `TestStageContract` para os dois; 50 casos table-driven; `TestStage_NoSideEffects` (input intacto); `testing.AllocsPerRun` trava o orçamento medido (meta: 0 no `Filter`). Stages não importam `pipeline` nem uns aos outros.

**Quebrar:** `Filter` modifica o Event; regex compilada por evento (meça o fator); `reflect.DeepEqual` no lugar de `==` (meça).

**Gate:** orçamento de alocação travado no CI; 3 mutantes mortos.

**Explicar:** por que uma variável local vai para o heap? Onde exatamente está o custo de uma chamada via interface, e quando o compilador o elimina?

---

## Semana 3 — Runner concorrente, `limit` e Ctrl+C

**Valor isolado:** o mesmo CLI, agora em pipeline concorrente, com `--limit N` que para cedo (como `head`) e Ctrl+C que drena e imprime o resumo. Nenhum Stage mudou.

**Construir:** `Run(ctx, Producer, []Stage, Consumer) (Stats, error)`; contratos de `Producer` e `Consumer`; `limit`, que devolve o sentinela `ErrLimitReached` e o **runner** traduz em soft cancel com causa. `signal.NotifyContext`: 1º SIGINT soft, 2º hard.

**Por baixo:** `context/context.go` (árvore de cancelamento, `WithCancelCause`, `Cause`, `AfterFunc`); `sync/waitgroup.go` e `WaitGroup.Go`; `errgroup` (leitura, não dependência); `testing/synctest` e por que a bolha detecta goroutine presa; `errors.Is` com sentinelas; por que fechar stdin não desbloqueia a leitura.

**Decidir (ADR 0005):**
1. Soft cancel (para de ler e drena) vs. hard cancel (para tudo): como o runner distingue?
2. Erro de evento (conta e segue) vs. erro fatal (cancela): como o tipo distingue?
3. Pânico num Stage: recuperar onde, e virar o quê?
4. Leitura de stdin não é cancelável: quem é dono dessa goroutine quando `Run` retorna?
5. Códigos de saída: 0, 1, 2, e o de `ErrLimitReached`.

**Provar:** I2, I3, I7, I9. `TestPipeline_DifferentialVsSequential` (= `RunSequential`, byte a byte); `TestPipeline_ShutdownGraceful`; `TestPipeline_NoLeak` em `synctest.Test`; `TestPipeline_CancelAtEveryStep`; `TestPipeline_FlushOnEOF` (uma vez, nunca após hard cancel); `TestCLI_SIGINT` (I1 vale no resumo). `internal/pipeline` só importa `event`.

**Quebrar:** esquecer um `close`; fechar do lado de quem recebe; `time.Sleep` no lugar de sincronização; `limit` com `panic`.

**Gate:** diferencial verde; invariantes verdes em todos os pontos de cancelamento; 3 mutantes mortos.

**Explicar:** por que só quem envia fecha o channel? Como o `synctest` sabe que uma goroutine está *durably blocked*?

---

## Semana 4 — Por dentro do runner: scheduler, channels, memória

**Valor isolado:** `hermes-pipe --stats` mostra throughput e contadores; o buffer padrão é escolhido por número, não por palpite; você responde com dados se o pipeline concorrente é mais rápido que o sequencial, e quando.

**Construir:** contadores ao vivo (atomics) expostos em `--stats`; `BenchmarkPipeline_Buffer` e a tabela buffer × throughput em `PERFORMANCE.md`.

**Por baixo:** `runtime/proc.go` (G, M, P, runq, work stealing, netpoller); `runtime/chan.go` (`hchan`, `sudog`, caminho rápido e lento); `go.dev/ref/mem` (happens-before de send, recv, close, atomics); `atomic.Int64` vs. mutex; CPU profile com `pprof`. Vyukov, *Scalable Go Scheduler Design Doc*.

**Decidir (ADR 0006):**
1. Buffer 0, 1 ou N? Escreva a fórmula do pior caso de eventos em trânsito no shutdown.
2. Argumento formal de ordem (I7) em termos de happens-before.
3. Contadores: atomic ou mutex, e como ficam invisíveis aos stages?
4. Concorrente vs. sequencial: para qual custo de Stage o concorrente ganha?

**Provar:** `FuzzPipeline` com cancelamento aleatório; benchmarks com buffer 0, 1, 16, 256, 1024; `make stress` (`-race -count=50`).

**Quebrar:** contador `int` comum (com que `-count` o `-race` pega?); inverter envio e incremento.

**Gate:** stress verde; tabela em `PERFORMANCE.md`; profile de CPU explicado.

**Explicar:** desenhe o happens-before entre o último envio de um Stage e o `close` do runner. O que o `chan.go` faz quando um envio encontra um receptor esperando?

---

## Semana 5 — Pipelines declarativos em YAML

**Valor isolado:** `hermes-pipe run -c pipeline.yaml` com pipelines versionáveis e reutilizáveis; config errada falha **antes do primeiro evento**, citando linha e coluna.

**Construir:** YAML estrito → pipeline via registro de construtores; `FileProducer`; `slog` para logs operacionais.

**Por baixo:** decodificação estrita e posições de nó na lib YAML; `flag` com subcomandos; `log/slog` (handlers, níveis, onde escreve).

**Decidir (ADR 0007):** lista linear de stages (por que não grafo?); como um stage valida a própria config sem conhecer YAML; as flags da semana 2 continuam como atalho ou saem?; onde o `slog` escreve se stdout é dado e stderr é erro.

**Provar:** 25 casos de config com golden de mensagens; os contratos de `Producer`/`Consumer` contra os reais; `FuzzConfig`. `internal/stage/*` não importa `config`.

**Quebrar:** stage desconhecido ignorado; campo desconhecido aceito; evento com erro no stdout.

**Gate:** todo exemplo do README em YAML roda no end-to-end; 3 mutantes mortos.

**Explicar:** por que o registro de construtores mantém `config` e `stage` desacoplados?

---

## Semana 6 — O analisador: arquitetura que o `go list` não vê

**Valor isolado:** um analisador estático próprio, só com stdlib, que roda sobre qualquer código Go e impõe as regras do projeto. Saber escrever um é útil em qualquer base de código.

**Construir:** `cmd/hermes-pipe-lint` com três regras para `internal/stage/...`:
1. Não chama `time.Now`, `time.Since`, `time.After`, `math/rand` (I4, I5).
2. Não declara nem recebe `chan`, `context.Context`, `*os.File`, `io.Writer` (I4).
3. `range` sobre map só com chaves ordenadas na mesma função, ou `//hermespipe:unordered-ok <motivo>` (I5).

**Por baixo:** `go/parser`, `go/ast`, `go/types`, `go/importer`; por que `time.Now` só é identificável com tipos (alias de import); como o `go vet` organiza analisadores (leitura do `copylock`). Alan Donovan, *go/types: The Go Type Checker*.

**Decidir (ADR 0008):** "é um Stage" por package ou por tipo? Cada regra prefere falso positivo ou negativo? Escape hatch com motivo obrigatório?

**Provar:** por regra, um arquivo ruim e um bom em `testdata/src/` com `// want "..."`; `TestAnalyzer_Aliases` (`import t "time"`); `TestArchitecture_Rules` roda sobre `internal/` e passa.

**Quebrar:** `time.Now` via alias no `Filter`; `range` sem ordenar no `Map`; remover o type checking (qual teste cai?).

**Gate:** 3 regras verdes sobre o código real; job no CI; 3 mutantes mortos.

**Explicar:** por que análise só sintática não basta? O que uma regra heurística garante e o que não garante?

---

## Semana 7 — `aggregate`: contagem por chave com memória limitada

**Valor isolado:** `hermes-pipe` responde "quem são os top N hosts/usuários/erros" sobre um arquivo de qualquer tamanho, sem estourar a memória, mesmo com milhões de chaves distintas.

**Construir:** agregação por chave (`count`, `sum`, `percentile`) emitida no `Flush`, com **teto de memória** e despejo determinístico e contado.

**Por baixo:** `runtime/malloc.go` (`mcache`, `mcentral`, `mheap`, size classes); `runtime/mgc.go` (tricolor, write barrier, mark assist, pacer, Green Tea GC); Swiss tables em `internal/runtime/maps` (grupos, control word, crescimento); `runtime/metrics` vs. `ReadMemStats`; `GOGC` e `GOMEMLIMIT`; memory profile (`inuse_space` vs. `alloc_space`). *A Guide to the Go Garbage Collector*; blog *Faster Go maps with Swiss Tables*.

**Decidir (ADR 0009):** LRU, LFU ou FIFO? Teto por chaves ou por bytes (como estimar bytes/chave)? Por que nem `sync.Map` nem mutex, se o estado mora numa goroutine só? Como testar o teto sem flakiness?

**Provar:** I6. `TestAggregate_Bounded` (1 milhão de chaves, heap vivo ≤ teto + margem, via `runtime/metrics` após `runtime.GC()`); `TestAggregate_Eviction` (chave quente sobrevive); saída ordenada e determinística, inclusive o despejo; `BenchmarkAggregate` com 10…10000 chaves.

**Quebrar:** remover o teto e rodar 10 milhões de chaves (documente o desastre); iterar o map sem ordenar (o analisador pega?).

**Gate:** gráfico heap × cardinalidade; pausas de GC com `GOGC=100` vs. `GOGC=off`+`GOMEMLIMIT`; 3 mutantes mortos.

**Explicar:** como a marcação tricolor funciona e o que o write barrier protege? Como uma Swiss table acha uma chave?

---

## Semana 8 — Folga

**Valor isolado:** o que foi feito até aqui fica sólido.

Nenhuma feature. Feche gates pendentes. Releia os ADRs 0001–0009 contra o código. Rode todos os mutantes de novo. Responda os "Explicar" das semanas 0–7 em voz alta, sem consulta; onde travar, volte à fonte.

**Gate:** nenhum gate aberto; ADRs coerentes com o código.

---

## Semana 9 — `window`: agregação no tempo

**Valor isolado:** "quantos erros por host por minuto", com resultado idêntico toda vez que você reprocessa o mesmo arquivo, mesmo com eventos fora de ordem.

**Construir:** janela por tempo do evento em volta do `aggregate`. `watermark = max(tempo visto) − tolerância`, nunca recua. A janela fecha quando o watermark passa do fim dela ou no `Flush`. O relógio de parede mora **no runner** (`Clock` injetado, para idle-flush opcional), desligado no replay.

**Por baixo:** `time/time.go` (por que `==` é armadilha e `Equal`/`Compare` não); timers no runtime e o que mudou no 1.23; `synctest` para simular 1 hora de silêncio; ring buffer. Akidau, *Streaming 101/102*; Kleppmann, *DDIA* cap. 11.

**Decidir (ADR 0010):** `[início, fim)` ou `[início, fim]`? Evento atrasado: descarta e conta, ou erro? Como isso fecha I1? Idle-flush existe, e como é desligado no replay?

**Provar:** I5. 20 casos de borda; `TestWindow_Determinism`; `TestWindow_ReorderWithinTolerance` (propriedade); `TestRunner_IdleFlush` em `synctest`; `TestClockContract`; `FuzzWindow`. O analisador garante que `window` não tem relógio.

**Quebrar:** `time.Now` no `Window`; limite errado; watermark que recua.

**Gate:** determinismo verificado; idle-flush testado sem tempo real; 3 mutantes mortos.

**Explicar:** o que é watermark e por que ele é o que torna a janela determinística?

---

## Semana 10 — `dedup` e generics: quando a duplicação é real

**Valor isolado:** `hermes-pipe` remove eventos repetidos (retries, logs duplicados) dentro de uma janela de tempo, com memória limitada. E o código fica menor: três stages passam a compartilhar uma estrutura genérica.

**Construir:** `dedup` (primeira ocorrência passa). Ele precisa da mesma estrutura com teto do `aggregate`, e o `window` já tem um ring buffer: **agora** existe duplicação real. Extraia `Bounded[K comparable, V any]` e `Ring[T]` com `All() iter.Seq[T]`.

**Por baixo:** GC shape stenciling com dicionários (por que `Ring[*Event]` e `Ring[*string]` compartilham código e `Ring[int]` e `Ring[int64]` não); `comparable` e o que mudou no 1.20; iteradores e como o compilador reescreve `range` sobre função. Design doc *GC Shape Stenciling*; blog *When To Use Generics*, *Range Over Function Types*.

**Decidir (ADR 0011):** chave de dedup por hash ou valor (probabilidade e efeito de colisão)? Por que a janela de dedup tem de ser tempo do evento? Iterador ou slice: quem aloca? Onde generics **não** entram (interfaces do pipeline, stages)?

**Provar:** `TestDedup_FirstWins`, `TestDedup_Bounded`, `TestDedup_ForcedCollision` (hash injetado); `testRingContract[T]` para `int`, `string`, `*Event`; `BenchmarkRing[int]` vs. `RingOfInt` concreto; um teste com `break` no `range` sobre `All()`.

**Quebrar:** hash de 8 bits (o teste de determinismo **passa**, o de corretude cai: entenda por quê); iterador que ignora o `false` do `yield`.

**Gate:** `aggregate`, `window` e `dedup` sobre as estruturas genéricas sem regressão no benchmark.

**Explicar:** o que é um GC shape? Qual a diferença entre um bug de determinismo e um de corretude, no seu código?

---

## Semana 11 — `--workers N`: paralelismo, backpressure, race

**Valor isolado:** `hermes-pipe --workers 8` processa mais rápido em máquina com vários núcleos, com a mesma saída de 1 worker, e não estoura a memória quando o destino é lento.

**Construir:** paralelismo dentro de um estágio, no runner (nunca no Stage). Ordem preservada por número de sequência + reordenação com buffer limitado. Stages com estado particionados por chave.

**Por baixo:** `sync/mutex.go` (modo normal e modo fome); `sync/once.go` (fast path atômico + mutex, por que não é um `bool` com mutex) e `OnceFunc`/`OnceValue`; `atomic.CompareAndSwap`, `Load`/`Store` e a seção de atomics do modelo de memória; `GOMAXPROCS` e o padrão sensível a cgroup (1.25); race detector (shadow memory, vector clocks, só vê o que executou); block/mutex profile; `runtime/trace` e `trace.FlightRecorder`. Paper *ThreadSanitizer* (2009).

**Decidir (ADR 0012):** quantos workers, e configurável onde? Particionar stages com estado por chave? Lotes (`[]*Event`) valem? Um worker falha: cancela todos ou espera?

**Provar:** I7 e I8. `TestPipeline_Backpressure` em `synctest` (por contadores, não por tempo); `TestPipeline_ParallelDeterminism` (N workers = 1 worker = `RunSequential`); `BenchmarkPipeline_Parallel` com 1, 2, 4, 8.

**Quebrar:** worker escreve num map compartilhado (o `make stress` pega?); remover o buffer de reordenação; workers > `GOMAXPROCS`.

**Gate:** stress verde; throughput × workers; p50/p99 extraídos do trace.

**Explicar:** onde exatamente a backpressure acontece no seu código? O que o race detector não consegue ver? Por que `sync.Once` é preferível a um `bool` protegido por mutex?

---

## Semana 12 — Determinismo e testes adversariais

**Valor isolado:** a suíte de testes vira uma rede de segurança comprovada: você sabe, por mutação, que ela pega bugs, e qualquer regressão de saída aparece como diff de golden no PR.

**Construir:** prova de que o pipeline inteiro é determinístico e de que os testes são bons.

**Por baixo:** por que a iteração de map é aleatória e como as Swiss tables a randomizam; `slices.SortStableFunc` e desempate; `encoding/json` ordena chaves de map; `reflect.DeepEqual` e suas armadilhas (NaN ≠ NaN, map/slice nil vs. vazio, ciclos, custo) e por que golden byte a byte não sofre delas; geradores com `math/rand/v2` e `rand.NewPCG` (seed impressa em toda falha); shrinking simples; como o fuzz guarda e minimiza o corpus; o que cobertura mede e o que não mede.

**Decidir (ADR 0013):** o que entra em "mesma execução" (versão de Go? `GOMAXPROCS`?); como revisar um diff de golden num PR; quais invariantes viram propriedade e por que as outras não; cobertura mínima.

**Provar:**
- `TestPipeline_Determinism`: todo pipeline de `testdata/pipelines/`, golden byte a byte, com 1 e N workers.
- `TestPipeline_Oracle`: contra `RunSequential` e contra uma implementação independente, escrita sem olhar o pipeline.
- `TestProperty_Conservation`, `TestProperty_NoLeak` com entradas e cancelamentos gerados.
- `TestInvariants`: tabela I1–I9 → teste; falha se alguma ficar sem teste.
- CI: `stress` noturno (`-race -count=100`), `cover` com mínimo de 80% em `internal/`, mutação semanal (10 mutantes cobrindo runner, stages e config).

**Quebrar:** os 10 mutantes, um a um. Um flake no stress é bug: ache a causa.

**Gate:** I5 byte a byte; oráculo verde; 10/10 mutantes mortos.

**Explicar:** como você *provou* que a saída é determinística? Por que cobertura de linha não é qualidade? Dê dois casos em que `reflect.DeepEqual` responde diferente do que você espera.

---

## Semana 13 — v1.0

**Valor isolado:** um binário versionado e reproduzível que outra pessoa baixa, roda e confia, com CLI e schema estáveis.

**Construir:** build reproduzível, README completo, `PERFORMANCE.md` consolidado. A API pública é **a CLI e o schema YAML** (todo o código é `internal/`).

**Por baixo:** `-trimpath`, `-buildvcs`, versão embutida pelo VCS (1.24) e `debug.ReadBuildInfo`; quando `-ldflags -X` ainda é necessário; `GOOS`/`GOARCH`/`CGO_ENABLED=0`; `go version -m`. Blog *Perfectly Reproducible, Verified Go Toolchains*.

**Decidir (ADR 0014):** versão por VCS ou `-ldflags`; quais otimizações ficaram (só as com número) e quais foram descartadas.

**Provar:** `TestBinary_Version`; `TestBinary_Reproducible` (dois builds, mesmo hash); `TestCLI_Stable` (golden de `--help` e do schema).

**Quebrar:** renomear uma flag ou campo do YAML; compilar sem `-trimpath` e comparar hashes.

**Gate:** `v1.0.0`; todos os ADRs; todos os "Explicar" respondidos em voz alta sem consulta; um post de 2000 palavras sobre a decisão mais profunda do projeto.

---

## O que o binário faz, semana a semana

| Sem. | Novo no binário | Novo na sua cabeça |
|---|---|---|
| 0 | CI | toolchain, módulos |
| 1 | valida JSON Lines, separa erros | `encoding/json`, `bufio`, `io` |
| 2 | `--filter`, `--map` | interfaces, escape analysis |
| 3 | pipeline concorrente, `--limit`, Ctrl+C limpo | `context`, `WaitGroup`, `synctest` |
| 4 | `--stats`, buffer medido | scheduler, `chan.go`, modelo de memória |
| 5 | `-c pipeline.yaml` | decodificação estrita, registro |
| 6 | `hermes-pipe-lint` | `go/ast`, `go/types` |
| 7 | top N por chave, memória limitada | alocador, GC, Swiss tables |
| 9 | contagem por janela de tempo | tempo, watermark, timers |
| 10 | `dedup` | generics, iteradores |
| 11 | `--workers N` | mutex, `sync.Once`, CAS, race detector, trace |
| 12 | determinismo provado | propriedade, fuzz, mutação, `reflect.DeepEqual` |
| 13 | `v1.0.0` reproduzível | build, versionamento |

## ADRs

| ADR | Tema | Sem. |
|---|---|---|
| 0001–0002 | Nome, layout, CI, dependências, toolchain | 0 |
| 0003 | `Event`, parser, saída de erros | 1 |
| 0004 | Assinatura do Stage, `filter`, `map` | 2 |
| 0005 | Runner, cancelamento, shutdown, `limit` | 3 |
| 0006 | Buffers, ordem, contadores | 4 |
| 0007 | Config e CLI | 5 |
| 0008 | Analisador | 6 |
| 0009 | Agregação e teto de memória | 7 |
| 0010 | Janela e watermark | 9 |
| 0011 | Dedup e generics | 10 |
| 0012 | Worker pool e backpressure | 11 |
| 0013 | Determinismo e testes adversariais | 12 |
| 0014 | Build e release | 13 |

---

## O que este projeto não é

Não é HTTP, banco, nuvem, Docker, segurança nem distribuição. A v1.0 é um build local reproduzível. **É sobre entender Go:** ao final, você explica modelo de memória, scheduler, channels, contexto, alocador, GC, Swiss tables, interfaces, escape analysis, generics, iteradores, race detector, pprof, trace e `go/types`, e tem um método (TDD, contratos, arquitetura testada, mutação, benchmark) para qualquer problema em Go.
