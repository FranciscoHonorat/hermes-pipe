# ADR 0012: Workers, ordem e backpressure

- **Status:** aceito
- **Data:** 2026-11-25

## Contexto

A semana 11 adiciona `--workers N`: mais de um núcleo processando, com a
**mesma saída** de 1 worker, e sem estourar a memória quando o destino é lento.
O paralelismo mora no runner, nunca no Stage (I4).

Medição de `BenchmarkPipeline_Parallel` (10.000 eventos, um stage caro de 400
rodadas de SHA-256 por evento, ~50 µs; i5-1135G7, 4 núcleos e 8 threads;
3 execuções):

| Workers | Tempo para 10 mil eventos | Ganho |
|---|---|---|
| 1 | ~510–530 ms | 1× |
| 2 | ~310–340 ms | ~1,6× |
| 4 | ~210–340 ms | ~2× |
| 8 | ~180–220 ms | ~2,7× |

Com um stage de 50 rodadas (~7 µs), o ganho foi **nenhum**: 1, 2, 4 e 8
workers ficaram em ~110–130 mil eventos/s.

## Decisão

### Quem pode rodar em paralelo

Só stages que implementam `Concurrent() bool` com `true`: `filter` e `map`,
que não têm estado mutável depois do `New` (a regex compilada é segura para
uso concorrente). `limit`, `aggregate`, `window` e `dedup` têm estado e rodam
numa goroutine só, mesmo com `--workers`.

`--workers` (ou `workers:` no YAML) vale para todos os stages `Concurrent` do
pipeline. O padrão é 1.

### Ordem preservada com buffer de reordenação limitado

Para um stage paralelo, o runner cria:

```
in ──▶ dispatcher ──jobs──▶ W workers
            │                    │ (cada um responde no seu canal de resultado)
            └──order (cap 2W)──▶ coletor ──▶ out
```

O dispatcher, para cada evento, cria um canal de resultado com buffer 1,
coloca esse canal em `order` **antes** de entregar o trabalho e só então
manda o job. O coletor lê `order` em sequência e espera cada resultado. A saída
sai na ordem da entrada mesmo que os workers terminem fora de ordem (I7).

Eventos rejeitados (ADR 0005) entram em `order` já resolvidos, sem passar por
worker, e saem no lugar certo.

### Backpressure (I8)

A capacidade de `order` (2W) é o buffer de reordenação **e** o freio: se o
coletor não anda (consumer lento), `order` enche, o dispatcher bloqueia, o
channel de entrada enche, e assim até o producer, que para de ler. A conta do
pior caso está no ADR 0006. `TestPipeline_Backpressure` verifica, dentro do
`synctest` e **por contadores**, que com o consumer parado o producer nunca
lê mais do que essa conta.

### Por que 8 workers dão ~2,7× e não 8×

- A máquina tem 4 núcleos físicos; as 8 threads são hyperthreading.
- **Amdahl:** o parse de JSON roda serial na goroutine do producer, a ~6 µs
  por linha (ADR 0003). Com o stage de 50 µs dividido por 8, cada worker
  leva ~6 µs por evento, o mesmo que o parse. Daí em diante o parse é o
  gargalo. Com o stage de 7 µs, o parse já era o gargalo com 1 worker, e por
  isso o ganho foi zero.
- Conclusão prática: `--workers` só ajuda quando o custo por evento dos stages
  paralelos é bem maior que o do parse. Para `filter` e `map` simples, deixe
  em 1.

### Particionar stages com estado por chave: não

Daria paralelismo ao `aggregate`/`dedup`, mas exige N instâncias, roteamento
por hash da chave e uma fusão determinística das saídas no `Flush`. Com o
parse como gargalo, o ganho seria pequeno para muita complexidade. Fica
registrado como a próxima otimização se um stage com estado virar o gargalo.

### Lotes (`[]Event` por job): não

O overhead por evento do modo paralelo (um channel de resultado alocado e
dois envios a mais) só pesa quando o stage é barato, e nesse caso o parse já
domina. Não foi medido isoladamente; sem um número que mostre ganho, a
complexidade não entra (ADR 0014).

### Um worker falha

Erro de evento: segue em banda, na ordem. Erro fatal ou pânico: o runner
cancela tudo (parada dura). Esperar os outros workers terminarem não faz
sentido, porque a saída não seria emitida de qualquer forma.

### `GOMAXPROCS`

Desde o 1.25, o padrão de `GOMAXPROCS` respeita o limite de CPU do cgroup.
Com mais workers que `GOMAXPROCS`, as goroutines extras só disputam os mesmos
Ps: o resultado continua correto (o teste roda com 8 workers em qualquer
máquina) e o throughput não sobe. O resumo com `--stats` imprime `gomaxprocs`
para deixar isso visível.

## Alternativas consideradas

- **Sem reordenação (saída na ordem de término).** Rejeitada: quebra I7 e o
  diferencial contra o sequencial.
- **Parse paralelo no lugar do producer.** Boa candidata para atacar o
  gargalo medido; fica para uma versão futura, porque exige mudar o contrato
  entre producer e runner.

## Consequências

- `TestPipeline_DifferentialVsSequential` roda com 1, 2, 4 e 8 workers e
  exige stdout e stderr iguais ao `RunSequential`.
- `TestPipeline_Determinism` (em `internal/config`) roda todo pipeline de
  `testdata/pipelines` com 1, 2 e 8 workers contra o golden.
- `make stress` (`-race -count=50`) passa.
