# ADR 0011: Dedup e generics

- **Status:** aceito
- **Data:** 2026-11-25

## Contexto

A semana 10 adiciona o `dedup`: remove eventos repetidos (retries, logs
duplicados) dentro de uma janela de tempo, deixando passar a primeira
ocorrência, com memória limitada. Ele precisa da mesma estrutura com teto do
`aggregate`, e o `window` precisa de um ring buffer. Agora existe duplicação
real, e é a hora de extrair estruturas genéricas.

Números de `go test -bench . -benchmem ./internal/ds ./internal/stage/dedup`:

| Benchmark | ns/op | allocs/op |
|---|---|---|
| `Ring/generic-int` (`Ring[int]`) | ~11,5 | 0 |
| `Ring/concrete-int` (`RingOfInt` escrito à mão) | ~9,5 | 0 |
| `Ring/generic-ptr` (`Ring[*ev]`) | ~7 | 0 |
| `Bounded/1000` (Get + Put em LRU) | ~120 | 1 (só na inserção) |
| `Dedup` | ~280 | 0 |

## Decisão

### Estruturas genéricas em `internal/ds`

- `ds.Bounded[K comparable, V any]`: map + lista duplamente encadeada
  intrusiva com sentinela, despejo LRU, `Get` (marca uso), `Peek` (não marca),
  `Put` (devolve o despejado), `Oldest`, `RemoveOldest`, `All() iter.Seq2[K, V]`.
  Usado por `aggregate`, `window` (chaves por janela) e `dedup`.
- `ds.Ring[T any]`: fila circular de capacidade fixa, com
  `All() iter.Seq[T]`. Usado pelo `window`.
- `internal/ds` é uma camada nova: os stages podem importar, e `ds` não importa
  nada do módulo (`architecture_test.go`).

### Onde generics **não** entram

- Nas interfaces do pipeline (`Stage`, `Producer`, `Consumer`): todos trafegam
  `event.Event`, não há um segundo tipo para parametrizar. `Stage[T]`
  espalharia o parâmetro pelo runner sem nenhum uso.
- Nos stages: cada stage é um tipo concreto.
- Só em `register[C, S]` (ADR 0007), onde elimina um construtor escrito à mão
  por stage.

### GC shape stenciling: o que os números mostram

O compilador gera **uma** instância de código por *GC shape* e passa um
dicionário com as informações de tipo. Todos os tipos ponteiro têm o mesmo
shape, então `Ring[*Event]` e `Ring[*string]` compartilham código. `int` e
`int64` têm shapes diferentes do ponto de vista do compilador e ganham
instâncias separadas.

`Ring[int]` ficou ~20% mais lento que o `RingOfInt` concreto (11,5 vs 9,5 ns;
operações de poucos nanossegundos, então a diferença absoluta é ~2 ns). No
caminho real, o custo é dominado pelo parse (~6 µs por evento, ADR 0003), e a
diferença some.

### Chave por hash de 64 bits

- O `dedup` guarda `hash(chave) → tempo da primeira ocorrência`, com FNV-64a.
  8 bytes por entrada, em vez da chave inteira.
- **Colisão:** duas chaves diferentes com o mesmo hash viram "duplicadas" e a
  segunda é descartada. Pelo paradoxo do aniversário, com n chaves na memória a
  chance de alguma colisão é ~n²/2⁶⁵: para o teto padrão de 100.000, ~3×10⁻¹⁰.
  O hash é injetável (`NewWithHasher`), e `TestDedup_ForcedCollision` usa um
  hash constante para provar o efeito.
- **Bug de determinismo vs. bug de corretude:** com um hash de 8 bits
  (experimento feito e desfeito), o `TestDedup_Deterministic` **continua
  passando**, porque duas execuções colidem do mesmo jeito. O
  `TestDedup_FirstWins` **também passou**: ele usa só 2 chaves, e elas não
  colidiram. Era um buraco na suíte. Daí nasceu o
  `TestDedup_DistinctKeysAllPass` (1.000 chaves distintas, todas precisam
  passar), que com 8 bits falha: só 256 passam. Determinismo diz que a saída
  se repete; corretude diz que ela está certa. Um não implica o outro.

### Janela no tempo do evento

A janela de dedup usa `Event.Time`, como o `window`. Com relógio de parede,
reprocessar o arquivo daria resultado diferente conforme a velocidade da
leitura. A distância é **absoluta**: um repetido fora de ordem, com tempo
anterior ao da primeira ocorrência, também é duplicado. Um duplicado não
renova a lembrança (`Peek`, não `Get`): a janela conta da primeira ocorrência.

### Iterador ou slice

`All()` devolve `iter.Seq`/`iter.Seq2`: o chamador percorre sem alocar um slice
intermediário, e pode parar no meio. O compilador reescreve o corpo do
`for range` como uma função `yield`. Quando o corpo faz `break`, o `yield`
devolve `false`, e o iterador **precisa** parar (se não parar, o runtime entra
em pânico). `TestRingContract` e `TestBounded_AllBreak` testam isso.

## Alternativas consideradas

- **Guardar a chave inteira.** Rejeitada: memória proporcional ao tamanho da
  chave, para evitar uma colisão com probabilidade desprezível.
- **`container/list` + map.** Rejeitada: `container/list` guarda `any`, o que
  custa uma alocação e um type assertion por acesso.
- **Um `Ring` por tipo, sem generics.** Rejeitada: era a duplicação que esta
  semana existe para eliminar.

## Consequências

- `testRingContract[T]` roda para `int`, `string` e `*ev`.
- `TestDedup_Bounded` (o teto esquece a chave mais antiga) e
  `TestDedup_Expires` (entradas fora da janela liberam memória).
- `aggregate`, `window` e `dedup` rodam sobre as estruturas genéricas, com os
  benchmarks acima.
