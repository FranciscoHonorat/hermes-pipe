# ADR 0009: Agregação por chave com teto de memória

- **Status:** aceito
- **Data:** 2026-11-25

## Contexto

A semana 7 responde "quem são os top N hosts/usuários/erros" sobre um arquivo
de qualquer tamanho. O stage `aggregate` agrupa por chave e emite um evento
por grupo no `Flush`, com as métricas `count`, `sum:f`, `min:f`, `max:f`,
`avg:f` e `pNN:f` (percentil). O problema é a memória: com milhões de chaves
distintas, um map sem limite cresce até o processo morrer.

Medição (`TestAggregate_Bounded` e um experimento sem teto, 1 milhão de chaves
distintas, `count` + `sum`, heap vivo lido de `/gc/heap/live:bytes` em
`runtime/metrics` depois de `runtime.GC()`):

| Teto | Heap vivo depois |
|---|---|
| 1.000 chaves | +412 KiB |
| sem teto (2 milhões) | +404 MiB (~423 B por chave) |

## Decisão

### LRU, com teto por número de chaves

- O estado é um `ds.Bounded[string, *stage.Group]` (ADR 0011): um map mais uma
  lista duplamente encadeada. `Get` move a chave para o fim; uma chave nova com
  o teto cheio despeja a menos usada recentemente.
- **LRU** porque o caso de uso é "top N": uma chave quente é acessada o tempo
  todo e nunca chega ao começo da lista (`TestAggregate_Eviction`). FIFO
  despejaria a chave quente só por ser antiga; LFU precisa de contadores e de
  uma estrutura de prioridade, que custa mais por evento e tem o problema de
  chaves antigas com contagem alta que nunca saem.
- O teto é por **número de chaves** (`max_keys`, padrão 100.000). Estimar
  bytes por chave depende do tamanho da chave e das métricas (~423 B no caso
  medido; o percentil guarda até 512 amostras por métrica). O teto por chaves
  é previsível para quem configura e testável sem flakiness. A conta de bytes
  fica documentada aqui: 100.000 chaves × ~0,5 KiB ≈ 50 MiB.

### O despejo é emitido, não perdido

O grupo despejado é emitido **na hora**, com `"evicted": true` e os valores
parciais. Nada some em silêncio, e o despejo depende só da ordem de entrada,
então é determinístico (I5). Quem quer só o top N usa `top:`, que se aplica
aos grupos residentes no `Flush`.

### Percentil com memória limitada

Cada métrica `pNN` guarda até `stage.MaxSamples = 512` valores. Depois disso
vira um reservatório (algoritmo R) com um xorshift de **semente fixa**.
`math/rand` é proibido nos stages (ADR 0008), e a semente fixa faz o mesmo
arquivo dar o mesmo percentil sempre. O resultado é aproximado acima de 512
valores, e `TestAggregate_PercentileBounded` verifica a tolerância e a
repetibilidade.

### Sem `sync.Map`, sem mutex

O stage roda numa goroutine só: `aggregate` não implementa `Concurrent`
(ADR 0012). Ninguém mais toca o map, então não há o que sincronizar. Um
`sync.Map` pagaria o custo de sincronização sem necessidade, e ainda é
otimizado para o caso de muitas leituras com poucas escritas, o oposto daqui.

### Ordem de saída

O `Flush` ordena com `slices.SortStableFunc`: pela primeira métrica
(decrescente) quando há `top`, e sempre com desempate pela chave. A iteração do
map nunca decide a ordem. O `ds.Bounded.All()` percorre a lista, não o map, e
mesmo assim a saída é ordenada explicitamente.

### Como testar o teto sem flakiness

- `TestAggregate_Bounded` mede o **heap vivo** depois de `runtime.GC()`, não o
  `HeapAlloc` (que inclui lixo não coletado), e compara com uma margem larga
  (1 KiB por chave + 4 MiB). Sem teto a diferença é de três ordens de grandeza,
  então a margem não esconde nada.
- O teste confere também `Len() == max_keys` e `Evicted()`, que são exatos.

## Alternativas consideradas

- **Contagem aproximada (Count-Min Sketch, Space-Saving).** Rejeitada por
  ora: dá memória constante, mas a saída deixa de ser exata. O LRU com despejo
  emitido mantém a exatidão para as chaves residentes.
- **Teto em bytes.** Rejeitada pelo motivo acima; pode voltar se as chaves
  forem muito irregulares.

## Consequências

- `BenchmarkAggregate` com 10 a 10.000 chaves: ~100–170 ns por evento, 0
  alocações em regime (a chave já existe; a string da chave de 1 campo não é
  copiada).
- O que o roadmap pede sobre GC fica registrado aqui como estudo, sem mudar o
  código: o heap de 404 MiB do experimento sem teto é exatamente o cenário em
  que `GOMEMLIMIT` faz o GC trabalhar mais perto do limite em vez de deixar o
  heap dobrar (`GOGC=100`). Com o teto, o heap vivo fica abaixo de 1 MiB e os
  dois modos se comportam igual.
- Mutante: iterar o map sem ordenar no `Flush` seria pego pelo analisador
  (regra `order`, ADR 0008).
