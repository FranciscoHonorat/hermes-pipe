# ADR 0013: Determinismo e testes adversariais

- **Status:** aceito
- **Data:** 2026-11-25

## Contexto

A semana 12 transforma a suíte numa rede de segurança comprovada: provar que o
pipeline inteiro é determinístico (I5) e que os testes pegam bugs de verdade.

## Decisão

### O que conta como "mesma execução"

Mesma entrada, mesma config e mesma versão do hermes-pipe dão a **mesma saída
byte a byte**, independente de:

- `GOMAXPROCS` e número de núcleos;
- `--workers` e `--buffer`;
- ordem de escalonamento das goroutines;
- iteração aleatória de maps.

**Fora do contrato:** a versão do Go (a formatação de `float64` ou de erros da
stdlib pode mudar entre versões) e o idle-flush, que depende de tempo real e é
desligado em replay (ADR 0010). O resumo com parada suave por `--limit`
também fica fora: o `read` depende de quanto o producer leu adiantado; os
**eventos e erros** emitidos não.

### De onde vem o determinismo

| Fonte de não determinismo | Como foi neutralizada |
|---|---|
| Iteração de map | `json.Marshal` ordena as chaves; `Flush` ordena com `slices.SortStableFunc` e desempate pela chave; analisador proíbe `range` sobre map sem ordenar (ADR 0008) |
| Escalonamento | um channel FIFO por elo; reordenação explícita com workers (ADR 0012); erros em banda (ADR 0005) |
| Relógio de parede | proibido nos stages (ADR 0008); janelas e dedup usam o tempo do evento |
| Aleatoriedade | proibida nos stages; o reservatório do percentil usa xorshift de semente fixa |
| Despejo por teto | LRU depende só da ordem de entrada (ADR 0009) |

### Como foi provado

- **Golden byte a byte:** `TestPipeline_Determinism` roda cada pipeline de
  `internal/config/testdata/pipelines/` no `RunSequential`, compara com o
  golden e depois roda `Run` com 1, 2 e 8 workers × buffers 0, 1 e 64,
  3 vezes cada. Tudo tem de bater com o sequencial.
- **Oráculo duplo:** `TestPipeline_DifferentialVsSequential` compara com o
  runner sequencial; `TestPipeline_Oracle` compara com uma implementação
  independente, escrita sem olhar o pipeline (laço direto com `encoding/json`
  sobre o arquivo).
- **Propriedades:** `TestProperty_Conservation` (200 casos gerados: I1 vale e
  `Run` = `RunSequential`), `TestProperty_NoLeak` (100 casos com hard cancel
  em ponto gerado, dentro do `synctest`) e `FuzzPipeline`. Os geradores usam
  `rand.New(rand.NewPCG(seed, ...))`, e toda falha imprime a seed.
- **Tabela de invariantes:** `TestInvariants` (na raiz) mapeia I1–I9 para os
  testes e falha se algum desses testes for renomeado ou apagado.

### Por que golden byte a byte e não `reflect.DeepEqual`

`reflect.DeepEqual` tem armadilhas que o golden não tem:

- `NaN != NaN`: dois resultados idênticos com NaN são "diferentes";
- slice/map `nil` e vazio são diferentes para ele, mas serializam igual em
  muitos casos (e diferente em outros: `null` vs `[]`);
- `json.Number("10")` e `json.Number("10.0")` são diferentes, mesmo valendo o
  mesmo número;
- em estruturas grandes ele custa caro, e o diff de uma falha é ilegível.

O golden compara exatamente o que o usuário recebe, e uma falha mostra o diff
em texto.

### Revisar um diff de golden num PR

Golden muda só com `-update`, e o diff entra no PR. A revisão pergunta: a
mudança na saída é intencional e explicada no PR? Um golden que muda junto com
código que "não deveria mudar comportamento" é um bug.

### Quais invariantes viram propriedade

I1 (conservação), I2 (sem vazamento) e I5/I7 (igual ao oráculo) viram
propriedades, porque valem para **qualquer** entrada e pipeline e são baratas
de verificar. I6 (memória) e I8 (backpressure) dependem de medição de heap ou
de bloquear o consumer, então ficam como testes de exemplo com números
escolhidos.

### Cobertura mínima: 80%

O job `cover` do CI roda `make cover-check` sobre `internal/` com
`-coverpkg=./internal/...` (conta o que os testes de outros pacotes exercitam).
Hoje está em **94,1%**. Cobertura de linha diz só que a linha **rodou**, não
que algum teste verificou o resultado. Uma linha coberta pode estar errada sem
que nada falhe. Por isso existe a mutação.

### Mutação: 10 mutantes

`scripts/mutate.sh` (`make mutate`) aplica um bug por vez, roda os testes do
pacote e exige que falhem. Os 10 cobrem runner (close esquecido, contador),
stages (`!=` vira `==`, `map` sem `Clone`, `limit` sem parada, watermark que
recua), event (off-by-one, `time:null`), config (campo desconhecido aceito) e
o analisador (falso positivo em método). Resultado: **10/10 mortos**.

Na primeira rodada, um mutante sobreviveu: era um mutante **equivalente**
(ADR 0010). A correção foi simplificar o código e mirar o mutante na condição
que importa, não "achar um teste que mate".

## Alternativas consideradas

- **Ferramenta de mutação externa.** Rejeitada pelo ADR 0002; o script cobre
  os 10 mutantes escolhidos à mão, onde o bug é plausível.
- **Cobertura de 100%.** Rejeitada: o resto são getters e mensagens de erro;
  o custo não compra qualidade.

## Consequências

- Os jobs agendados (stress noturno com `-count=100` e mutação semanal) não
  estão no CI por decisão explícita, para não gastar minutos de Actions sem
  commits. `make stress` e `make mutate` rodam localmente.
- Qualquer fonte nova de não determinismo (map, relógio, scheduler) quebra
  pelo menos um entre analisador, golden e diferencial.
