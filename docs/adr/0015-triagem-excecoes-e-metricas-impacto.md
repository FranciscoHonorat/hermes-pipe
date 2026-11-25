# ADR 0015: Triagem de exceções (human-in-the-loop) e métricas de impacto

- **Status:** aceito
- **Data:** 2026-11-25

## Contexto

Em telemetria industrial (Indústria 5.0), filtrar e descartar falhas não basta.
O operador precisa de:

1. **menos fadiga cognitiva:** anomalias classificadas e consolidadas, com o
   que exige julgamento humano marcado de forma explícita;
2. **indicadores de eficiência e sustentabilidade** por janela: energia por
   peça boa, refugo, retrabalho;
3. tudo isso **sem abrir mão de I1–I9**: stages puros, determinismo entre 1 e
   N workers, memória com teto e conservação dos contadores.

## Decisão

### 1. Stage `triage`, puro e sem estado

`internal/stage/triage` avalia regras ordenadas e anexa ao evento o objeto:

```json
"triage": {"decision": "human_review", "rule": "degradacao_latencia_cobo",
           "reason": "Latência crítica…", "requires_ack": true}
```

- `decision` é um de `auto_ok`, `human_review` ou `quarantine`.
- As regras são avaliadas de cima para baixo e **a primeira que casa vence**.
  Uma regra casa quando todas as expressões de `where` são verdadeiras.
- Se nenhuma regra casa, vale `default_decision` (vazio = `auto_ok`), com
  `rule` e `reason` em `null` e `requires_ack` em `false`.
- Na config, `name` precisa ser único e não vazio, `where` não pode ser vazio,
  `decision` precisa ser válido e `reason` é obrigatório (é o texto que o
  operador lê). Tudo isso é validado no `Load`, antes do primeiro evento.
- **Todo evento sai exatamente uma vez**, na ordem de entrada. O triage não
  descarta nada, não segura evento e não faz I/O. Quem roteia é um `filter`
  depois dele (`triage.decision != auto_ok`) ou o consumidor do stdout. I1 e
  I7 não mudam.
- Ele é `Concurrent`: escala com `--workers`, e a saída é idêntica byte a byte
  (`yaml-industria5-workers.golden` == `yaml-industria5.golden`).

**Objeto aninhado, não chaves com ponto.** `"triage": {...}` é JSON normal
para quem consome, e `event.Get` já resolve `triage.decision` descendo no
objeto. Por isso `filter`, `dedup` e `window` usam os subcampos sem mudar nada.
Na saída de `window`/`aggregate`, a chave de agrupamento mantém o nome do `by`
(`"triage.rule": ...`), como acontece com qualquer outro caminho.

Um campo `triage` que já exista na entrada é substituído.

**I4 com cópia rasa.** `Process` copia só o mapa de topo (`maps.Clone`) e
escreve uma chave nova. Os valores aninhados ficam compartilhados, mas ninguém
os modifica. A anotação é um mapa novo por evento: compartilhar um mapa entre
eventos seria estado mutável escondido. O custo medido é de 583 ns e 8
alocações por evento (`BenchmarkTriage`).

### 2. Linguagem de expressão única: `stage.Cond`

O avaliador de `where` saiu de `filter` e foi para o pacote pai
`internal/stage` (`ParseCond`, `ParseConds`, `All`). Stages não importam uns
aos outros (ADR 0001). `filter` e `triage` aceitam exatamente a mesma
linguagem e devolvem os mesmos erros. A refatoração foi validada pela suíte
existente do filter, inclusive `TestFilter_ZeroAllocs`.

### 3. Métrica `ratio:num:den` em `aggregate` e `window`

Não há stage de sustentabilidade separado: `ratio` é mais uma métrica de
`stage.ParseMetrics`, e por isso vale nos dois stages.

- Valor = **soma de `num` ÷ soma de `den`** no grupo (razão das somas), no
  campo `ratio_<num>_<den>`. Energia total ÷ peças boas é o indicador que o
  chão de fábrica usa. A média das razões daria o mesmo peso a um lote de 1
  peça e a um de 1000.
- **Só entram eventos que têm os dois campos numéricos.** Um evento com
  energia e sem contagem de peças não puxa a razão para nenhum lado.
- **Denominador zero** (ou nenhum par no grupo): o valor sai `null` e o grupo
  ganha `"<métrica>_status": "undefined_denominator"`. Nunca sai `NaN` nem
  `Inf`, que são JSON inválido. O status é por métrica, não um único
  `metric_status`, porque um grupo pode ter vários `ratio`. Emitir `0` foi
  rejeitado: ficaria indistinguível de um `0` real (numerador zero, que é
  definido e sai `0`).
- Memória: um `float64` a mais por métrica por grupo. O teto de I6 (`max_keys`,
  `max_open`) não muda.

### 4. Config estrita também dentro de listas

Até aqui, `decodeStrict` só recusava chave desconhecida no primeiro nível do
stage. Com `rules`, um erro de digitação dentro de uma regra (`prioridade: 1`,
`reasn: ...`) passaria em silêncio. Agora a checagem desce em campos struct e
em listas de struct e aponta linha e coluna:

```
28-triage-campo-regra-desconhecido.yaml:9:9: campo desconhecido "prioridade" (campos: name, where, decision, reason, requires_ack)
```

`hermes-pipe schema` também descreve os campos de cada item de `rules`.

### 5. Composição recomendada

```
triage → filter (decision != auto_ok) → dedup (host + triage.rule, 30s) → window (1m)
```

Veja `examples/industria5-supervisao.yaml`. O `dedup` antes da janela
transforma uma rajada do mesmo incidente numa ocorrência, e a janela entrega
um resumo por célula, regra e decisão.

## Alternativas rejeitadas

| Alternativa | Por que não |
|---|---|
| `triage` que descarta ou desvia eventos | mistura classificação com roteamento; o `filter` já faz isso e fica explícito no YAML |
| `triage` que espera aprovação humana | bloquearia o pipeline; viola I8/I9 e o determinismo |
| Stage separado de sustentabilidade | duplicaria o agrupamento, os tetos e o despejo que `aggregate`/`window` já têm |
| `ratio` como média das razões | dá o mesmo peso a lotes de tamanhos diferentes |
| Denominador zero → `0` | ambíguo com um zero real |
| Chaves planas `"triage.decision"` | saída menos natural em JSON; `Get` já resolve o aninhado |

## Consequências

- **Positivas:** nenhuma mudança no runner nem nas invariantes; `RunSequential`
  continua sendo o oráculo; contrato de Stage, golden e fuzz cobrem o código
  novo; `hermes-pipe-lint` passa.
- **Negativas:** a aprovação humana não volta para o mesmo pipeline. Um ack
  precisa reentrar como evento novo e imutável (por exemplo, com `ack_of_id`
  e chave de idempotência), o que fica para um ADR futuro. A cópia rasa por
  evento custa alocações que o `filter` não tem.
- **Limitação conhecida:** se a soma estourar para `Inf` (valores perto de
  `1e308`), o valor também sai `null` com `undefined_denominator`. O JSON
  continua válido, mas o rótulo fica impreciso nesse caso extremo.

## Como foi provado

| Propriedade | Teste |
|---|---|
| Contrato de Stage (I4, válido, determinístico) | `triage.TestStageContract`, `config.TestStage_NoSideEffects/triage` |
| Primeira regra vence; padrão; campo pré-existente | `TestTriage_Decisions`, `TestTriage_DefaultDecision` |
| Subcampos visíveis para stages seguintes | `TestTriage_DottedPathWorksDownstream` |
| Validação da config | `triage.TestNew_Errors`, `testdata/config/27-*`, `28-*`, `29-ok-triage` |
| `ratio`: soma, pares, denominador zero, sem NaN/Inf | `stage.TestRatio`, `TestParseMetrics_Ratio` |
| I5 com 1 e N workers e buffers diferentes | `testdata/pipelines/triagem.yaml`, `yaml-industria5(-workers).golden` |
| I1 | `Conserved()` em todo pipeline de `TestPipeline_Determinism` |
