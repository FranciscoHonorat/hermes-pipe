# ADR 0016: Reconciliação idempotente de decisões humanas

- **Status:** aceito
- **Data:** 2026-11-25

## Contexto

O ADR 0015 marca com `triage.requires_ack: true` o que exige julgamento
humano. Falta a volta: a decisão do operador (aprovar, ajustar, rejeitar)
precisa reentrar no fluxo e fechar a exceção. Na prática, isso traz três
riscos:

1. **efeito duplicado:** reenvio da rede ou clique repetido no painel aplica a
   mesma decisão duas vezes;
2. **estado infinito:** pendências esperando respostas que nunca chegam;
3. **quebrar I1–I9:** relógio de parede, I/O, memória sem teto ou saída que
   muda com o número de workers.

## Decisão

### 1. Contrato de eventos

O campo `kind` separa três tipos de evento (o nome do campo é fixo; ausente
conta como telemetria):

| Evento | Como é reconhecido | Campos obrigatórios |
|---|---|---|
| **exceção** | `kind: "exception"` **ou** `triage.requires_ack: true` | a chave `correlate_by` (não nula) |
| **decisão humana** | `kind: "human_decision"` | `decision_id`, `target_key`, `operator_id`, `action`, `reason` |
| qualquer outro | — | — |

`decision_id`, `operator_id`, `action` e `reason` precisam ser texto não
vazio. `reason` é obrigatório porque a decisão é auditável. `target_key` é
comparado com a chave da exceção pela mesma regra de `stage.KeyString` do
agrupamento, então `42` e `"42"` casam. `action` é texto livre: o pipeline
registra a ação, não a interpreta.

Um evento que viola o contrato é **erro de evento** (`ErrNoKey`,
`ErrInvalidDecision`): vai para o stderr com o motivo e conta como `errored`
(I1).

### 2. Stage `reconcile`, com estado e com teto

Configuração: `correlate_by` (obrigatório), `window` (prazo, > 0),
`max_keys` (pendências; padrão 10 000), `max_decisions` (`decision_id`
lembrados; padrão 100 000), `on_timeout` (`escalate` | `quarantine` | `drop`;
padrão `escalate`) e `on_duplicate` (`drop` | `mark`; padrão `drop`).

**A exceção fica retida até o desfecho e sai exatamente uma vez**, com
`"reconcile": {"status": ...}`:

| Desfecho | `status` | Também leva |
|---|---|---|
| decisão chegou no prazo | `resolved` | `decision_id`, `operator_id`, `action`, `reason`, `decided_at` |
| prazo venceu | `escalated_timeout` ou `quarantined_timeout` (com `drop`, não sai) | `deadline` |
| teto `max_keys` | `evicted` (e `"evicted": true` no topo, como nos outros stages) | `deadline` |
| fim do stream (`Flush`) | `unresolved` | `deadline` |
| já havia pendência com a mesma chave | `duplicate_pending`, sai na hora; a primeira continua valendo | — |

**Decisões:**

- A decisão aplicada é **absorvida**: o conteúdo dela vai para o
  `reconcile` da exceção, que é o registro de auditoria. Para os contadores,
  a decisão é `dropped` e a exceção que sai depois é `generated`, então I1
  fecha sem tratamento especial no runner.
- Uma decisão sem pendência (incidente desconhecido, já resolvido ou vencido)
  sai como `orphan_decision`. **Nada some em silêncio:** um segundo clique com
  outro `decision_id` fica visível no stdout, mas não muda nada.
- Um `decision_id` repetido não tem efeito. Com `drop` ele é descartado e
  contado em `Duplicates()`; com `mark` sai com `duplicate_ignored`.

**Por que reter a exceção em vez de repassá-la já marcada como pendente?**
Assim cada exceção vira **um** registro com o desfecho, e não um par
"aberta/fechada" para o consumidor juntar. O painel que mostra o que está
aberto fica **antes** do `reconcile`, na saída do `triage`. O custo é que a
exceção sai atrasada, no instante da decisão. Um `window` depois do
`reconcile` pode tratá-la como atrasada. Isso está documentado, e a ordem
recomendada é `triage → reconcile`, com a agregação em outro pipeline ou
antes.

### 3. Tempo, prazo e ordem

- O prazo de uma pendência é `time` da exceção + `window`. O "agora" do stage
  é o **maior tempo de evento visto**, e ele nunca recua. Nada usa relógio de
  parede (`hermes-pipe-lint` passa).
- O prazo é **inclusivo**: uma decisão com tempo igual ao prazo resolve. A
  pendência vence quando o maior tempo visto passa do prazo.
- Em cada `Process`, o stage primeiro fecha o que venceu e depois trata o
  evento. Uma decisão atrasada encontra a exceção já escalada e sai órfã: ela
  não ressuscita a pendência.
- Uma exceção que já chega vencida fecha na hora e **não despeja** uma
  pendência viva para abrir espaço (`TestReconcile_LateExceptionDoesNotEvict`).
- Pendências moram num **heap mínimo indexado** por (prazo, ordem de chegada),
  com um map por chave. Vencimento, despejo e `Flush` saem em ordem de prazo,
  com desempate determinístico pela chegada. Resolver uma pendência a remove
  do heap em O(log n), sem deixar entrada morta, então a memória fica de fato
  em `max_keys`. Não há `range` sobre map.
- O despejo pelo teto tira a pendência de **prazo mais próximo**: é a que
  venceria primeiro.
- O stage não é `Concurrent`: roda numa goroutine só, como `window` e
  `dedup`. A saída com `--workers 8` é idêntica à de 1 worker
  (`yaml-reconciliacao-workers.golden`) e à do `RunSequential`
  (`testdata/pipelines/reconciliar.yaml`).
- Não há `Idle`: silêncio da fonte não é motivo para escalar uma decisão
  humana.

### 4. Memória dos `decision_id`

`decision_id` fica num `ds.Bounded` (LRU) com teto `max_decisions`. Quando um
id é esquecido pelo teto e chega de novo, ele não encontra mais a pendência,
porque ela já foi resolvida e removida, e sai como órfão. **O efeito nunca é
aplicado duas vezes.** O único caso de risco é a mesma chave ser reaberta por
uma exceção nova *e* um reenvio antigo chegar depois que o id foi esquecido.
Dimensione `max_decisions` para cobrir a janela de reenvio do produtor.

## Alternativas rejeitadas

| Alternativa | Por que não |
|---|---|
| Repassar a exceção na hora e emitir um evento de resolução separado | dois registros por incidente para o consumidor juntar; a visão "aberto agora" já existe na saída do `triage` |
| Fila por inserção para vencimento (como o `dedup`) | com exceções fora de ordem, uma pendência vencida ficaria presa atrás de uma viva; o resultado mudaria com a ordem de chegada |
| Heap com remoção preguiçosa | entradas mortas acumulam além de `max_keys` quando a resolução é rápida |
| Descartar decisões órfãs | tira do operador a prova de que o clique chegou tarde ou em dobro |
| Lista fixa de `action` | o pipeline não interpreta a ação; validar o vocabulário é papel do produtor |
| `kind` configurável | nada pediu isso ainda; um campo fixo deixa o contrato mais simples de documentar |

## Consequências

- **Positivas:** fecha o ciclo human-in-the-loop com idempotência, auditoria
  (quem, o quê, por quê e quando) e teto de memória, sem estado fora do
  pipeline e sem mudar o runner.
- **Negativas:** o produtor das decisões precisa gerar um `decision_id`
  estável por ação humana e usar timestamps coerentes com o stream. Uma
  decisão com tempo muito à frente avança o "agora" e vence pendências antes
  da hora. A exceção sai no instante da decisão, não no seu.
- **Custo:** 880 ns e 12 alocações por ciclo exceção + decisão
  (`BenchmarkReconcile`).

## Como foi provado

| Propriedade | Teste |
|---|---|
| Contrato de Stage (I4, válido, determinístico) | `reconcile.TestStageContract`, `config.TestStage_NoSideEffects/reconcile` |
| Idempotência e órfãs | `TestReconcile_Idempotent`, `TestReconcile_DuplicateMark`, `TestReconcile_DecisionMemoryBounded` |
| Prazo inclusivo, decisão atrasada, políticas de timeout | `TestReconcile_DeadlineInclusive`, `TestReconcile_LateDecision`, `TestReconcile_Timeout` |
| Ordem de vencimento com entrada fora de ordem | `TestReconcile_ExpiryOrderIsDeadlineThenArrival` |
| Teto (I6) | `TestReconcile_Bounded`, `TestReconcile_LateExceptionDoesNotEvict`, `reconciliar.golden` (10 despejos) |
| Erros de evento | `TestReconcile_EventErrors` |
| I1, I5, I7 com 1 e N workers | `TestPipeline_Determinism/reconciliar.yaml`, `yaml-reconciliacao(-workers).golden` |
| Mutantes manuais (prazo exclusivo, não lembrar `decision_id`) | pegos por `DeadlineInclusive` e `Idempotent` |
