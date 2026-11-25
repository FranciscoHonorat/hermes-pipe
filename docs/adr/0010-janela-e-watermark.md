# ADR 0010: Janela de tempo e watermark

- **Status:** aceito
- **Data:** 2026-11-25

## Contexto

A semana 9 responde "quantos erros por host por minuto", com o mesmo resultado
toda vez que o arquivo é reprocessado, mesmo com eventos fora de ordem. O
stage `window` agrega por chave dentro de janelas fixas do **tempo do evento**
e reusa as métricas e o `Group` do `aggregate` (`internal/stage/agg.go`).

## Decisão

### Janelas `[início, fim)`

Fim aberto: um evento às 12:01:00 pertence à janela das 12:01, não à das
12:00. Com intervalos fechados nos dois lados, o evento exatamente na borda
cairia em duas janelas. O índice da janela é a divisão com piso de
`UnixNano()` pelo tamanho, inclusive antes de 1970 (testado).

### Watermark

```
watermark = maior tempo de evento visto − lateness
```

- O watermark só muda quando chega um novo máximo. Como o máximo só cresce, o
  watermark **nunca recua**. A primeira versão tinha uma guarda extra
  (`if wm.After(w.watermark)`); o teste de mutação mostrou que ela era
  redundante: remover a guarda é um mutante **equivalente**, que nenhum teste
  pode matar. A guarda saiu, e o mutante agora ataca a condição que importa
  (atualizar o máximo com qualquer evento), que o `FuzzWindow` pega.
- Uma janela fecha (é emitida) quando `fim <= watermark`. A partir daí, nada
  mais pode entrar nela.
- O watermark é o que torna a janela **determinística**: a decisão de fechar
  depende só dos tempos dos eventos já lidos, nunca do relógio de parede.
  Reprocessar o arquivo repete exatamente as mesmas decisões.

### Evento atrasado: erro do evento

Um evento cuja janela já fechou devolve `window.ErrLate`, que é erro de
evento: vai para o stderr com o evento e conta em `errored`. Assim I1 fecha
sem contador novo, e o usuário **vê** o que perdeu. Descartar em silêncio
esconderia dados; um contador "atrasados" à parte quebraria a fórmula de I1.

### Ring buffer de janelas abertas

As janelas abertas são contíguas: do índice que contém o watermark até o
maior visto. Elas moram num `ds.Ring[*win]` (ADR 0011): a da frente é a mais
antiga, e fechar é `PopFront`.

- `max_open` (padrão 64) limita as janelas abertas. Um evento muito à frente
  força o fechamento das mais antigas (emitidas na hora). Um evento para uma
  janela fechada assim também é `ErrLate`.
- `max_keys` (padrão 10.000) limita as chaves **por janela**, com o mesmo
  despejo emitido do `aggregate`. A memória total é `max_open × max_keys`
  grupos no pior caso (I6).

### Idle-flush: o relógio mora no runner

Num stream ao vivo, a última janela só fecharia com o próximo evento. O
runner pode chamar `Idle(emit)` nos stages `Idler` quando a entrada deles fica
parada por `--idle-flush`. O `window` fecha todas as janelas abertas no
`Idle`.

- O relógio é um `pipeline.Clock` injetado (`WithClock`), e o padrão é
  `time.After`. O **stage** continua sem relógio, e o analisador garante isso
  (ADR 0008).
- O padrão é **desligado** (`idle_flush: 0`). Em replay de arquivo, o
  idle-flush faria a saída depender da velocidade da leitura. Ligado, ele
  serve para stdin ao vivo.

### Timers

Desde o Go 1.23, um timer criado por `time.After` e não referenciado é
coletado mesmo sem disparar, e os channels de timer não têm buffer. O laço do
runner cria um `time.After` por iteração, só quando o idle está ligado, sem
vazar timers.

## Alternativas consideradas

- **Janela fechando por relógio de parede.** Rejeitada: replay deixaria de
  ser determinístico.
- **Evento atrasado entra numa janela "atrasada" emitida de novo.** Rejeitada
  por ora: exige retratação (o consumidor teria de substituir um resultado já
  emitido).
- **Map de janelas abertas em vez de ring.** Rejeitada: o ring dá a ordem de
  fechamento de graça e não aloca depois de criado.

## Consequências

- `TestWindow_Edges` tem 20 casos de borda (fim aberto, tolerância exata,
  lacuna grande, teto de janelas, antes de 1970...).
- `TestWindow_Determinism` (seed impressa) e `TestWindow_ReorderWithinTolerance`
  (propriedade: trocar eventos com distância menor que a tolerância não muda a
  saída).
- `TestRunner_IdleFlush` simula **uma hora** de silêncio dentro do `synctest`
  sem esperar uma hora: o relógio da bolha só anda quando todas as goroutines
  estão bloqueadas. `TestClockContract` verifica que `After(90s)` dispara em
  exatos 90 s de tempo da bolha.
- `FuzzWindow` verifica que o watermark nunca recua e que todo evento aceito é
  contado exatamente uma vez.
