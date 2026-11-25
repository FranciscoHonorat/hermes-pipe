# ADR 0003: O tipo Event, o parser e a saída de erros

- **Status:** aceito
- **Data:** 2026-11-25

## Contexto

A semana 1 transforma o hermes-pipe num validador de JSON Lines:
`hermes-pipe < in.jsonl > ok.jsonl 2> bad.jsonl`. Tudo o que vem depois
(filter, map, aggregate, window, dedup) trabalha sobre o tipo `Event`, então as
escolhas daqui aparecem no projeto inteiro:

1. Como guardar os campos: `map[string]any` ou `map[string]json.RawMessage`?
2. O instante do evento como `time.Time` ou `int64`?
3. `Event` passa por valor ou por ponteiro?
4. Linha grande demais: erro do evento ou erro fatal?
5. Qual é o formato da saída de erros?

Os números abaixo vêm de `internal/event/bench_test.go`
(`go test -bench . -benchmem ./internal/event`, i5-1135G7, Go 1.27.2) e valem
para a linha de exemplo do benchmark, de 6 campos com um objeto aninhado.

| Benchmark | ns/op | B/op | allocs/op |
|---|---|---|---|
| `ParseEvent` (escolhido: Decoder + UseNumber + validação + time) | ~6000 | 2033 | 39 |
| `ParseAlt_UnmarshalAny` (`json.Unmarshal` em `map[string]any`) | ~4100 | 1000 | 28 |
| `ParseAlt_UnmarshalRaw` (`map[string]json.RawMessage`) | ~2600 | 576 | 13 |
| `ParseAlt_DecoderToken` (só tokeniza) | ~2200 | 1144 | 41 |
| `TimeAlt_TimeTime` (`time.Parse` RFC 3339) | ~70 | 0 | 0 |
| `AppendJSON` (serializar de volta) | ~3000 | 416 | 18 |

## Decisão

### Campos: `map[string]any` com `json.Number`

```go
type Event struct {
    Seq    uint64         // linha de origem; 0 para eventos gerados
    Time   time.Time      // nunca zero
    Fields map[string]any // valores de encoding/json com UseNumber
}
```

- O parse usa `json.Decoder` com `UseNumber()`. Os números viram
  `json.Number`, que é a string original. Assim um id como
  `12345678901234567890` não perde precisão passando por `float64`, e a saída
  reproduz o número como entrou.
- `map[string]json.RawMessage` aloca menos da metade, mas só adia o trabalho:
  todo `filter`, `map` e `aggregate` precisaria decodificar o valor de novo, e
  campos aninhados (`http.status`) exigiriam um segundo parse por acesso. Com
  `any`, o custo é pago uma vez por linha.
- O parse escolhido custa cerca de 1,5× o `Unmarshal` puro. A diferença vem do
  que o `Unmarshal` não faz: `utf8.Valid`, a verificação de que não há lixo
  depois do objeto (`dec.Token()` precisa devolver `io.EOF`) e o
  `time.Parse`. Esse custo é a validação, que é a razão de existir da semana 1.
- `encoding/json/v2` existe no 1.27, mas a semântica de números e de chaves
  duplicadas é diferente. Fica para uma comparação futura; o `bench_test.go` é
  onde ela entraria.

### Tempo: `time.Time`, obrigatório

- `Time` é `time.Time`, lido do campo `"time"` em RFC 3339 (com ou sem
  nanossegundos e fuso). O `time.Parse` custa ~70 ns e não aloca, ou seja,
  ~1% do parse. Guardar `int64` economizaria quase nada e perderia o fuso e
  a API de `time` que window e dedup usam.
- `time.Parse` devolve um `Time` **sem leitura monotônica**: só relógio de
  parede. É o certo aqui, porque o tempo do evento não é medição de duração
  local. Comparações usam `Before`/`After`/`Equal`, nunca `==`, que compara
  também o `*Location`.
- **Invariante:** todo `Event` tem `Time` não zero e `Fields["time"]`
  presente. Sem timestamp é erro (`ErrMissingTime`), nunca zero.
  `{"time":null}` conta como ausente; `{"time":1700000000}` é
  `ErrInvalidTime` (epoch não é aceito: é ambíguo entre s, ms e ns).
- Eventos gerados (aggregate, window) são criados por `event.New`, que escreve
  `Fields["time"]` a partir de `Time`, então a invariante vale também para eles.

### `Event` por valor

`Event` tem 40 bytes (`uint64` + `time.Time` + ponteiro do map). Passar por
valor não aloca e evita que um stage mude o `Seq` ou o `Time` de quem chamou.
O **map**, porém, é compartilhado na cópia. Por isso o contrato do Stage
(ADR 0004) proíbe modificar o evento recebido, e `Clone()` faz cópia profunda.

### Linha grande demais: erro do evento

- `bufio.Scanner` trata linha acima do buffer (64 KiB por padrão) como erro
  **fatal** do stream: o Scanner para. Um log com uma linha gigante mataria
  o processamento.
- O `event.LineReader` usa `bufio.Reader.ReadSlice` e trata
  `bufio.ErrBufferFull`: acima de `max_line` (padrão 1 MiB) ele descarta o
  resto da linha, devolve `ErrLineTooLong` **para aquela linha** e segue.
- `\r` final (`\r\n`) é tirado pelo `Parse`, não pelo leitor: o leitor
  devolve bytes exatos, e quem interpreta é o parser.

### Saída de erros: JSON Lines no stderr

Cada linha rejeitada vira uma linha JSON no stderr:

```json
{"line":13,"stage":"parse","error":"JSON inválido: ...","raw":"isto não é json"}
{"line":40,"stage":"window(1m0s by host)","error":"evento atrasado: ...","event":{...}}
```

- `line` é o número da linha de entrada, `stage` diz quem rejeitou. Erro de
  parse leva `raw` (a linha original como string; UTF-8 inválido vira U+FFFD).
  Erro de stage leva `event`, o evento já parseado.
- No fim, o stderr recebe uma linha `{"summary":{...}}` com os contadores de
  I1. Logs operacionais (`slog`) também vão para o stderr em JSON, só a partir
  de WARN (ADR 0007). Tudo no stderr é JSON Lines, e cada tipo de linha tem
  uma chave que o distingue (`line`, `summary`, `msg`).
- A saída usa `json.Marshal` sobre o map, que **ordena as chaves**: a saída é
  determinística mesmo com a iteração aleatória de maps (ADR 0013).

## Alternativas consideradas

- **`bufio.Scanner` com `Buffer` grande.** Rejeitada: continua fatal acima do
  limite, e fixar o buffer em 1 MiB custa essa memória sempre.
- **Struct fixo para os campos conhecidos.** Rejeitada: o hermes-pipe não
  conhece o schema dos logs de ninguém.
- **Erros no stdout com uma flag `"ok":false`.** Rejeitada: o stdout tem de
  ser consumível direto por outra ferramenta, sem filtrar.
- **Resumo em texto livre.** Rejeitada: quebraria o "tudo no stderr é JSON".

## Consequências

- `TestParseEvent` cobre 21 casos (vazia, `{}`, `{"time":null}`, UTF-8 inválido,
  1 MB, `\r\n`, lixo depois do objeto...). `FuzzParseEvent` verifica que um
  evento aceito sempre reparseia para a mesma saída, e `FuzzLineReader` que a
  contagem de linhas nunca se perde.
- O parse é o custo dominante do pipeline (~6 µs por linha) e roda numa
  goroutine só. Isso limita o ganho de `--workers` (ADR 0012).
- Onde o `ParseEvent` aloca: `go build -gcflags=-m ./internal/event` mostra
  `&bytes.Reader{...} escapes to heap` (o `Decoder` guarda o reader) e
  `moved to heap: v` (o `any` decodificado sobrevive à função dentro do
  Event). O resto das 39 alocações vem de dentro do `encoding/json`: o buffer
  do `Decoder`, o map, e uma string ou `json.Number` por valor. Contado com
  `-benchmem`.
