# ADR 0007: Config em YAML e CLI

- **Status:** aceito
- **Data:** 2026-11-25

## Contexto

A semana 5 adiciona `hermes-pipe run -c pipeline.yaml`: pipelines versionáveis
e reutilizáveis. Uma config errada precisa falhar **antes do primeiro evento**,
citando linha e coluna. Os stages continuam sem saber que YAML existe (I4 e as
camadas do ADR 0001: `stage` não importa `config`).

## Decisão

### Schema

```yaml
version: 1            # obrigatório; única versão aceita
input: logs.jsonl     # opcional; vazio ou "-" = stdin
buffer: 64            # opcional
workers: 1            # opcional
idle_flush: 0s        # opcional; 0 = desligado
max_line: 1048576     # opcional
stages:               # lista linear, em ordem
  - type: filter
    where: ["level == error"]
  - type: aggregate
    by: [host]
    metrics: [count, "p95:latency_ms"]
    top: 10
```

`hermes-pipe schema` imprime o schema completo, gerado por reflexão a partir
dos structs `Config` de cada stage. `TestCLI_Stable` trava essa saída num
golden: mudar um campo aparece no diff do PR (ADR 0014).

### Lista linear, não grafo

O runner é uma cadeia: cada stage tem uma entrada e uma saída. Um grafo
(fan-out para dois destinos, join) exigiria outra semântica de ordem, de
backpressure e de I1, para um uso que o projeto não tem. Quem quiser dois
destinos roda dois pipelines.

### Registro de construtores

```go
var registry = map[string]registration{
    "filter": register(filter.New),
    "map":    register(mapper.New),
    ...
}
```

- Cada stage expõe `type Config struct{...}` em Go puro (com tags `yaml`, que
  são só strings) e `New(Config) (*T, error)`, que valida. O stage valida a
  própria config sem conhecer YAML.
- `register[C, S]` é genérico: decodifica o nó YAML em `C`, chama `New` uma
  vez para validar (com a posição do nó no erro) e guarda uma função que cria
  instâncias novas. `Pipeline.Stages()` devolve stages novos a cada chamada,
  porque stages com estado não podem ser reusados.
- O registro mora em `config`, que importa os stages. A seta nunca volta:
  adicionar um stage é escrever o pacote e uma linha no registro.

### Decodificação estrita com posição

- `yaml.Decoder.KnownFields(true)` só existe no `Decoder`; `yaml.Node.Decode`
  não tem. Como o documento é lido como `yaml.Node` (para ter linha e coluna
  de cada stage), `decodeStrict` compara as chaves do nó com as tags `yaml`
  do struct, por reflexão, antes de decodificar. Chave desconhecida ou
  repetida é erro com a posição da chave.
- Erros saem como `arquivo:linha:coluna: mensagem`:
  `pipeline.yaml:4:5: campo desconhecido "wher" (campos: where)`.

### As flags continuam como atalho

`--filter`, `--map` e `--limit` montam o mesmo `config.Pipeline` que o YAML
(`config.FromFlags`), na ordem filter → map → limit. Não há dois caminhos de
execução. Para aggregate, window e dedup só existe o YAML: uma flag por campo
seria uma segunda linguagem de config.

Com `run -c`, as flags `--buffer`, `--workers`, `--max-line` e
`--idle-flush` **ganham** do YAML se forem passadas explicitamente
(`flag.Visit` diz quais foram).

### Onde o `slog` escreve

stdout é dado e stderr é erro de evento. Os logs operacionais vão para o
**stderr**, em JSON (`slog.NewJSONHandler`), a partir de **WARN**; com `-v`, a
partir de INFO. Por padrão, o stderr só tem erros de evento e o resumo; uma
linha de log se distingue pela chave `msg`.

## Alternativas consideradas

- **`yaml.Decoder` com `KnownFields` e struct com `[]StageConfig` união.**
  Rejeitada: perde a posição de cada stage e obriga um struct com os campos
  de todos os tipos.
- **Stage lendo o próprio `yaml.Node`.** Rejeitada: `stage` passaria a
  depender da lib YAML (ADR 0002).
- **Logs num arquivo separado.** Adiada: sem necessidade por enquanto.

## Consequências

- `TestLoad_Golden`: 26 configs em `internal/config/testdata/config/` com a
  mensagem esperada (ou `ok:`) em golden. Cobre YAML quebrado, stage
  desconhecido, campo desconhecido, campo repetido, tipo errado, duração
  inválida e a validação de cada stage.
- `FuzzConfig`: qualquer entrada ou é recusada com o nome do arquivo na
  mensagem, ou constrói stages sem erro.
- Mutante verificado: aceitar campo desconhecido é pego pelo golden.
- Todo exemplo do README em YAML (`examples/*.yaml`) roda no
  `TestCLI_EndToEnd` e no `TestPipeline_Determinism`.
