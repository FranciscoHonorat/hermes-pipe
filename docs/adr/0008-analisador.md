# ADR 0008: O analisador hermes-pipe-lint

- **Status:** aceito
- **Data:** 2026-11-25

## Contexto

O `architecture_test.go` garante as camadas com `go list -deps`, mas não vê
**o que** um stage faz com o que importa. `internal/stage/window` pode
importar `time` (precisa de `time.Duration`), e nada impede que chame
`time.Now()`, o que quebraria o determinismo (I5). A semana 6 escreve um
analisador estático próprio, só com stdlib, para essas regras.

## Decisão

### Três regras para `internal/stage/...`

| Regra | Proíbe | Invariante |
|---|---|---|
| `clock` | `time.Now`, `Since`, `Until`, `After`, `AfterFunc`, `Tick`, `NewTimer`, `NewTicker`, `Sleep`; qualquer uso de `math/rand` ou `math/rand/v2` | I4, I5 |
| `io` | declarar ou receber `chan`, `context.Context`, `*os.File`, `io.Writer` (parâmetro, resultado, campo, variável); `make(chan ...)` | I4 |
| `order` | `range` sobre map sem ordenação (`sort.*`, `slices.Sort*`) depois, na mesma função, a não ser com `//hermespipe:unordered-ok <motivo>` | I5 |

### "É um stage" por pacote

O analisador roda sobre os pacotes que recebe (`hermes-pipe-lint
./internal/stage/...`, padrão no CI e no `make lint-arch`). Decidir por tipo
("tudo que implementa `Stage`") pegaria menos coisa: um helper fora do método
`Process` poderia chamar `time.Now`. Por pacote, tudo dentro de
`internal/stage` segue as regras, inclusive `stagetest` e o pacote pai.

### Tipos, não só sintaxe

O carregamento usa `go list -export -deps` para obter os dados de export das
dependências, `go/parser` para o código dos pacotes alvo e `go/types` com
`importer.ForCompiler(fset, "gc", lookup)` para o type check.

Cada regra olha o **objeto** que um identificador usa (`types.Info.Uses`), não
o nome:

- `import t "time"` seguido de `t.Now()`: o objeto é `time.Now` do pacote
  `"time"`, então é pego (`TestAnalyzer_Aliases`).
- Uma variável local chamada `time` com um campo `Now` não é o pacote, então
  não é pega (`testdata/src/clockgood`).
- `a.After(b)` com `a time.Time` é um **método**, também `*types.Func`, mas
  com receptor. A primeira versão acusava esse caso como violação; ele
  apareceu ao rodar o analisador sobre o código real (`window.go` usa
  `t.After`). A regra agora exige `Signature().Recv() == nil`, e o mutante 10
  do `scripts/mutate.sh` prova que o teste pega a volta desse falso positivo.

Removendo o type check, só sobra comparar o nome do pacote no seletor:
`TestAnalyzer_Aliases` cai (o alias `t` não se chama `time`) e `clockgood`
passa a dar falso positivo (a variável `time`).

### Falso positivo ou falso negativo, por regra

- `clock`: quase sem ambiguidade com tipos. Prefere falso positivo: qualquer
  uso de `math/rand` é acusado, mesmo com seed fixa (um stage que precisa de
  aleatoriedade determinística usa um PRNG próprio, como o reservatório do
  `aggregate`).
- `io`: prefere falso positivo. Um campo `[]io.Writer` também é pego.
- `order`: **heurística, prefere falso negativo.** Ela garante que *alguma*
  ordenação acontece depois do `range` na mesma função, não que as chaves
  certas foram ordenadas. Um `range` que só soma valores precisa da diretiva.

### Escape hatch com motivo obrigatório

`//hermespipe:unordered-ok <motivo>` na linha do `range` ou na anterior. Sem
motivo, o analisador acusa a própria diretiva. Outro comentário (`// want`)
não conta como motivo.

## Alternativas consideradas

- **`golang.org/x/tools/go/analysis`.** Rejeitada pelo ADR 0002 (só stdlib).
  O desenho segue a mesma ideia (um passe por pacote, diagnósticos com
  posição), e o harness de teste imita o `analysistest` com `// want "regex"`.
- **Análise só sintática (`go/ast`).** Rejeitada pelos casos de alias e de
  sombreamento acima.
- **Regras no `golangci-lint` com `forbidigo`.** Rejeitada: `forbidigo`
  trabalha com padrões de texto e não resolve aliases.

## Consequências

- `TestAnalyzer` roda cada pacote de `internal/lint/testdata/src/` (um ruim e
  um bom por regra) e exige que cada diagnóstico tenha um `// want` na mesma
  linha, e vice-versa.
- `TestArchitecture_Rules` roda o analisador sobre `internal/stage/...` real e
  falha com qualquer violação. O job `arch` do CI faz o mesmo pelo binário.
- O analisador depende do `go` no `PATH` (para o `go list`). Isso é aceitável
  numa ferramenta de desenvolvimento.
