# ADR 0001: Nome, módulo, versão do Go e layout de pacotes

- **Status:** aceito
- **Data:** 2026-10-12

## Contexto

O hermes-pipe é um processador de stream local, distribuído como **um binário**
(`cmd/hermes-pipe`). Ele lê JSON Lines do stdin, passa cada evento por uma sequência
de stages e escreve no stdout. Não é uma biblioteca: nenhum outro projeto deve
importar os pacotes dele.

O projeto também serve para estudar o runtime do Go 1.27. Por isso a versão do Go
precisa ser fixa e a mesma no CI e na máquina local.

Antes de decidir o layout, três coisas precisam estar definidas:

1. O caminho do módulo, que vai aparecer em todo import.
2. Onde ficam os pacotes (`internal/`, `pkg/` ou a raiz).
3. Como impedir que as dependências entre pacotes se desorganizem com o tempo.

## Decisão

### Nome e módulo

- Nome: **hermes-pipe**. Módulo: **`github.com/FranciscoHonorat/hermes-pipe`**.
  O caminho é igual à URL do repositório, então `go install` e `go get` funcionam
  sem configuração extra.
- O projeto se chamava `cascade` antes e foi renomeado antes do primeiro release.
  Como ainda não havia nenhum consumidor, a troca não quebrou nada.
  <!-- TODO(Francisco): escreva em uma frase por que trocou o nome. -->

### Versão do Go

- `go.mod` declara `go 1.27.2`. Essa linha é o **requisito mínimo**: uma toolchain
  mais antiga não compila o módulo. Com `GOTOOLCHAIN=auto`, ela baixa a 1.27.2 em
  vez de compilar.
- Não há linha `toolchain`. Quando ela não existe, a toolchain sugerida é a própria
  versão da linha `go`.
- O CI usa `GOTOOLCHAIN=local`: usa só a toolchain que o `setup-go` instalou a
  partir do `go.mod` e nunca baixa outra por conta própria. Se as versões não
  baterem, o CI falha em vez de trocar a toolchain sem avisar. O job de teste
  imprime `go version` para registrar qual foi usada.

### `internal/` e não `pkg/`

Todo o código fica em `internal/`. O compilador só permite importar um pacote em
`.../internal/...` a partir de pacotes que estejam na árvore do diretório pai de
`internal`. Na prática, só o próprio módulo consegue importar esse código.

Isso significa que nenhuma API é pública, e por isso tipos e assinaturas podem
mudar a qualquer semana sem precisar de versionamento semântico. Usar `pkg/` seria
prometer uma API estável que o projeto não tem nem quer ter.

### Camadas

```
cmd/hermes-pipe  →  internal/config  →  internal/pipeline, internal/stage/*  →  internal/event
```

| Camada      | Pacotes                                    | Pode importar                     |
|-------------|--------------------------------------------|-----------------------------------|
| `cmd`       | `cmd/hermes-pipe`                          | config, pipeline, stage, event    |
| `config`    | `internal/config`                          | pipeline, stage, event            |
| `pipeline`  | `internal/pipeline`                        | event                             |
| `stage`     | `internal/stage`, `internal/stage/*`       | event, ds, `internal/stage` (pai) |
| `ds`        | `internal/ds`                              | nada do módulo                    |
| `event`     | `internal/event`                           | nada do módulo                    |
| `lintcmd`   | `cmd/hermes-pipe-lint`                     | lint                              |
| `lint`      | `internal/lint`                            | nada do módulo                    |

`ds` (estruturas genéricas com teto, semana 10, ADR 0011) e `lint` (o
analisador, semana 6, ADR 0008) foram adicionadas depois desta decisão. O
analisador é uma ferramenta separada: nada do pipeline o importa.

Regras:

- Nenhuma seta aponta para trás. `event` não conhece ninguém.
- `pipeline` e `stage` não se conhecem: o runner recebe stages por interface, e
  quem liga os dois é `cmd/hermes-pipe` (ou `config`).
- Um stage não importa outro stage. Ele só pode importar o pacote pai
  `internal/stage`, onde fica o contrato.
- Todo pacote novo do módulo precisa ser classificado numa camada. Se não for, o
  teste falha.

A regra é aplicada por `architecture_test.go`, na raiz do módulo. O teste roda
`go list -deps -json ./...` e compara cada import com a tabela acima.
`TestArchitecture_DetectsViolations` confirma que o próprio verificador acusa as
violações.

## Alternativas consideradas

- **`pkg/` para os pacotes reutilizáveis.** Rejeitada: não há consumidor externo, e
  `pkg/` não tem nenhum efeito no compilador, é só convenção. Seria prometer
  estabilidade sem ganhar nada em troca.
- **Tudo na raiz do módulo, sem `internal/`.** Rejeitada: qualquer pacote poderia ser
  importado por terceiros, e as camadas ficariam só na convenção.
- **Vários módulos (um `go.mod` por camada).** Rejeitada: obriga a usar
  `replace`/workspaces e versionar cada parte separadamente, sem benefício num
  binário único.
- **Fiscalizar as camadas com linter (`depguard`) em vez de teste.** Rejeitada por
  enquanto: o teste roda com `go test`, não depende de configuração externa e
  testa a si mesmo. Se a tabela crescer, posso reavaliar.

## Consequências

- Um import proibido quebra o `go test` e, portanto, o job de teste do CI, com a
  mensagem `layer violation: ...`.
- Criar um pacote novo exige editar `layerOf` em `architecture_test.go`. É
  intencional: a decisão de onde o pacote fica passa a ser explícita.
- A raiz do módulo tem só `architecture_test.go` (package `hermespipe`). Ela não
  deve ganhar código de produção.
- Para atualizar o Go, mudo o `go.mod` e mais nada: o CI lê a versão de lá
  (`go-version-file`).
