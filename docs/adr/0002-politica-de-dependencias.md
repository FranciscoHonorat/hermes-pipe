# ADR 0002: Política de dependências e organização do CI

- **Status:** aceito
- **Data:** 2026-10-12

## Contexto

O objetivo do projeto é entender Go por baixo: o runtime, a stdlib, o scheduler e
o GC. Cada dependência externa é código que eu não li, que pode trazer
vulnerabilidades e que pode puxar dependências transitivas. Em quase tudo que o
hermes-pipe faz, a stdlib basta: `encoding/json`, `bufio`, `context`, `log/slog`,
`testing`, `iter`.

A exceção é a semana 5 (pipelines em YAML). A stdlib não tem parser de YAML, e
escrever um não faz parte do objetivo.

Também preciso de um CI que pegue erro de verdade e que deixe claro **por que**
falhou.

## Decisão

### Dependências do módulo

1. **Stdlib, mais uma lib YAML e nada além disso.** A única dependência externa
   permitida é `go.yaml.in/yaml/v3`, e **só** no pacote `internal/config`.
2. A regra é aplicada pelo código, não por convenção: `allowedExternal` em
   `architecture_test.go` lista o que cada camada pode importar de fora. Um import
   externo fora dessa lista faz o teste falhar com
   `dependência externa proibida em "<camada>"`.
3. Para adicionar outra dependência, é preciso escrever um ADR novo e editar
   `allowedExternal` no mesmo PR.

### Ferramentas não entram no `go.mod`

`golangci-lint`, `govulncheck` e `benchstat` rodam no CI pela action ou com
`go run`/`go install pkg@versão`. Elas não aparecem em `go.mod`/`go.sum`. Assim o
grafo de dependências do binário continua sendo só o que o binário usa.

### CI: um job por pergunta

`.github/workflows/ci.yml` tem quatro jobs independentes:

| Job       | Pergunta que responde                         | Quando        |
|-----------|-----------------------------------------------|---------------|
| `test`    | O código compila, os testes passam com `-race`, e o `go.mod` está arrumado (`go mod tidy` + `git diff --exit-code`)? | push e PR |
| `lint`    | O código segue as regras do `.golangci.yml`?  | push e PR     |
| `vuln`    | Algum código vulnerável é **alcançável** a partir do nosso código? (`govulncheck`) | push e PR |
| `bench`   | O PR mudou o desempenho? (`benchstat` base × PR, `-count=10`) | só PR |

Configurações comuns a todos os jobs: `GOTOOLCHAIN=local`, versão do Go lida do
`go.mod`, `permissions: contents: read` e `shell: bash` (que liga o `pipefail`).

### Por que lint e teste ficam em jobs separados

- **Diagnóstico.** Cada job aparece como um ✓/✗ próprio no PR. "Lint falhou" e
  "teste falhou" são problemas diferentes e devem ser visíveis sem abrir o log.
- **Um não esconde o outro.** No mesmo job, um teste quebrado interromperia o job
  antes do lint (ou o contrário), e eu só veria um problema por vez.
- **Paralelismo.** Os jobs rodam ao mesmo tempo, então o tempo total é o do job
  mais lento, não a soma.
- **Ambientes diferentes.** O lint usa a action do golangci-lint, com cache e
  versão próprios (fixada em `v2.14.0`). O teste precisa do `-race`, que usa cgo.
  Misturar os dois acopla configurações que não têm relação.

## Alternativas consideradas

- **Sem nenhuma dependência: escrever um parser de YAML mínimo ou usar JSON para
  a config.** Rejeitada: um parser próprio sai do escopo e teria bugs. JSON não
  aceita comentários, o que é ruim para pipelines versionáveis.
- **Liberar dependências caso a caso, sem regra no código.** Rejeitada: sem uma
  verificação automática, a política vira só uma intenção.
- **Diretiva `tool` no `go.mod` (Go 1.24+) para fixar as ferramentas.** Rejeitada:
  ela coloca as ferramentas e as dependências delas no `go.mod`, misturando o
  grafo do binário com o das ferramentas. Pode ser reavaliada se a falta de
  versão fixa (`@latest`) começar a quebrar o CI.
- **Um único job de CI com todos os passos.** Rejeitada pelos motivos acima.
- **Só Dependabot, sem `govulncheck`.** Rejeitada: o Dependabot compara versões
  do `go.mod` com uma base de vulnerabilidades. O `govulncheck` analisa o grafo de
  chamadas e só acusa quando uma função vulnerável é realmente alcançada pelo
  nosso código. Ele também verifica a stdlib da toolchain usada.

## Consequências

- O `go.sum` fica vazio até a semana 5. Depois disso, deve ter só o
  `go.yaml.in/yaml/v3` e as dependências dele.
- Um PR que adiciona uma dependência proibida falha no job `test` pelo motivo
  certo. Esse é o gate da semana 0.
- `govulncheck@latest` e `benchstat@latest` não estão fixados, então uma versão
  nova dessas ferramentas pode mudar o resultado do CI sem nenhuma mudança no
  código. Aceito por enquanto. Se acontecer, fixo a versão.
- O job `bench` não faz nada até existirem benchmarks (semana 4). Ele não tem
  limite de regressão: só publica a tabela do `benchstat` no resumo do job, e
  decidir se a diferença importa fica comigo.
