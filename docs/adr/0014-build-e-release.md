# ADR 0014: Build reproduzível e release

- **Status:** proposto (falta a tag `v1.0.0`)
- **Data:** 2026-11-25

## Contexto

A semana 13 fecha a v1.0: um binário versionado e reproduzível que outra pessoa
baixa, roda e confia. Todo o código é `internal/` (ADR 0001), então a **API
pública é a CLI e o schema YAML**: flags, subcomandos, formato de saída,
códigos de saída e campos do YAML.

## Decisão

### Versão pelo VCS, sem `-ldflags`

- `hermes-pipe version` (e `--version`) lê `debug.ReadBuildInfo()`. O `go
  build` embute a versão do módulo e as informações do VCS (`vcs.revision`,
  `vcs.modified`, `vcs.time`), e desde o 1.24 o `go build` local também deriva
  uma pseudo-versão do commit (`v0.0.0-<data>-<hash>`, com `+dirty` se a
  árvore estiver modificada). Com a tag `v1.0.0` e `go install
  ...@v1.0.0`, a versão é `v1.0.0`.
- `-ldflags -X main.version=...` seria necessário só para uma versão que não
  vem do VCS (ex.: build a partir de um tarball sem `.git`). Não é o caso, e
  sem `-ldflags` não existe uma segunda fonte de verdade.
- `go version -m bin/hermes-pipe` mostra a mesma informação sem rodar o
  binário (`make release` imprime).

### Build reproduzível

```
CGO_ENABLED=0 go build -trimpath -o bin/hermes-pipe ./cmd/hermes-pipe
```

- `-trimpath` tira os caminhos absolutos da máquina do binário (nomes de
  arquivo nos stack traces e no build info). Sem ele, dois builds em
  diretórios diferentes dão hashes diferentes.
- `CGO_ENABLED=0` faz um binário estático, sem depender da libc nem do
  compilador C da máquina.
- `TestBinary_Reproducible` compila duas vezes, em diretórios de saída
  diferentes, e exige o mesmo SHA-256. `GOOS`/`GOARCH` mudam o binário (e
  ele é reproduzível para cada par).

### Estabilidade da CLI e do schema

`TestCLI_Stable` guarda em golden a saída de `--help`, `run --help` e
`hermes-pipe schema` (gerado por reflexão a partir dos structs `Config`).
Renomear uma flag ou um campo do YAML muda o golden, o diff aparece no PR, e a
mudança precisa ser deliberada (é quebra de compatibilidade a partir da v1).

### Otimizações: só as com número

| Ficou | Número |
|---|---|
| Buffer padrão 64 | ~205 mil vs ~130 mil eventos/s com buffer 0 (ADR 0006) |
| `emit` + buffer reusado (`collector`) | `Filter` com 0 alocações (ADR 0004) |
| Regex compilada no `New` | ~15× vs compilar por evento (ADR 0004) |
| Contadores atômicos | sem lock entre goroutines independentes (ADR 0006) |
| Hash de 8 bytes no dedup | memória independente do tamanho da chave (ADR 0011) |

| Não entrou | Por quê |
|---|---|
| `sync.Pool` para eventos | o Event é passado adiante e sobrevive; não há ponto claro de devolução, e nenhuma medição mostrou o GC como gargalo |
| Lotes nos workers | sem ganho medido onde o parse domina (ADR 0012) |
| `map[string]json.RawMessage` | parse mais barato, mas cada stage pagaria de novo (ADR 0003) |
| Particionar stages com estado | complexidade sem gargalo medido (ADR 0012) |

## Consequências

- `make release` gera o binário, imprime `go version -m` e o SHA-256.
- Falta, para fechar a semana 13: criar a tag `v1.0.0`, e os itens pessoais do
  roadmap (responder os "Explicar" sem consulta e o texto de 2.000 palavras).
