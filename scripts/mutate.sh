#!/usr/bin/env bash
# Testes de mutação: introduz um bug por vez e confere que algum teste falha.
# Um mutante que sobrevive (testes verdes com o bug) é um buraco na suíte.
#
#   ./scripts/mutate.sh          # roda os 10 mutantes
#   ./scripts/mutate.sh 3        # roda só o mutante 3
#
# Cada mutante troca a primeira ocorrência de um trecho literal num arquivo.
# O arquivo original é restaurado sempre, mesmo com Ctrl+C.
set -euo pipefail
cd "$(dirname "$0")/.."

# arquivo | trecho original | trecho mutante | pacote a testar | o que quebra
mutants=(
  "internal/pipeline/run.go|	defer close(out)
	for soft.Err() == nil {|	for soft.Err() == nil {|./internal/pipeline|producer não fecha o channel (I3/I2)"
  "internal/pipeline/stats.go|c.dropped.Add(1)|c.dropped.Add(0)|./internal/pipeline|descartes não contados (I1)"
  "internal/stage/filter/filter.go|return eq == (c.op == opEq)|return eq|./internal/stage/filter|!= vira =="
  "internal/stage/mapper/mapper.go|out := e.Clone()|out := e|./internal/stage/mapper|map modifica o evento recebido (I4)"
  "internal/stage/limit/limit.go|if l.seen == l.n {|if l.seen == l.n+1 {|./internal/stage/limit|limit não pede parada"
  "internal/event/reader.go|return lr.seq, lr.buf[:len(lr.buf)-1], nil|return lr.seq, lr.buf[:len(lr.buf)-2], nil|./internal/event|off-by-one no slice da linha"
  "internal/event/event.go|if !ok \|\| raw == nil {|if !ok {|./internal/event|time null deixa de ser ausente"
  "internal/stage/window/window.go|if first \|\| e.Time.After(w.maxSeen) {|if true {|./internal/stage/window|watermark recua (I5)"
  "internal/config/config.go|if !slices.Contains(known, k.Value) {|if false {|./internal/config|campo desconhecido aceito"
  "internal/lint/lint.go|isFunc && fn.Signature().Recv() == nil|isFunc|./internal/lint|métodos de time.Time viram falso positivo"
)

only="${1:-}"
killed=0
survived=0
backup=""
target=""
restore() { if [[ -n "$backup" ]]; then cp "$backup" "$target"; rm -f "$backup"; backup=""; fi; }
trap restore EXIT INT TERM

for i in "${!mutants[@]}"; do
  n=$((i + 1))
  [[ -n "$only" && "$only" != "$n" ]] && continue
  IFS='|' read -r -d '' target from to pkg desc < <(printf '%s\0' "${mutants[$i]//\\|/$'\x01'}") || true
  from="${from//$'\x01'/|}"
  to="${to//$'\x01'/|}"
  backup="$(mktemp)"
  cp "$target" "$backup"
  FROM="$from" TO="$to" perl -0pi -e 's/\Q$ENV{FROM}\E/$ENV{TO}/' "$target"
  if cmp -s "$target" "$backup"; then
    echo "mutante $n: trecho não encontrado em $target (o código mudou?)" >&2
    exit 2
  fi
  if go test -count=1 -short -timeout 120s "$pkg" >/dev/null 2>&1; then
    echo "SOBREVIVEU  $n: $desc ($target)"
    survived=$((survived + 1))
  else
    echo "morto       $n: $desc"
    killed=$((killed + 1))
  fi
  restore
done

echo "mutantes mortos: $killed/$((killed + survived))"
[[ "$survived" -eq 0 ]]
