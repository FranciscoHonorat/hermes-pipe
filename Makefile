BINARY  := hermes-pipe
BIN_DIR := bin
CMD     := ./cmd/hermes-pipe
PKGS    ?= ./...

FUZZ_PKG  ?=
FUZZ      ?=
FUZZTIME  ?= 30s

BENCH_COUNT ?= 10
BENCH_OUT   ?= bench.txt

OLD ?= old.txt
NEW ?= new.txt

.DEFAULT_GOAL := help
COVER_MIN ?= 80
FUZZ_TARGETS := ./internal/event:FuzzParseEvent ./internal/event:FuzzLineReader \
	./internal/stage/window:FuzzWindow ./internal/config:FuzzConfig ./internal/pipeline:FuzzPipeline

.PHONY: help build release lint lint-arch test bench fuzz fuzz-all stress cover cover-check mutate benchcmp

help:
	@echo "Usage: make [target]"
	@echo "Targets:"
	@echo "  build      Compila $(BIN_DIR)/$(BINARY)"
	@echo "  release    Build reproduzível (-trimpath, CGO_ENABLED=0)"
	@echo "  lint-arch  Roda o hermes-pipe-lint sobre internal/stage"
	@echo "  fuzz-all   Roda todos os alvos de fuzz por FUZZTIME"
	@echo "  cover-check Falha se a cobertura de internal/ < $(COVER_MIN)%"
	@echo "  mutate     Roda os 10 mutantes (scripts/mutate.sh)"
	@echo "  lint       Roda o golangci-lint"
	@echo "  test       Roda os testes com -race"
	@echo "  bench      Roda os benchmarks e grava em $(BENCH_OUT)"
	@echo "  fuzz       Roda um alvo de fuzz (FUZZ_PKG=... FUZZ=...)"
	@echo "  stress     Roda os testes com -race -count=50"
	@echo "  cover      Gera cobertura (resumo + coverage.html)"
	@echo "  benchcmp   Compara $(OLD) e $(NEW) com benchstat"

build:
	go build -o $(BIN_DIR)/$(BINARY) $(CMD)

release:
	CGO_ENABLED=0 go build -trimpath -o $(BIN_DIR)/$(BINARY) $(CMD)
	go version -m $(BIN_DIR)/$(BINARY) | head -3
	sha256sum $(BIN_DIR)/$(BINARY)

lint-arch:
	go run ./cmd/hermes-pipe-lint ./internal/stage/...

lint:
	golangci-lint run $(PKGS)

test:
	go test -race $(PKGS)

bench:
	go test -run='^$$' -bench=. -benchmem -count=$(BENCH_COUNT) $(PKGS) > $(BENCH_OUT); \
		status=$$?; cat $(BENCH_OUT); exit $$status

fuzz:
	@test -n "$(FUZZ_PKG)" -a -n "$(FUZZ)" || { echo "uso: make fuzz FUZZ_PKG=./internal/event FUZZ=FuzzParseEvent"; exit 2; }
	go test -run='^$$' -fuzz='^$(FUZZ)$$' -fuzztime=$(FUZZTIME) $(FUZZ_PKG)

fuzz-all:
	@for t in $(FUZZ_TARGETS); do \
		pkg=$${t%%:*}; fn=$${t##*:}; \
		echo "== $$fn"; \
		go test -run='^$$' -fuzz="^$$fn$$" -fuzztime=$(FUZZTIME) $$pkg || exit 1; \
	done

stress:
	go test -race -count=50 $(PKGS)

cover:
	go test -coverprofile=coverage.out $(PKGS)
	go tool cover -func=coverage.out
	go tool cover -html=coverage.out -o coverage.html

cover-check:
	go test -short -coverpkg=./internal/... -coverprofile=coverage.out ./internal/...
	@total=$$(go tool cover -func=coverage.out | awk '/^total:/ {sub("%","",$$NF); print $$NF}'); \
		echo "cobertura internal/: $$total% (mínimo $(COVER_MIN)%)"; \
		awk -v t="$$total" -v m="$(COVER_MIN)" 'BEGIN { exit !(t >= m) }'

mutate:
	./scripts/mutate.sh

benchcmp:
	benchstat $(OLD) $(NEW)
