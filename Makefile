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
.PHONY: help build lint test bench fuzz stress cover benchcmp

help:
	@echo "Usage: make [target]"
	@echo "Targets:"
	@echo "  build      Compila $(BIN_DIR)/$(BINARY)"
	@echo "  lint       Roda o golangci-lint"
	@echo "  test       Roda os testes com -race"
	@echo "  bench      Roda os benchmarks e grava em $(BENCH_OUT)"
	@echo "  fuzz       Roda um alvo de fuzz (FUZZ_PKG=... FUZZ=...)"
	@echo "  stress     Roda os testes com -race -count=50"
	@echo "  cover      Gera cobertura (resumo + coverage.html)"
	@echo "  benchcmp   Compara $(OLD) e $(NEW) com benchstat"

build:
	go build -o $(BIN_DIR)/$(BINARY) $(CMD)

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

stress:
	go test -race -count=50 $(PKGS)

cover:
	go test -coverprofile=coverage.out $(PKGS)
	go tool cover -func=coverage.out
	go tool cover -html=coverage.out -o coverage.html

benchcmp:
	benchstat $(OLD) $(NEW)
