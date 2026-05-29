.PHONY: build build-version test race lint vet fmt install

# Version metadata stamped into main.BuildCommit / main.BuildTime so a
# `make build-version` binary reports its provenance via `tee-rex -version`.
COMMIT ?= $(shell git rev-parse --short HEAD 2>/dev/null)
TIME   ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
LDFLAGS = -X main.BuildCommit=$(COMMIT) -X main.BuildTime=$(TIME)

build:
	go build .

# Release-style local build with version info baked in (mirrors the
# ldflags the Release workflow uses).
build-version:
	go build -trimpath -ldflags "$(LDFLAGS)" .

test:
	go test ./...

# tee-rex_test.go is white-box and exercises the concurrent fanOut path,
# so always run the race detector.
race:
	go test -race ./...

vet:
	go vet ./...

fmt:
	gofmt -w .

lint:
	golangci-lint run

install:
	go install .
