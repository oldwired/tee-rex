.PHONY: build build-version test race lint vet fmt install

# Version metadata stamped into main.Version / main.BuildCommit /
# main.BuildTime so a `make build-version` binary reports its provenance via
# `tee-rex -version`. VERSION comes from `git describe` (e.g. 1.1.0,
# 1.1.0-3-gabc1234-dirty) with the leading v stripped; the source default is
# "dev". The Release workflow stamps VERSION from the pushed tag instead.
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null | sed 's/^v//')
COMMIT  ?= $(shell git rev-parse --short HEAD 2>/dev/null)
TIME    ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
LDFLAGS = -X main.Version=$(VERSION) -X main.BuildCommit=$(COMMIT) -X main.BuildTime=$(TIME)

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
