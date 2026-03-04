.PHONY: test lint build tidy

## test: run all tests with race detector enabled
test:
	go test -race ./...

## lint: run golangci-lint (must be installed: https://golangci-lint.run/usage/install/)
lint:
	golangci-lint run ./...

## build: compile both service binaries
build:
	CGO_ENABLED=0 go build ./cmd/processor
	CGO_ENABLED=0 go build ./cmd/scanner

## tidy: tidy and verify go.mod / go.sum
tidy:
	go mod tidy
	go mod verify
