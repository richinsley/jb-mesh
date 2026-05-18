# jb-mesh build helpers

BINARY = jb-mesh
CMD = ./cmd/jb-mesh
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")

# Local build
build:
	go build -o $(BINARY) $(CMD)

# Cross-compile for Linux amd64
build-linux:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o $(BINARY)-linux $(CMD)

# Test
test:
	go test ./... -timeout 60s

test-v:
	go test ./... -v -timeout 60s

# Clean local build artifacts
clean:
	rm -f $(BINARY) $(BINARY)-linux $(BINARY)-new $(BINARY)-backup $(BINARY)-bin

.PHONY: build build-linux test test-v clean
