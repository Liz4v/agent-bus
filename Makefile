# agent-bus build, test and deploy targets.
#
# CGO_ENABLED=0 is set per build target only, never exported: the race
# detector used by `make test` needs cgo.

PI_HOST ?=
PI_ARCH ?= arm64
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -s -w -X main.version=$(VERSION)

.PHONY: test build pi-arm64 pi-armv7 deploy

test:
	go test -race -cover ./...

build:
	mkdir -p dist
	CGO_ENABLED=0 go build -ldflags '$(LDFLAGS)' -o dist/agent-bus ./cmd/agent-bus

pi-arm64:
	mkdir -p dist
	CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -ldflags '$(LDFLAGS)' -o dist/agent-bus-linux-arm64 ./cmd/agent-bus

pi-armv7:
	mkdir -p dist
	CGO_ENABLED=0 GOOS=linux GOARCH=arm GOARM=7 go build -ldflags '$(LDFLAGS)' -o dist/agent-bus-linux-armv7 ./cmd/agent-bus

# Upload to a temporary name next to the installed binary, then rename it over
# the installed path. Writing over the running binary fails with ETXTBSY on
# every deploy after the first; rename replaces the directory entry and leaves
# the running process alone. The ssh user must be able to write to
# /usr/local/bin for the upload (the rename and restart use sudo).
deploy:
	@test -n "$(PI_HOST)" || { echo "set PI_HOST=user@host" >&2; exit 1; }
	@case "$(PI_ARCH)" in arm64|armv7) ;; *) echo "PI_ARCH must be arm64 or armv7" >&2; exit 1;; esac
	$(MAKE) pi-$(PI_ARCH)
	scp dist/agent-bus-linux-$(PI_ARCH) $(PI_HOST):/usr/local/bin/agent-bus.new
	ssh $(PI_HOST) 'sudo mv /usr/local/bin/agent-bus.new /usr/local/bin/agent-bus && sudo systemctl restart agent-bus'
