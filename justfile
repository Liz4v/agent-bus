# agent-bus build, test and deploy recipes.
#
# CGO_ENABLED=0 is set per build recipe only, never exported: the race
# detector used by `just test` needs cgo.

broker_host := env("BROKER_HOST", "")
broker_arch := env("BROKER_ARCH", "arm64")
version := `git describe --tags --always --dirty 2>/dev/null || echo dev`
ldflags := "-s -w -X main.version=" + version

test:
    go test -race -cover ./...

build:
    mkdir -p dist
    CGO_ENABLED=0 go build -ldflags '{{ldflags}}' -o dist/agent-bus ./cmd/agent-bus

linux-arm64:
    mkdir -p dist
    CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -ldflags '{{ldflags}}' -o dist/agent-bus-linux-arm64 ./cmd/agent-bus

linux-armv7:
    mkdir -p dist
    CGO_ENABLED=0 GOOS=linux GOARCH=arm GOARM=7 go build -ldflags '{{ldflags}}' -o dist/agent-bus-linux-armv7 ./cmd/agent-bus

# Upload to a temporary name next to the installed binary, then rename it over
# the installed path. Writing over the running binary fails with ETXTBSY on
# every deploy after the first; rename replaces the directory entry and leaves
# the running process alone. The ssh user must be able to write to
# /usr/local/bin for the upload (the rename and restart use sudo).
deploy: _check-deploy
    just linux-{{broker_arch}}
    scp dist/agent-bus-linux-{{broker_arch}} {{broker_host}}:/usr/local/bin/agent-bus.new
    ssh {{broker_host}} 'sudo mv /usr/local/bin/agent-bus.new /usr/local/bin/agent-bus && sudo systemctl restart agent-bus'

[private]
_check-deploy:
    #!/usr/bin/env bash
    set -eu
    [ -n "{{broker_host}}" ] || { echo "set BROKER_HOST=user@host" >&2; exit 1; }
    case "{{broker_arch}}" in arm64|armv7) ;; *) echo "BROKER_ARCH must be arm64 or armv7" >&2; exit 1;; esac
