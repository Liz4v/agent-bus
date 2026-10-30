# agent-bus build, test and deploy recipes.
#
# CGO_ENABLED=0 is set per build recipe only, never exported: the race
# detector used by `just test` needs cgo.

pi_host := env("PI_HOST", "")
pi_arch := env("PI_ARCH", "arm64")
version := `git describe --tags --always --dirty 2>/dev/null || echo dev`
ldflags := "-s -w -X main.version=" + version

test:
    go test -race -cover ./...

build:
    mkdir -p dist
    CGO_ENABLED=0 go build -ldflags '{{ldflags}}' -o dist/agent-bus ./cmd/agent-bus

pi-arm64:
    mkdir -p dist
    CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -ldflags '{{ldflags}}' -o dist/agent-bus-linux-arm64 ./cmd/agent-bus

pi-armv7:
    mkdir -p dist
    CGO_ENABLED=0 GOOS=linux GOARCH=arm GOARM=7 go build -ldflags '{{ldflags}}' -o dist/agent-bus-linux-armv7 ./cmd/agent-bus

# Upload to a temporary name next to the installed binary, then rename it over
# the installed path. Writing over the running binary fails with ETXTBSY on
# every deploy after the first; rename replaces the directory entry and leaves
# the running process alone. The ssh user must be able to write to
# /usr/local/bin for the upload (the rename and restart use sudo).
deploy: _check-deploy
    just pi-{{pi_arch}}
    scp dist/agent-bus-linux-{{pi_arch}} {{pi_host}}:/usr/local/bin/agent-bus.new
    ssh {{pi_host}} 'sudo mv /usr/local/bin/agent-bus.new /usr/local/bin/agent-bus && sudo systemctl restart agent-bus'

[private]
_check-deploy:
    #!/usr/bin/env bash
    set -eu
    [ -n "{{pi_host}}" ] || { echo "set PI_HOST=user@host" >&2; exit 1; }
    case "{{pi_arch}}" in arm64|armv7) ;; *) echo "PI_ARCH must be arm64 or armv7" >&2; exit 1;; esac
