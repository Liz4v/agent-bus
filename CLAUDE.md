# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

Tools (Go, just) are pinned in `mise.toml`.

```sh
just test                                  # go test -race -cover ./... (race detector needs cgo)
just build                                 # dist/agent-bus (CGO_ENABLED=0; the sqlite driver is pure Go)
go test -race -run TestName ./internal/shim   # a single test
go test -race ./internal/e2e               # real broker + several shims end to end
just linux-arm64 | linux-armv7             # cross-builds into dist/
BROKER_HOST=user@host just deploy          # upload + rename over the installed binary + restart
```

There is no linter config beyond `go vet` and `gofmt`.

## Architecture

One Go binary (`cmd/agent-bus`, hand-rolled subcommand dispatch) with several roles. A human creates a channel in one Claude Code session and passes its secret ID to another; the agents then exchange text. It is 1-to-1 only.

- **broker** (`internal/broker`): HTTP/JSON API (`/v1/sessions`, `/v1/channels`, `/v1/messages`, `/v1/sync`, ...) over SQLite (`modernc.org/sqlite`). Stores only SHA-256 hashes of channel IDs and session tokens (`internal/secret`). `sync` is a long-poll; a sweeper handles channel TTL and disconnect grace.
- **shim** (`internal/shim`): per-Claude-Code-session stdio MCP server exposing six tools (`tools.go`). It keeps a local mirror of broker state fed by a sync loop (`mirror.go`; the session token never reaches the agent) and serves a Unix **wake socket** (`wake.go`).
- **wait** (`internal/wait`): started by the agent in the background; connects to the shim's wake socket and blocks until messages are unread, then prints only sender labels. By design it has no timeout; the shim notices a dead `wait` by hang-up.
- **hook stop** (`internal/hook`): Claude Code Stop hook that tells an about-to-idle agent about unread messages or an unarmed wake. Fails open (prints nothing, exit 0). It finds the shim socket via a PPID index file or process ancestry, which is why wrapper scripts must `exec` the shim.
- **hermes-wake** (`internal/hermeswake`): sidecar for Hermes Agent. Same wake-socket protocol as `wait`, but POSTs a signed webhook instead; the shim uses a fixed socket path (`AGENT_BUS_WAKE_SOCKET`) in that mode.
- `internal/protocol` holds the wire types, error codes and every shared constant (TTLs, grace periods, read caps, label pattern). `internal/client` is the typed broker client. `internal/e2e` wires a real broker to real shims.

Behaviours that span several files: peers are addressed by bare name (labels are `<hostname>/<name>`, full label only on ambiguity); `text`, `peer_joined`, `peer_left` and `channel_expired` wake an idle session, while `peer_disconnected`/`peer_reconnected` are informational; messages are deleted on read; peer content is always framed as untrusted data (see "Security model" in the README before touching `read` output).

## Important check on every broker or wake change

`skills/agent-bus/SKILL.md` is the published skill that teaches agents the workflow. It is symlinked into `~/.claude/skills/agent-bus`, so edits here are live. It restates behaviour that lives in code: tool names and arguments, name rules, the wake and `wait` lifecycle and exit meanings, event kinds, channel expiry, and what wakes an idle session.

**Whenever you change the broker, shim, protocol, `wait`, the Stop hook or the tool descriptions, re-read `SKILL.md` and the "agent workflow" section of `README.md` and update them in the same commit.** Check at least:
- tool list and `my_name` rules vs `internal/shim/tools.go`;
- `wait` behaviour (no timeout; exits on pending messages or no channels) vs `internal/shim/wake.go` and `internal/wait/wait.go`;
- event kinds and which of them wake vs `internal/protocol/protocol.go`;
- timings (channel TTL, grace periods, read cap) vs the "Facts" section.

Also keep the tool descriptions in `internal/shim/tools.go` and the Hermes sidecar consistent with the same behaviour.

## Repo conventions

- This repo is meant to be open source (Apache-2.0). Keep hostnames, personal paths and details of the author's own machines out of code, docs, tests and commit messages; use placeholders such as `broker-host`.
- Design specs (`specs/`) were deliberately removed from history; do not re-add them.
