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

One Go binary (`cmd/agent-bus`, hand-rolled subcommand dispatch) with several roles. Each Claude Code session claims a broker-wide unique name; a human tells one session the other's name and the agents then exchange text, one sender and one recipient session per message. The broker is multi-user and trusted: accounts approved by one hostmaster, TLS with a pinned key.

- **broker** (`internal/broker`): HTTPS/JSON API (`/v1/register`, `/v1/account`, `/v1/sessions`, `/v1/name`, `/v1/messages`, `/v1/sync`, `/v1/admin/*`, ...) over SQLite (`modernc.org/sqlite`). Stores only SHA-256 hashes of session tokens and account credentials; claim codes live in memory (`claim.go`). A session's name lives on its `sessions` row (`names.go`: claim, release, send, and `depart`, the one path that tells partners a name went away); `partners` records which sessions exchanged a message (the recipients of presence events) and `departed` gives a former partner a precise send error. The `/v1/channels*` and `/v1/leave` routes only answer 410 `upgrade_required`. Accounts (`accounts.go`, `server_accounts.go`) own sessions: removing an account deletes its sessions in the same transaction, so every session-authenticated route stays one hash lookup. `sync` is a long-poll; a sweeper handles disconnect grace, pending-registration expiry and retired-row pruning. A host-side Unix **control socket** (`control.go`: `RECOVER`, `FINGERPRINT`) backs `agent-bus broker recover|fingerprint`.
- **identity** (`internal/identity`): the broker's ECDSA key, self-signed cert, and the `SHA256:` SPKI fingerprint clients pin. `internal/client` requires https plus a pin (`tls.go`), and `internal/brokerfile` is the client-side file (pin, credential, hostmaster hint) that the shim and the CLI share. `internal/enroll` holds the human-only, TTY-gated commands `pin`, `unpin`, `register`, `claim` and the read-only `status`. `internal/testutil` starts TLS test servers.
- **shim** (`internal/shim`): per-Claude-Code-session stdio MCP server exposing five tools (`tools.go`: `claim_name`, `send`, `read`, `release_name`, `wake_command`), plus six `admin_*` tools only when the broker file says hostmaster (`admin.go`, which also holds the subscription keeper that restores the admin subscription after a dropped session). Without a usable broker file it still serves MCP and every tool returns a setup message. It keeps a local mirror of broker state fed by a sync loop (`mirror.go`; the session token never reaches the agent) and serves a Unix **wake socket** (`wake.go`).
- **wait** (`internal/wait`): started by the agent in the background; connects to the shim's wake socket and blocks until messages are unread, then prints only sender names. By design it has no timeout; the shim notices a dead `wait` by hang-up.
- **hook stop** (`internal/hook`): Claude Code Stop hook that tells an about-to-idle agent about unread messages or an unarmed wake. Fails open (prints nothing, exit 0). It finds the shim socket via a PPID index file or process ancestry, which is why wrapper scripts must `exec` the shim.
- **hermes-wake** (`internal/hermeswake`): sidecar for Hermes Agent. Same wake-socket protocol as `wait`, but POSTs a signed webhook instead; the shim uses a fixed socket path (`AGENT_BUS_WAKE_SOCKET`) in that mode.
- `internal/protocol` holds the wire types, error codes and every shared constant (grace periods, read caps; the name rules are in `accounts.go`). `internal/client` is the typed broker client. `internal/e2e` wires a real broker to real shims.

Behaviours that span several files: peers are addressed by a bare, broker-wide unique name (case-insensitive; `admin` and `agent-bus` reserved; no listing); `text`, `peer_left`, `peer_removed` and `registration_pending` wake an idle session, while `peer_disconnected`/`peer_reconnected` are informational; a send to a name nobody holds fails at once and keeps nothing; the first text a session gets from a new sender session is flagged first-contact; messages are deleted on read; peer content is always framed as untrusted data (see "Security model" in the README before touching `read` output).

## Important check on every behavior change

`skills/agent-bus/SKILL.md` is the published skill that teaches agents the workflow, and `skills/agent-bus-hostmaster/SKILL.md` teaches the hostmaster's admin tools. They are symlinked into `~/.claude/skills/`, so edits here are live. They restate behaviour that lives in code: tool names and arguments, name rules, the wake and `wait` lifecycle and exit meanings, event kinds, name release and the session grace period, account and setup rules, and what wakes an idle session.

**Whenever you change the broker, shim, protocol, `wait`, the Stop hook or the tool descriptions, re-read both `SKILL.md` files and the "agent workflow" and "Security model" sections of `README.md` and update them in the same commit.** Check at least:
- tool list and `claim_name` name rules vs `internal/shim/tools.go` and `internal/protocol/accounts.go`;
- `wait` behaviour (no timeout; exits on pending messages or no name) vs `internal/shim/wake.go` and `internal/wait/wait.go`;
- event kinds and which of them wake vs `internal/protocol/protocol.go`;
- timings (grace periods, read cap) vs the "Facts" section;
- the six admin tools, their arguments and descriptions in `internal/shim/admin.go`, vs the hostmaster skill;
- account limits and codes (`DefaultMaxAccounts`, `PendingLimit`, `PendingExpiry`, `RetiredKeep`, error codes) in `internal/protocol/accounts.go`, vs both skills and the README;
- the human-only commands (`pin`, `unpin`, `register`, `claim`) and the setup messages in `internal/shim/tools.go` (`CheckSetup`) vs the "Accounts" section of `SKILL.md`.

Also keep the tool descriptions in `internal/shim/tools.go` and the Hermes sidecar consistent with the same behaviour.

## Repo conventions

- This repo is meant to be open source (Apache-2.0). Keep hostnames, personal paths and details of the author's own machines out of code, docs, tests and commit messages; use placeholders such as `broker-host`.
