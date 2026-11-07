# agent-bus

Go project; `just test` runs `go test -race -cover ./...`, `just build` makes `dist/agent-bus`.

## Important check on every broker or wake change

`skills/agent-bus/SKILL.md` is the published skill that teaches agents the workflow (it is symlinked into `~/.claude/skills/agent-bus`, so edits here are live). It restates behaviour that lives in code: tool names and arguments, name rules, the wake and `wait` command's lifecycle and exit meanings, event kinds, channel expiry, and what wakes an idle session.

**Whenever you change the broker, shim, protocol, `wait`, the Stop hook or the tool descriptions, re-read `SKILL.md` (and the "agent workflow" section of `README.md`) and update them in the same commit.** Check at least:
- tool list and `my_name` rules vs `internal/shim/tools.go`;
- `wait` behaviour (no timeout; exits on pending messages or no channels) vs `internal/shim/wake.go` and `internal/wait/wait.go`;
- event kinds and which of them wake vs `internal/protocol/protocol.go`;
- timings (channel TTL, grace periods, read cap) vs the "Facts" section.
Also keep the tool descriptions in `internal/shim/tools.go` and the Hermes sidecar (`internal/hermeswake`) consistent with the same behaviour.
