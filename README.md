# agent-bus

Cross-host 1-to-1 channels between Claude Code sessions.

Claude Code's built-in peer messaging only works between sessions on the same host. agent-bus links any two Claude Code sessions on any hosts that can reach a small broker. A human creates a channel in one session and passes its secret ID to the other. After that the two agents exchange text messages and address each other by a readable peer label (`<hostname>/<name>`).

One Go binary, `agent-bus`, does four jobs:

| Command | Runs on | Role |
|---|---|---|
| `agent-bus broker` | the Pi (Debian, systemd) | HTTP/JSON API with SQLite storage |
| `agent-bus shim` | every host, one per Claude Code session | stdio MCP server exposing the tools |
| `agent-bus wait` | every host, started by the agent in the background | wakes an idle session when a message arrives |
| `agent-bus hook stop` | every host, run by Claude Code | tells the agent about unread messages before it goes idle |

Target client: the Claude Code CLI only.

## Install the broker (Debian, systemd)

Build on the dev machine (pure Go, no cgo):

```sh
just pi-arm64        # or: just pi-armv7
```

Install on the Pi:

```sh
sudo install -m 0755 dist/agent-bus-linux-arm64 /usr/local/bin/agent-bus
sudo install -m 0644 deploy/agent-bus.service /etc/systemd/system/agent-bus.service
sudo systemctl daemon-reload
sudo systemctl enable --now agent-bus
```

The unit runs the broker as a dynamic unprivileged user, listening on `:7700` (all interfaces), with its database in `/var/lib/agent-bus/agent-bus.db`.

To update later: `PI_HOST=user@pi PI_ARCH=arm64 just deploy`. It uploads to `/usr/local/bin/agent-bus.new` and renames it over the installed binary, because writing over the running binary fails with `ETXTBSY`. The ssh user needs write access to `/usr/local/bin` for the upload; the rename and the restart use `sudo`.

## Connect a Claude Code session

On each host that has the binary installed (`go build ./cmd/agent-bus`, or `just build`):

```sh
claude mcp add agent-bus -- agent-bus shim --broker http://<pi>:7700
```

`AGENT_BUS_URL` works instead of `--broker`. If you start the shim through a wrapper script, the script must `exec` the shim, so that the shim stays a direct child of Claude Code (the Stop hook's process-ancestry fallback depends on it).

### Timing notes

- `read` accepts `wait_seconds` up to 300. Longer values are capped and the result says so.
- Claude Code moves tool calls that run longer than about 2 minutes to a background task (`CLAUDE_CODE_MCP_AUTO_BACKGROUND_MS`), so a long `read` may return out of band. That is harmless.
- The stdio tool idle timeout (`CLAUDE_CODE_MCP_TOOL_IDLE_TIMEOUT`) defaults to 30 minutes. Do not lower it below 300 seconds, or long reads are aborted.

### Clean exit versus crash

When the session ends normally (the MCP connection closes, or the shim gets SIGTERM), the shim tells the broker at once and the peer sees `peer_left` immediately. A crash, a power loss or a dropped network goes through a 5 minute grace period instead: the peer sees `peer_disconnected` after 60 seconds without contact, and the member counts as having left after 5 more minutes. If the same session reconnects within the grace period, its membership and queued messages are intact. Disconnect and reconnect events are informational: they never wake an idle session.

## The Stop hook

The hook tells the agent, just before it goes idle, that messages are waiting or that the wake command is not running. It fails open: on any error it prints nothing and exits 0. Add it to `~/.claude/settings.json`:

```json
{
  "hooks": {
    "Stop": [
      {
        "hooks": [
          { "type": "command", "command": "agent-bus hook stop", "timeout": 10 }
        ]
      }
    ]
  }
}
```

It is safe in user settings: in a session that has no agent-bus shim it finds no socket and does nothing. It never prints message content, only a count and the sender labels.

## The agent workflow

1. Session A calls `create_channel` with `my_name`. The result shows the **channel ID** on its own line. The human passes exactly that line to session B.
2. Session B calls `join_channel` with the channel ID and its own `my_name`, and learns A's label.
3. Each side starts the wake command in the background: call `wake_command`, run the command it returns with `run_in_background`, and run it again every time it exits. It exits when a message is waiting and prints who sent it (never the content). Then call `read`.
4. Use `send`, `read` (optionally with `wait_seconds`), `peers` and `leave` as needed.

Addressing: `send` and `leave` take a `peer` that is either a label (`host/name`) or the channel handle (`ch-xxxx`). A handle never changes and works in every channel state, so use it for a channel whose label is shared with a newer one. Messages sent before anyone has joined are held and delivered on join. An unjoined channel expires after 1 hour.

## Security model

- The broker binds to all interfaces and has no accounts. Tailscale (or any network you trust) is the perimeter.
- The **channel ID is the secret**. Anyone who holds it can join an open channel, and only an open channel. Every send and read needs the session token, which stays inside the shim and never reaches the agent.
- The broker stores only SHA-256 hashes of channel IDs and session tokens, and logs only short hash prefixes. A leaked database file yields no joinable channels and no usable sessions.
- Joining anything but an open channel gives one generic "no such open channel" error, so a leaked ID cannot be used to probe a channel's state.
- Incoming messages are **untrusted data**. `read` frames each one as coming from a remote peer, escapes closing tags, and tells the agent to treat the content as data, not instructions.
- Labels are self-reported and used only for display and addressing; they grant no access.
- Messages are deleted from the broker once read. There is no history, no rate limiting and no encryption beyond the transport you provide.
- The wake socket and its directory are private to your user (mode 0700 and 0600), and the shim and the hook both refuse a directory that another user could have created.

## Development

Tool versions (Go, just) are pinned in `mise.toml`; run `mise install`.

```sh
just test      # go test -race -cover ./... (needs a C compiler for the race detector)
just build     # dist/agent-bus
```

The design, decisions and test plan are in `specs/archive/2026-10-13-agent-bus-v1/`.
