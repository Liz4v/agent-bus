# Architect: Multi-User Trusted Broker

> Phase 2 — Design decisions. Approved before coding begins.
> Implementation checklist is in tasks.md.

## Approach

The broker gains accounts as a first-class row that owns sessions. Everything else about channels, the inbox, the long-poll sync and the shim mirror is unchanged. Each session row carries the ID of the account it was created under. Every existing session-authenticated route keeps working unchanged, because a session exists only while its account is active: revoking an account deletes its sessions in the same transaction. So "is this caller allowed?" stays one hash lookup per request, exactly as today.

The design reuses the existing patterns throughout:
- Secrets: claim codes, account credentials and session tokens are random, sent as bearer values, and stored as SHA-256 only (`internal/secret`).
- Writes: every mutation goes through `Store.update` (one `BEGIN IMMEDIATE` transaction, then a notify broadcast to the bumped sessions).
- Events: new event kinds are plain inbox rows. They reach the shim through the existing sync, so wake, `wait`, the Stop hook and the Hermes sidecar need almost no changes.
- Shared constants and wire types live in `internal/protocol`.
- The shim keeps every secret away from the agent. Agent-facing text is framed as untrusted data.

New moving parts, each in its own file or package:

| Part | Where | Why separate |
|---|---|---|
| Broker identity (key, self-signed cert, fingerprint) | `internal/identity` | Used by the broker, the client, the CLI and tests. Pure crypto, no I/O beyond the key file. |
| Client-side broker file (pin + credential) | `internal/brokerfile` | Shared by the shim and the human CLI. One file per broker, mode 0600. |
| Human enrolment flows (pin, unpin, register, claim, status) | `internal/enroll` | Interactive and TTY-gated. Kept out of `main.go` so it can be tested with injected I/O. |
| Account store operations | `internal/broker/accounts.go` | The largest new logic. Keeps `store.go` and `channels.go` focused. |
| Claim-code state | `internal/broker/claim.go` | In-memory, single-use, re-issued per start. |
| Host-side control socket | `internal/broker/control.go` | The recovery command and fingerprint printing. Being able to reach the socket is the proof of host access. |
| Account and admin HTTP handlers | `internal/broker/server_accounts.go` | Keeps `server.go` as wiring. |
| Admin MCP tools | `internal/shim/admin.go` | Registered only for hostmaster credentials. |
| TLS test helper | `internal/testutil` | One place to start a TLS test server and get its pin. Imports only `identity`, to avoid import cycles. |

### Design decisions

**M1 — Accounts own sessions; revocation is deletion.**
- A session row gets `account_id`. Registration creates an `accounts` row with `status` = `pending` or `active`.
- Reject, expiry and removal delete the account row, which frees its name. The row is replaced by a `retired` row keyed by the credential hash and holding the outcome, so `status` can still answer "rejected / expired / removed".
- Retired rows are pruned after `RetiredKeep` (7 days, user decision). After that the credential is simply unknown (401).

**M2 — Hard cut-over migration.**
- Migration 2 drops `inbox`, `held`, `members`, `channels` and `sessions`, then recreates them with the new columns. It also creates `accounts`, `retired` and `settings`.
- Nothing from before the upgrade survives, which the Analyst approved. A v1 database opens as v2 with no accounts, so the broker starts unclaimed.

**M3 — Credentials and authentication.**
- Account credential: `secret.NewToken()` (32 random bytes, hex). It is returned once, by `POST /v1/register`.
- The broker stores only `cred_hash`.
- `POST /v1/sessions` now requires `Authorization: Bearer <credential>`. The other session routes keep `Bearer <session token>`.
- Credential failures map to distinct 403 codes, so the shim can tell the human exactly what is wrong. Codes: `account_pending`, `account_rejected`, `account_expired`, `account_removed`, and an unknown credential gives 401.
- A session token never authenticates as a credential, and a credential never authenticates as a session token, because each route looks in exactly one table.

**M4 — Claim codes live in memory.**
- When the broker starts with no hostmaster, it generates a claim code and logs it at WARN on stderr (the journal). Only its hash is kept, in memory.
- Each restart therefore invalidates the previous code automatically.
- Format: 20 characters of RFC 4648 base32 (100 bits), shown as `XXXXX-XXXXX-XXXXX-XXXXX`. Input is normalised by uppercasing and removing `-` and spaces. Comparison is constant-time on the hashes.
- At most one code is live at a time. A code has a *mode*: `claim` (no hostmaster) or `recover` (replaces the existing hostmaster).
- The claim check, the code consumption and the account insert happen under one mutex held across the store transaction. So of two simultaneous claims exactly one wins, and the code is consumed only if the insert commits.
- Every refusal that involves a claim code returns the single generic `registration_refused`. That covers a missing code on an unclaimed broker, a wrong code, a used code, and a code sent to a claimed broker with no recovery pending.
- The claim code is the one secret the broker logs. This is documented as the residual risk.
- Issuing goes through one exported method, `(*Store).IssueClaim(ctx) (code, mode string, err error)`, which picks the mode from whether a hostmaster exists. `Run` (start-time issue), the control socket (`RECOVER`) and tests in other packages (client, shim, e2e harnesses) all use it; the `claims` type itself stays unexported. *(Critic R1-B2.)*

**M5 — Host-side control socket for recovery and fingerprint.**
- The broker listens on a Unix socket, default `<dir of --db>/control.sock`, mode 0600, inside the state directory. The state directory is `/var/lib/private/agent-bus` under `DynamicUser`, mode 0700, so only root and the broker's own user can reach it.
- Protocol (same style as the wake socket): one line in, one JSON line out.
  - `RECOVER` issues a fresh code: mode `recover` if a hostmaster exists, otherwise `claim`. The code is returned to the caller and not logged.
  - `FINGERPRINT` returns the broker's public-key fingerprint.
- `agent-bus broker recover` and `agent-bus broker fingerprint` are thin clients of this socket. They are run with `sudo` on the broker host.
- Like the wake socket, a control path longer than 100 bytes is a clear start error rather than a truncated bind.
- This avoids opening the database from another process: a root-owned `-wal` or `-shm` file would break a `DynamicUser` broker. It also works while the broker is running.

**M6 — Recovery replaces the hostmaster atomically.**
- A successful `recover`-mode claim does everything in one transaction:
  - deletes the old hostmaster account, with the full removal path from M8;
  - inserts the new account as active and hostmaster;
  - consumes the code.
- When checking whether the new name is taken, the old hostmaster's name is ignored, so the human can reclaim the same name.
- A partial unique index (`is_hostmaster = 1`) guarantees at most one hostmaster.

**M7 — Registration admission order** (one transaction). Each check depends only on what earlier checks established:
1. Expire stale pending rows (lazy, so counts are exact).
2. A claim code is present → M4/M6 path. A code is absent and there is no hostmaster → `registration_refused`. The claim path still applies checks 3 and 4 below (in `recover` mode the old hostmaster's name is ignored for check 4, per M6), but not 5 and 6: a claim never adds to the active count beyond replacing or creating the one hostmaster. *(Critic R1-W5.)*
3. `ValidName` fails → `registration_refused`.
4. The name is taken, case-insensitively, by an active or pending account → `registration_refused`.
5. The pending count is at or above `PendingLimit` (5) → `registration_refused`.
6. The active count is at or above `max_accounts` → `registration_refused`.
7. Insert the pending row, then enqueue a `registration_pending` event to every session that has `admin_sub = 1` (only the hostmaster's sessions can have it).

*User decision (Phase 2), which supersedes the Analyst's "clear reason" wording for name-taken and the limits:* the unauthenticated register endpoint returns one generic `registration_refused` for **every** refusal, so a stranger learns nothing about existing names or broker capacity. The broker logs the real reason at INFO (no credential, no code), so the hostmaster can see it in the journal. The CLI checks the name pattern locally before sending, so a malformed name still gets a precise message without any broker state being disclosed. The refusal message tells the registrant to try another name or ask the hostmaster.

`approve` re-checks the active cap (6) and returns `account_limit` to the authenticated hostmaster, so lowering the cap never lets an approval exceed it. `approve`, `reject` and `ListAccounts` run the lazy pending expiry (1) first in their own transaction, so a request past `PendingExpiry` that the sweeper has not reached yet is `no_such_account`, never approved. *(Critic R1-N1.)* The cap is a `settings` row. When the row is absent the cap is `DefaultMaxAccounts` (5). The valid range is `1..MaxAccountsCeiling` (1000).

**M8 — Removal reason on the channel.**
- `removeMember` gains a reason: `left` (today's behaviour) or `removed`.
- With `removed`, a joined channel moves to a new channel state `removed`, not `ended`. The remaining member gets a waking `peer_removed` event, and its channel list shows `peer_removed`.
- `send` to such a channel returns `peer_removed` (410) with the message "the peer's account was removed by the hostmaster; the channel has ended".
- Removing an account runs `deleteSession(reason=removed)` for each of its sessions. That deletes the inbox (unread messages discarded) and an open channel the account created (held messages discarded).
- Session reaping and clean exit keep `left`.
- `Remove`, and `Register` on the recovery path, return the IDs of the sessions they deleted. The HTTP handler calls `tracker.Forget(id)` for each, as `deleteSelf` and the sweeper's reaping already do, so the in-memory liveness map does not keep (or, through a late deferred `Touch`, re-add) deleted sessions. *(Critic R1-W2.)*
- Every successful admin mutation (approve, reject, remove, max-accounts change, recovery claim) and every subscribe/unsubscribe is logged at INFO with the action, the target account name and the acting account name, never a credential or code. An agent can perform these actions, so the journal is the hostmaster's audit trail. *(Critic R1-W7.)*

**M9 — Account names in channel data.**
- The broker copies the verified account name at create/join time, alongside the label it already stores:
  - `members.account` and `members.peer_account`;
  - `held.from_account`;
  - `inbox.from_account`.
- Wire additions:
  - `Message.FromAccount *string`
  - `Channel.PeerAccount *string`
  - `KnownPeer.Account`
  - `JoinChannelResponse.PeerAccount`
  - `CreateSessionResponse.Account`
- `read` adds an `account="…"` attribute to `<peer-message>`, escaped like the other attributes. The content stays untrusted exactly as today.
- Account names never appear for channels the session is not a member of. There is no listing route for other users' sessions or channels.

**M10 — Admin authorisation and guard rails.**
- `/v1/admin/*` routes use session-token auth and then require the session's account to be the hostmaster. Otherwise they return 403 `forbidden`.
- Every mutating admin request names exactly one account (`name`, validated with `ValidName`, so wildcards and empty values are impossible) and checks its state:
  - `approve` and `reject` only touch `pending`;
  - `remove` only touches `active` and non-hostmaster accounts;
  - anything else returns 409 `wrong_state` or 404 `no_such_account`.
- The hostmaster cannot be removed through the API. Only M6 replaces a hostmaster.

**M11 — Admin notification subscription.**
- `sessions.admin_sub` (0/1) is set by `POST /v1/admin/subscribe`. It lives and dies with the session row.
- The response carries the current pending count, so a fresh subscriber learns about requests that arrived earlier. Events are not replayed.
- An admin event is an inbox row with `handle = "admin"` (`protocol.AdminHandle`), no `from_label`, kind `registration_pending`, and the registrant's name as text.
- `registration_pending` is waking.
- Rendering: the row's `text` is the registrant's name, so `shim.body` must handle `registration_pending` **before** its "non-empty text is the body" shortcut. It renders a fixed explanatory sentence with the name embedded (the name already matches `NamePattern`, and the whole body still goes through `escapeContent`). Otherwise the frame would show only the bare name and lose the confirm-with-your-human guidance. *(Critic R1-B1.)*
- `SyncResponse.AdminSubscribed` tells the shim the session is subscribed.
- `SetAdminSub` bumps the caller's session version in its transaction, and the `admin_subscribe` tool calls `Mirror.Refresh` before returning, so the mirror knows the new state at once rather than after the next 20 s long-poll. *(Critic R1-B3.)*
- The wake socket does not reply `no_channels` while the session is subscribed. That way a Hermes gateway with no channels stays armed, and the Stop hook treats a subscribed session like one with live channels. `StatusReply` gains `subscribed` (additive).
- "Subscribed" for the wake socket and `STATUS` means `Summary.AdminSubscribed || Session.WantAdmin()`: the broker-confirmed flag **or** the shim's local intent from M12. Without the second half, a `wait` started right after `admin_subscribe`, or during the keeper's re-establishment gap after a drop (the mirror is reset to empty), would get `no_channels` and exit, and a Claude Code hostmaster would stop listening. *(Critic R1-B3.)*

**M12 — The shim keeps the subscription alive** (auto-restore confirmed by the user in Phase 2). The subscription belongs to the agent's session, which is the shim process. If the broker session is dropped (401 after a reap), the shim re-establishes it in the background when the agent had subscribed:
- it re-runs `Ensure` and then subscribes again;
- it backs off 1 s to 60 s between attempts;
- it stops on shutdown, on unsubscribe, on a 403 (the account is no longer hostmaster, or no longer active), or on a 401 from `POST /v1/sessions` (the credential is unknown, e.g. its retired row was pruned). When it stops for a 401 or 403 it clears `wantAdmin`, so the wake socket can again answer `no_channels`, and the session notice says the subscription could not be restored and why. *(Critic R1-W3.)*

Without this, an unattended Hermes hostmaster would silently stop waking after a network outage. The one-time drop notice says that channels were lost and that the subscription was restored or not.

**M13 — TLS and broker authenticity.**

*Broker side:*
- On first start, `identity.LoadOrCreateKey` creates an ECDSA P-256 key as a PKCS#8 PEM file. The file is created with `O_EXCL` and mode 0600 at `--key` (default `<dir of --db>/broker-key.pem`).
- An existing but unreadable or invalid key file is a fatal start error and is never silently regenerated.
- At every start the broker builds a fresh self-signed certificate from that key, valid for 1 year from the start. That makes renewal automatic while the key, and so the fingerprint, stays the same.
- The listener is `tls.NewListener`, TLS 1.3 minimum. A plain HTTP request gets Go's standard 400 "client sent an HTTP request to an HTTPS server", so it is refused.
- The fingerprint is logged at every start.

*Fingerprint:* `SHA256:` plus the unpadded standard base64 of the SHA-256 of the certificate's DER SubjectPublicKeyInfo. This is the OpenSSH style, so it is familiar.

*Client side (`internal/client`):*
- The client always requires `https://` and a pin. `client.New(Config)` returns an error otherwise.
- The TLS config sets `InsecureSkipVerify: true` with a `VerifyConnection` callback that compares the leaf certificate's SPKI fingerprint with the pin. The CA chain is irrelevant by design.
- A mismatch yields `*client.PinError{Expected, Got}`. The handshake fails before any bearer value is sent.
- `client.Probe(url)` performs a handshake that only captures the fingerprint. It is for the human pin flow only.

**M14 — Client-side broker file.** Location: `$AGENT_BUS_CONFIG_DIR`, else `$XDG_CONFIG_HOME/agent-bus`, else `~/.config/agent-bus`, then `brokers/<host>_<port>.json`. Directory 0700, file 0600, written atomically. Fields:
- normalised `url` (`https://host:port`, port 7700 by default, `http://` refused);
- `fingerprint` (the pin);
- optional `account`, `credential` and `hostmaster`.

Copying this one file to another host carries both the trust decision and the account, which matches the "copy the credential file" decision. `hostmaster` is a local hint only. It decides whether the shim registers admin tools; the broker always enforces.

**M15 — Human-only operations are TTY-gated CLI commands, never MCP tools.**
- New commands: `agent-bus pin`, `agent-bus unpin`, `agent-bus register`, `agent-bus claim`, `agent-bus status`.
- `pin`, `unpin`, `register` and `claim` refuse to run unless stdin is a terminal (`os.Stdin.Stat()`, `ModeCharDevice`), and they require typed confirmation. The claim code is read from the prompt, never from a flag or the environment, so it does not land in shell history or agent transcripts.
- `register` and `claim` run the pin flow inline on first contact (TOFU).
- Before sending the request, `register` and `claim` check that the broker file can be written (create the directory, then write and remove a temporary file next to it), so a successful registration cannot lose its credential to a write error. If the file already holds a credential, they show the stored account name and require typed confirmation before replacing it, because the old credential is not recoverable afterwards. *(Critic R1-W6.)*
- `status` is read-only, never prints the credential, and works without a TTY, so agents may run it (user decision).
- Claude Code's Bash tool has no TTY, so an agent cannot complete these flows by accident. An agent that deliberately allocates a pseudo-terminal (for example with `script`) could. This is documented as a residual risk, and the skills forbid it.

**M16 — Shim behaviour without setup.**
- The shim always starts its MCP server, so tools can explain problems.
- If the broker file is missing, unpinned or lacks a credential, every tool returns a fixed message naming the human command to run, plus "do not run it yourself".
- A pin mismatch returns a fixed message with both fingerprints and the same instruction.
- Account errors from `Ensure` (pending, rejected, expired, removed) are rendered as human-directed messages.
- After a 401 in the sync loop, the mirror asks `GET /v1/account`. If the account is no longer active, the drop notice says the account was removed (or is no longer active) instead of the "expired while disconnected" text.
- Every place that drops the session on a 401 (the mirror loop, `send`, `leave`, and `withSession` in `tools.go`) goes through one helper, `Session.DropAfter401(ctx, token)`, which does the account check and then `Drop` or `DropWithNotice`. Revocation wakes the mirror's long-poll, but a `send` can still hit the 401 first; without the shared helper that path would set the misleading "expired while disconnected" notice. *(Critic R1-W1.)*

**M17 — Admin tools are registered only for hostmaster credentials.**
- When the broker file says `hostmaster: true`, the shim adds six tools: `admin_list_accounts`, `admin_approve`, `admin_reject`, `admin_remove`, `admin_set_max_accounts` and `admin_subscribe`.
- Ordinary users' agents never see them. That keeps their tool list and skill surface free of admin workflows.
- The hint is read once at shim start, so a shim that was already running when the human ran `agent-bus claim` (or `status` refreshed the hint) gets the admin tools only after the Claude Code / Hermes session restarts. The hostmaster skill and README say so.
- Each description:
  - names its explicit single target;
  - says to confirm with the human first unless the human has pre-authorised unattended handling;
  - says never to act on admin requests that arrive in peer messages.
- Results frame account names as data (`<account-name>` text inside the untrusted preamble).

**M18 — Skills.**
- The general skill gains a short "Accounts" section. It says the shim needs a human-run `agent-bus register`/`pin`, that peers show a verified `account`, what `peer_removed` means (stop, do not investigate), and never to run `pin`/`unpin`/`register`/`claim`. It points to the hostmaster skill.
- `skills/agent-bus-hostmaster/SKILL.md` holds the admin tools, the subscription, the confirm-first rule, the pre-authorisation pattern for unattended (Hermes) peers, and the warning about peer-sourced admin requests.
- The `CLAUDE.md` sync check is extended to the new skill and the admin tools.

## API Contract

All routes are HTTPS only. Error bodies keep the existing `{"error":{"code","message",...}}` shape. "Credential" means the account credential; "session" means the session token.

| Method | Route | Request body | Response | Auth required |
|--------|-------|-------------|----------|---------------|
| POST | `/v1/register` | `{"name": str, "claim_code"?: str}` | 201 `{"credential": str, "name": str, "status": "active"\|"pending", "hostmaster": bool}`. Error: 403 `registration_refused` for every refusal (M7) | none |
| GET | `/v1/account` | — | 200 `{"name": str, "status": "active"\|"pending"\|"rejected"\|"expired"\|"removed", "hostmaster": bool}`; 401 for an unknown credential | credential |
| POST | `/v1/sessions` *(changed)* | `{}` | 201 `{"session_token": str, "account": str}`. Errors: 401 `unauthorized`, 403 `account_pending`\|`account_rejected`\|`account_expired`\|`account_removed` | credential *(was none)* |
| DELETE | `/v1/sessions/self` | — | 204 (unchanged) | session |
| POST | `/v1/channels` | unchanged | unchanged | session |
| POST | `/v1/channels/join` | unchanged | 200 `{"handle", "peer_label", "peer_account"}` *(added field)* | session |
| POST | `/v1/messages` | unchanged | 202. New error: 410 `peer_removed` | session |
| POST | `/v1/leave` | unchanged | 204. Removed channels resolve and can be left like ended ones | session |
| POST | `/v1/sync` | unchanged | adds `messages[].from_account`, `channels[].peer_account`, channel state `peer_removed`, `admin_subscribed` | session |
| GET | `/v1/admin/accounts` | — | 200 `{"accounts": [{"name", "status": "active"\|"pending", "hostmaster", "created_at", "expires_at"?}], "max_accounts", "active", "pending", "pending_limit"}` | session, hostmaster |
| POST | `/v1/admin/approve` | `{"name": str}` | 200 `{"name", "status": "active"}`. Errors: 404 `no_such_account`, 409 `wrong_state`, 403 `account_limit` | session, hostmaster |
| POST | `/v1/admin/reject` | `{"name": str}` | 204. Errors: 404, 409 `wrong_state` | session, hostmaster |
| POST | `/v1/admin/remove` | `{"name": str}` | 204. Errors: 404, 409 `wrong_state` (pending, or the hostmaster) | session, hostmaster |
| POST | `/v1/admin/max-accounts` | `{"max": int}` (1..1000) | 200 `{"max_accounts", "active"}`. Error: 400 `bad_request` | session, hostmaster |
| POST | `/v1/admin/subscribe` | `{"subscribed": bool}` | 200 `{"subscribed": bool, "pending": int}` | session, hostmaster |

Non-hostmaster sessions get 403 `forbidden` on every `/v1/admin/*` route. The check runs before body validation, so the routes do not reveal anything.

**Control socket** (Unix, host-side only): one request line, one JSON reply line.

| Command | Reply |
|---|---|
| `RECOVER` | `{"claim_code": str, "mode": "claim"\|"recover"}` |
| `FINGERPRINT` | `{"fingerprint": str}` |
| other | `{"error": "unknown command"}` |

**New CLI surface:**

| Command | Who | TTY | Effect |
|---|---|---|---|
| `agent-bus broker [--listen] [--db] [--key] [--control]` | service | no | As today, plus TLS, a claim code when unclaimed, and the control socket |
| `agent-bus broker recover [--control]` | operator (sudo) | no | Prints a fresh claim/recovery code |
| `agent-bus broker fingerprint [--control]` | operator (sudo) | no | Prints the broker fingerprint, to compare at pin time |
| `agent-bus pin --broker URL` | human | yes | Probes, shows the fingerprint, records it after `yes`. Refuses if a different pin exists |
| `agent-bus unpin --broker URL` | human | yes | Removes the pin (keeps the credential) |
| `agent-bus register --broker URL --name NAME` | human | yes | Pins on first contact, registers, stores the credential |
| `agent-bus claim --broker URL --name NAME` | human | yes | Pins on first contact, prompts for the code, registers as hostmaster |
| `agent-bus status [--broker URL]` | human or agent | no | Shows the account name and status, and refreshes the local `hostmaster` hint |
| `agent-bus shim --broker URL` *(changed)* | Claude Code / Hermes | no | URL default scheme `https`, reads the broker file |

`--broker` falls back to `AGENT_BUS_URL` everywhere, as the shim does today.

## Data Model Changes

Migration 2 (`PRAGMA user_version = 2`) needs a migration step. It drops and recreates the five existing tables (M2) and adds three new ones.

```
accounts(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL,                 -- as registered
  name_key TEXT NOT NULL UNIQUE,      -- lower(name), case-insensitive uniqueness
  cred_hash BLOB NOT NULL UNIQUE,
  status TEXT NOT NULL CHECK (status IN ('pending','active')),
  is_hostmaster INTEGER NOT NULL DEFAULT 0,
  created_at INTEGER NOT NULL,
  decided_at INTEGER NULL)
CREATE UNIQUE INDEX accounts_one_hostmaster ON accounts(is_hostmaster) WHERE is_hostmaster = 1

retired(
  cred_hash BLOB PRIMARY KEY,
  name TEXT NOT NULL,
  outcome TEXT NOT NULL CHECK (outcome IN ('rejected','expired','removed')),
  at INTEGER NOT NULL)

settings(key TEXT PRIMARY KEY, value TEXT NOT NULL)   -- 'max_accounts'

sessions   + account_id INTEGER NOT NULL REFERENCES accounts ON DELETE RESTRICT
           + admin_sub INTEGER NOT NULL DEFAULT 0
channels   state CHECK adds 'removed'
members    + account TEXT NOT NULL, + peer_account TEXT NULL
held       + from_account TEXT NOT NULL
inbox      + from_account TEXT NULL
CREATE INDEX sessions_account ON sessions(account_id)
```

`ON DELETE RESTRICT` on `sessions.account_id` makes it impossible to delete an account without first deleting its sessions through the removal path (M8).

`protocol` constants:
- `DefaultMaxAccounts = 5`
- `MaxAccountsCeiling = 1000`
- `PendingLimit = 5`
- `PendingExpiry = 2h`
- `RetiredKeep = 7 × 24h`
- `NamePattern = ^[A-Za-z0-9._-]{1,64}$` (moved from `shim/text.go`)

## Files to Create

| File | Purpose |
|------|---------|
| `internal/protocol/accounts.go` | Account and admin wire types, status and code constants, limits, `NamePattern`/`ValidName` |
| `internal/identity/identity.go` | Load or create the broker key, build the self-signed cert, compute the fingerprint, build the server TLS config |
| `internal/identity/identity_test.go` | Tests for the above |
| `internal/testutil/tlsserver.go` | `TLSServer(t, http.Handler) (*httptest.Server, pin string)` for tests in other packages |
| `internal/brokerfile/brokerfile.go` | Client-side broker file: `Dir`, `NormalizeURL`, `PathFor`, `Load`, `Save` (atomic, 0600) |
| `internal/brokerfile/brokerfile_test.go` | Tests |
| `internal/broker/claim.go` | Claim-code state: issue (mode), check+consume under a mutex, formatting and normalisation |
| `internal/broker/claim_test.go` | Tests |
| `internal/broker/accounts.go` | Store methods: `Register`, `AccountByCredential`, `CreateSession` auth path, `Approve`, `Reject`, `Remove`, `ListAccounts`, `SetMaxAccounts`, `SetAdminSub`, `expirePending`, `pruneRetired`, `deleteAccount` |
| `internal/broker/accounts_test.go` | Tests |
| `internal/broker/server_accounts.go` | HTTP handlers for register, account and admin routes, and the hostmaster gate |
| `internal/broker/server_accounts_test.go` | Tests |
| `internal/broker/control.go` | Control socket server (`RECOVER`, `FINGERPRINT`) and the client used by the CLI |
| `internal/broker/control_test.go` | Tests |
| `internal/client/tls.go` | Pinned transport, `PinError`, `ErrNotPinned`, `Probe` |
| `internal/client/tls_test.go` | Tests |
| `internal/client/accounts.go` | `Register`, `AccountStatus`, admin calls |
| `internal/client/accounts_test.go` | Tests |
| `internal/enroll/enroll.go` | Interactive `pin`, `unpin`, `register`, `claim` and `status` flows with injected I/O and a TTY check |
| `internal/enroll/enroll_test.go` | Tests |
| `internal/shim/admin.go` | Admin MCP tools and the subscription keeper (M12) |
| `internal/shim/admin_test.go` | Tests |
| `skills/agent-bus-hostmaster/SKILL.md` | Hostmaster skill |

## Files to Modify

| File | Change |
|------|--------|
| `internal/protocol/protocol.go` | Kinds `peer_removed` and `registration_pending` (both waking), `StatePeerRemoved`, `AdminHandle`, `CodePeerRemoved`, account fields on `Message`/`Channel`/`KnownPeer`/`JoinChannelResponse`/`CreateSessionResponse`, `SyncResponse.AdminSubscribed`, `StatusReply.Subscribed` |
| `internal/protocol/protocol_test.go` | Cover `ValidName` and the new waking kinds |
| `internal/broker/store.go` | Migration 2; claim state on `Store`; `CreateSession(ctx, accountID)`; `removeMember`/`deleteSession` take a reason; `Sync` returns `from_account` and `admin_subscribed`; `channelList` returns `peer_account` and the `removed` state |
| `internal/broker/channels.go` | Copy account names into `members`/`held`/`inbox`; `ErrPeerRemoved` on send; account in known peers; join returns `peer_account` |
| `internal/broker/sweeper.go` | Expire pending registrations, prune retired rows; reaping passes reason `left` |
| `internal/broker/server.go` | Routes; `createSession` authenticates the credential; `Config.KeyPath`/`ControlPath`; `Run` loads the identity, issues and logs the claim code, logs the fingerprint, starts the control socket, serves TLS |
| `internal/broker/store_test.go`, `channels_test.go`, `sweeper_test.go`, `server_test.go` | Adapt to account-owned sessions; new cases |
| `internal/client/client.go` | `New(Config) (*Client, error)` with URL, pin and credential; https only; `CreateSession` sends the credential |
| `internal/client/client_test.go` | TLS test server with a pin |
| `internal/shim/text.go` | `account` attribute; bodies for `peer_removed` and `registration_pending`; preamble mentions registrants; `ValidateName` uses `protocol.ValidName` |
| `internal/shim/session.go` | Account-error rendering; `DropWithNotice`; `DropAfter401` (the single 401 drop path); `wantAdmin`/`WantAdmin()`; admin subscription keeper hook |
| `internal/shim/mirror.go` | `AdminSubscribed` in `Snapshot`/`Summary`; account check after a 401 |
| `internal/shim/wake.go` | A subscribed session never gets `no_channels`; `StatusReply.Subscribed` |
| `internal/shim/tools.go` | Setup and pin errors; account in the join result and known peers; registers admin tools when the hint is set |
| `internal/shim/*_test.go` (`text`, `session`, `mirror`, `wake`, `wake_fixed`, `tools`) | Adapt to the TLS client; new cases |
| `internal/hook/hook.go`, `hook_test.go` | Treat `subscribed` like live channels for the unarmed-wake reminder |
| `cmd/agent-bus/main.go` | New subcommands, broker flags, `broker recover`/`fingerprint`, shim loads the broker file, `https` default |
| `internal/e2e/e2e_test.go` | TLS broker plus accounts in the harness; new end-to-end scenarios |
| `skills/agent-bus/SKILL.md` | Accounts section, `account` field, `peer_removed`, pointer to the hostmaster skill, human-only commands |
| `README.md` | Install (key, claim, fingerprint), Connect (pin/register/claim/status), agent workflow, Hermes (https URL, credential file, subscription), Security model rewrite |
| `CLAUDE.md` | Architecture: accounts, identity, brokerfile, enroll, control socket. Sync check: hostmaster skill and admin tools |

The logging middleware keeps logging only a short hash of the bearer value, but on the credential-authenticated routes (`POST /v1/sessions`, `GET /v1/account`) it uses the attribute name `cred` instead of `session`, so the journal does not mislabel it. *(Critic R1-N2.)*

`deploy/agent-bus.service` needs no change: the key and the control socket live in the existing `StateDirectory`, and `ExecStart` stays the same.

## Upgrade and Rollback

*(Critic R1-W4.)*
- **Upgrade order:** deploy the broker first and claim it, then upgrade each client host (binary, `https://` URL in the MCP config, `agent-bus register` or `claim`). Old shims cannot talk to the new broker (plain HTTP is refused) and new shims cannot talk to an old broker (no TLS), so there is no mixed period that works; the README says so.
- **Existing client configs** that pass `http://<broker-host>:7700` (README examples, Hermes `config.yaml`) stop working. The shim's setup message for a refused `http://` URL tells the human to change it to `https://`.
- **Rollback:** migration 2 is one-way. An old binary opening a v2 database sees `user_version >= 1`, skips migration and then fails on the new `NOT NULL` columns. Rolling back therefore means: stop the broker, move `agent-bus.db*` aside (the key file can stay), install the old binary, start it, and switch clients back to `http://`. Accounts are lost on rollback; re-upgrading needs a new claim. The README documents this next to the backup guidance.

## Dependencies Between Steps

The step numbers refer to `tasks.md`.

- Steps 1–2 (protocol) come before everything else.
- Step 4 (identity) comes before 6 (testutil), 17 (server `Run`), 26 (client TLS) and 32 (enroll).
- Step 7 (brokerfile) comes before 32 (enroll), 38 (tools: setup errors) and 49 (main).
- Broker chain: 9 → 10 (both edit `store.go`, sequential) → 11 (channels) → 12 (claim; step 10 compiles only once it exists) → 13 (accounts, which needs claim state and the reason-aware removal) → 14 (sweeper) → 15 (handlers) → 16 (control socket) → 17 (server wiring, which starts the control socket).
- Broker tests (18–25) come after 17. Checkpoint A: `go test -race ./internal/broker` passes after step 25.
- Client: 26 → 27 → 28, then tests 29–31 (these need 6 and the broker handler from 17). Step 27 changes `client.New`, so the shim, e2e and main stop compiling until steps 35–50 adapt them. Checkpoint B: `go test -race ./internal/client` passes after step 31.
- Step 32 (enroll) needs 4, 7 and 26–28.
- Shim: 34 (text) → 35 (session) → 36 (mirror) → 37 (wake) → 38 (tools) → 39 (admin), then tests 40–46. Checkpoint C: `go test -race ./internal/shim` passes after step 46.
- Steps 47–48 (hook) need step 2 only.
- Step 49 (main) needs 7, 12–17, 26–28, 32 and 38–39. From here `go build ./...` is green again.
- Step 50 (e2e) needs everything above.
- Steps 51–54 (docs and skills) need the final behaviour from 38–39 and 49. Step 55 is the final verification.

## Parallelisation Opportunities

Steps are safe to run in parallel only when they touch different files and do not need each other's exported symbols.

- **Safe in parallel after 1–2:**
  - 4–5 (identity);
  - 7–8 (brokerfile);
  - 9–17 (broker code; internally sequential);
  - 47–48 (hook).
- **Safe in parallel after 4–8:** broker code (9–17) and client code (26–28) touch disjoint packages. The client tests (29–31) wait for step 17 because they run the real broker handler.
- **Within the shim:** 34 (`text.go`) and 37 (`wake.go`) are independent. Steps 35, 36, 38 and 39 form a chain (`session` → `mirror` → `tools` → `admin`).
- **Docs:** 51, 52, 53 and 54 are four independent files.

File locks that must not be parallelised:
- `store.go`: steps 9 and 10.
- `protocol.go`: step 2 only, but every package reads it, so finish it first.
- `server.go`: step 17.
- `main.go`: step 49.
- `e2e_test.go`: step 50.
- The test files of each package compile together. Steps 18–25 (broker) and 40–46 (shim) can be written in parallel, but they can only be run once every test file in the package compiles.

## Risks & Open Questions

Known risks that Phase 3 (Critic) should scrutinise:
1. **TTY gate is a speed bump, not a wall.** An agent with shell access running as the same user can allocate a pseudo-terminal, or edit the broker file directly. The guarantees are only that the paths are not MCP tools, that Claude Code's Bash tool has no TTY, and that the skills forbid it. *User decision (Phase 2): accepted, and the residual risk is documented in the README Security model.*
2. **Claim code in the journal.** Anyone who can read the broker host's logs before the hostmaster claims can claim. This is accepted in the Analyst; check that it is documented and that `recover` does not log its code.
3. **Generic registration refusal (user decision).** Every `/v1/register` refusal is the same `registration_refused`, so name existence and capacity are not disclosed. Check that the status code, message and timing do not differ between refusal causes, and that the INFO log line with the real reason carries no secret. The cost: a genuine registrant cannot tell a taken name from a full broker without asking the hostmaster.
4. **No rate limit on `/v1/register` or credential guesses.** Pending flooding is bounded by `PendingLimit`. Credentials and claim codes have ≥100 bits of entropy. Repeated invalid requests only cost log lines.
5. **Subscription keeper (M12)** adds a background goroutine in the shim. Check it stops on shutdown and on 403, and cannot loop hot.
6. **Migration 2 is destructive.** It drops all channel and session data on upgrade, which is approved. Check that a v2 database is never re-migrated, and that a failed migration rolls back fully.
7. **The hostmaster hint in the broker file can go stale.** For example, a hostmaster is replaced by recovery but keeps the file. The broker enforces, so the effect is only visible tools that fail with `forbidden`. `agent-bus status` refreshes the hint.
8. **Self-signed cert validity.** The certificate is re-issued at each start with a 1-year lifetime. A broker that runs for more than a year without a restart serves an expired cert. Clients ignore validity (pin-only), so this is harmless, but confirm that no code path verifies validity.
9. **`peers` tool mismatch.** The Analyst mentions account names "in `peers`", but this repository's shim has no `peers` tool (the binary installed on the dev machine is a different build that has one). This design shows the account wherever a peer is identified today: the join result, `read` frames, known-peers lists in errors, and the synced channel list. It does **not** add a `peers` tool. *User decision (Phase 2): no new tool. Peer listing is a separate change.*
10. **Hermes on a different host or user** needs the broker file (pin + credential) in that user's config dir. The README must spell out copying it, or running `register` there.
11. **Broker key file and the `DynamicUser` state dir.** The key lives in `/var/lib/private/agent-bus`. Check that the backup and restore guidance in the README keeps the key and the database together. Restoring one without the other forces every client to re-pin, or leaves credentials orphaned.
12. **Labels can now collide across users.** Labels are `<hostname>/<name>` and self-reported. Two users whose machines share a common hostname (for example `laptop`) and pick the same name produce the same label, and `JoinChannel` refuses with `same_label` ("cannot join under the creator's own label"), which is confusing when the creator is someone else. This is unchanged behaviour and the channel model is out of scope; the workaround is to pick another `my_name`. *(Critic R1, residual risk.)*

## Architect Checklist

- [x] Approach fits existing project patterns
- [x] API contract defined before any code (if applicable)
- [x] Schema changes identified (or explicitly none)
- [x] Auth and ownership checks included in plan
- [x] No step requires modifying multiple unrelated files
- [x] Parallelisation opportunities and file locks declared
- [x] Notification/event needs considered for mutations affecting live state
