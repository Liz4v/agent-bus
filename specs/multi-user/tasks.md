# Tasks: Multi-User Trusted Broker

> Phase 2 output — implementation checklist.
> Each step is atomic: one file created, one migration, one component.
> Dependencies and parallelisation opportunities are documented in Architect.md.
> Design references (M1–M18) point to Architect.md.

### Layer: Protocol
- [ ] Step 1: Create `internal/protocol/accounts.go`. Contents:
  - constants `DefaultMaxAccounts=5`, `MaxAccountsCeiling=1000`, `PendingLimit=5`, `PendingExpiry=2h`, `RetiredKeep=7*24h`;
  - account statuses (`active`, `pending`, `rejected`, `expired`, `removed`);
  - error codes (`registration_refused` (the only register refusal, M7), `account_limit` (approve only), `account_pending`, `account_rejected`, `account_expired`, `account_removed`, `forbidden`, `wrong_state`, `no_such_account`);
  - `NamePattern` and `ValidName`;
  - the request and response types for register, account, all admin routes and the control socket (API Contract).

  Done: `go build ./internal/protocol`.
- [ ] Step 2: Modify `internal/protocol/protocol.go`:
  - add `KindPeerRemoved` and `KindRegistrationPending`, and make both waking in `IsWaking`;
  - add `StatePeerRemoved`, `AdminHandle = "admin"` and `CodePeerRemoved`;
  - add `Message.FromAccount`, `Channel.PeerAccount`, `KnownPeer.Account`, `JoinChannelResponse.PeerAccount`, `CreateSessionResponse.Account`, `SyncResponse.AdminSubscribed` and `StatusReply.Subscribed` (M9, M11).

  Done: `go build ./internal/protocol`.
- [ ] Step 3: Modify `internal/protocol/protocol_test.go`: table tests for `ValidName` (pattern, length 64, case, injection-like strings rejected) and `IsWaking` for the two new kinds. Done: `go test -race ./internal/protocol`.

### Layer: Identity and test helper
- [ ] Step 4: Create `internal/identity/identity.go` (M13):
  - `LoadOrCreateKey(path)`: ECDSA P-256, PKCS#8 PEM, `O_EXCL` 0600 on create; an existing invalid file is an error;
  - `SelfSignedCert(key, now)`: valid for 1 year;
  - `Fingerprint(cert)` and `FingerprintSPKI(der)`: `SHA256:` plus unpadded base64 of the SPKI hash;
  - `ServerTLSConfig(cert)`: TLS 1.3 minimum.

  Done: `go build ./internal/identity`.
- [ ] Step 5: Create `internal/identity/identity_test.go`:
  - create, then reload gives the same key and fingerprint;
  - two certs from one key share a fingerprint;
  - the file mode is 0600;
  - a corrupt or unreadable file is an error and is not overwritten;
  - the fingerprint format is right.

  Done: `go test -race ./internal/identity`.
- [ ] Step 6: Create `internal/testutil/tlsserver.go`: `TLSServer(t, http.Handler) (*httptest.Server, pin string)`. It uses a fresh `identity` key and cert and must not import `broker` or `client`. Done: `go build ./internal/testutil`.

### Layer: Client-side broker file
- [ ] Step 7: Create `internal/brokerfile/brokerfile.go` (M14):
  - `Dir()`, from `AGENT_BUS_CONFIG_DIR`, then `XDG_CONFIG_HOME`, then `~/.config/agent-bus`;
  - `NormalizeURL`: defaults to `https://`, port 7700, refuses `http://`, strips the path;
  - `PathFor(url)` → `brokers/<host>_<port>.json`;
  - `Load` and `Save`: atomic write, directory 0700, file 0600; `Load` refuses a group- or world-readable file;
  - the `File` struct: `url`, `fingerprint`, `account`, `credential`, `hostmaster`.

  Done: `go build ./internal/brokerfile`.
- [ ] Step 8: Create `internal/brokerfile/brokerfile_test.go`: URL normalisation table (including `http://` refused and IPv6 hosts), directory precedence, round trip, modes, permissive-mode refusal, missing file. Done: `go test -race ./internal/brokerfile`.

### Layer: Broker
- [ ] Step 9: Modify `internal/broker/store.go` for migration 2 only (M2, Data Model):
  - drop and recreate the five tables with their new columns, and create `accounts`, `retired` and `settings` and the indexes, all in one transaction;
  - set `user_version = 2`;
  - a v1 database migrates and a v2 database is left untouched;
  - add a `maxAccounts(tx)` helper that reads `settings` with the default.

  Done: `go vet ./internal/broker` compiles the non-test code.
- [ ] Step 10: Modify `internal/broker/store.go` for account-owned sessions and removal reasons (M1, M8, M9, M11):
  - `CreateSession(ctx, accountID)` stores `account_id`;
  - `removeMember(m, reason)` and `deleteSession(id, reason)`: with `removed`, a joined channel goes to state `removed` and the peer gets `peer_removed`;
  - `Sync` returns `from_account` and `admin_subscribed`;
  - `channelList` returns `peer_account` and maps `removed` to `StatePeerRemoved`;
  - `Store` holds the claim state from step 12 as a field.

  Done: compiles once step 12 exists, so check after step 12.
- [ ] Step 11: Modify `internal/broker/channels.go` (M8, M9):
  - `CreateChannel` and `JoinChannel` copy the session's account name into `members.account` and `peer_account`;
  - `held.from_account` and `inbox.from_account` are filled on send and on held delivery;
  - `JoinChannel` returns the peer's account;
  - `Send` to a `removed` channel returns the new `ErrPeerRemoved` (410);
  - `unknownPeer` includes the account;
  - `resolve` and `Leave` handle the `removed` state like `ended`.

  Done: compiles.
- [ ] Step 12: Create `internal/broker/claim.go` (M4):
  - a `claims` type with a mutex: `Issue(mode) string`, `Check(code) (mode, ok)` and `Consume()`, used under the mutex held across the store transaction;
  - base32 formatting with dashes, normalisation, constant-time hash compare;
  - a `HasHostmaster`-driven start-time issue helper used by `Run`;
  - the exported `(*Store).IssueClaim(ctx) (code, mode string, err error)` (M4), the single entry point used by `Run`, the control socket and the client/shim/e2e test harnesses.

  Done: `go vet ./internal/broker` compiles.
- [ ] Step 13: Create `internal/broker/accounts.go` (M1, M3, M6, M7, M10, M11):
  - `Register(ctx, name, claimCode)`, with the admission order and the claim and recovery paths; the claim path also applies `ValidName` and the case-insensitive name-taken check (M7 step 2); every refusal is the same `registration_refused`, with the real reason logged at INFO (M7);
  - `AccountByCredential(ctx, cred)`, giving the status, including retired outcomes;
  - `SessionAccount` for the `POST /v1/sessions` auth path, with a 403 code per status;
  - `ListAccounts`, `Approve` (re-checks the cap), `Reject` (→ `retired` `rejected`), `Remove` (refuses pending and hostmaster; `deleteSession(removed)` for each session; → `retired` `removed`), `SetMaxAccounts` (1..ceiling), `SetAdminSub` (returns the pending count and bumps the caller's session version, M11);
  - `ListAccounts`, `Approve` and `Reject` run `expirePending` first (M7);
  - `Remove` and the recovery path of `Register` also return the deleted session IDs (M8), for the handler to forget;
  - `expirePending(tx)` (→ `retired` `expired`), `pruneRetired(tx)`, `deleteAccount(tx, id, outcome)`;
  - enqueue `registration_pending` to `admin_sub` sessions on a new pending row.

  Done: compiles.
- [ ] Step 14: Modify `internal/broker/sweeper.go`: each sweep runs pending expiry and retired pruning in their own transactions after channel expiry; reaping calls `deleteSession(id, left)`. Done: compiles.
- [ ] Step 15: Create `internal/broker/server_accounts.go`:
  - handlers for `POST /v1/register`, `GET /v1/account` (credential bearer) and every `/v1/admin/*` route;
  - an `adminAuth` wrapper that resolves the session, then requires the hostmaster before decoding the body (M10);
  - the remove and register handlers call `tracker.Forget` for every session ID the store reports deleted (M8);
  - each successful admin mutation and subscribe/unsubscribe logs one INFO line with action, target name and acting name, never a credential or code (M8).

  Done: compiles.
- [ ] Step 16: Create `internal/broker/control.go` (M5):
  - `ListenControl(path, store, fingerprint, log)`: removes a stale socket (same probe rule as the wake socket), chmods 0600, and serves `RECOVER` and `FINGERPRINT` with a 5 s read deadline, one request per connection;
  - `ControlRequest(path, cmd)`, the client the CLI uses;
  - `RECOVER` does not log the code (it calls `Store.IssueClaim`);
  - a path longer than 100 bytes is a clear error (M5).

  Done: compiles.
- [ ] Step 17: Modify `internal/broker/server.go`:
  - register the new routes;
  - `createSession` authenticates the credential and returns `account`;
  - add `Config.KeyPath` and `Config.ControlPath`, defaulting to the directory of `DBPath`;
  - `Run` loads the identity, issues and logs the claim code (WARN) when there is no hostmaster, logs the fingerprint (INFO), starts the control socket and serves on a TLS listener built from `identity.ServerTLSConfig`;
  - the logging middleware still never logs bearer values, and labels the short hash `cred` instead of `session` on `POST /v1/sessions` and `GET /v1/account`.

  Done: `go vet ./internal/broker` compiles the non-test code.
- [ ] Step 18: Modify `internal/broker/store_test.go`:
  - the test helper creates an active account and then a session;
  - v1→v2 migration on a seeded v1 file (data dropped, version 2);
  - v2 reopen is a no-op;
  - removal reason semantics.

  Done: compiles with the other broker tests.
- [ ] Step 19: Modify `internal/broker/channels_test.go`: account fields on join, held delivery and known peers; `Send` to a removed channel → `peer_removed`; leave on a removed channel. Done: compiles.
- [ ] Step 20: Create `internal/broker/claim_test.go`: format and normalisation, single use, re-issue invalidates the previous code, wrong and missing codes indistinguishable, constant-time path exercised. Done: compiles.
- [ ] Step 21: Create `internal/broker/accounts_test.go`. Cases:
  - claim on an unclaimed broker;
  - two concurrent claims with the same code → exactly one hostmaster;
  - register without a code when unclaimed → `registration_refused`;
  - pending, then approve, reject and remove;
  - wrong-state refusals;
  - the hostmaster cannot be removed;
  - cap at registration and at approval;
  - cap lowered below the current count;
  - pending limit;
  - expiry (fake clock);
  - a bad name, a case-insensitively taken name, the pending limit and the active cap all return the identical `registration_refused` (same status and message), and each logs its real reason at INFO;
  - name reuse after reject, expire and remove;
  - retired status reporting;
  - recovery replaces the hostmaster (the old hostmaster's sessions are deleted and their peers on joined channels get `peer_removed`; the same name is allowed; the deleted session IDs are returned);
  - a claim with an invalid name, or with a name taken by another active or pending account, is refused with `registration_refused`;
  - `registration_pending` is delivered only to subscribed sessions;
  - `SetAdminSub` bumps the session version;
  - approving a pending account past `PendingExpiry` before any sweep → `no_such_account`, and it is retired as `expired`;
  - `Remove` returns the removed account's session IDs;
  - `SetMaxAccounts` bounds.

  Done: compiles.
- [ ] Step 22: Modify `internal/broker/sweeper_test.go`: pending expiry writes `retired` `expired`, pruning after `RetiredKeep`, reaping still emits `peer_left` (not `peer_removed`). Done: compiles.
- [ ] Step 23: Modify `internal/broker/server_test.go`:
  - the harness registers accounts through the store and uses credentials;
  - `POST /v1/sessions` without or with a bad credential → 401;
  - pending → 403 `account_pending`;
  - the logging middleware does not log credentials.

  Done: compiles.
- [ ] Step 24: Create `internal/broker/server_accounts_test.go`:
  - a non-hostmaster gets 403 `forbidden` on each admin route, before body validation;
  - the hostmaster flows over HTTP;
  - after `remove`, the removed account's session IDs are forgotten by the tracker (a `Touch` after it does not re-add them);
  - admin mutations log one INFO line with the action and names, and no credential;
  - `GET /v1/account` for each status;
  - status codes match the API Contract.

  Done: compiles.
- [ ] Step 25: Create `internal/broker/control_test.go`: `FINGERPRINT` reply; `RECOVER` when unclaimed (mode `claim`) and when claimed (mode `recover`); the issued code works once through `Register`; unknown command; socket mode 0600; stale socket replaced. Done: **Checkpoint A** — `go test -race ./internal/broker` passes.

### Layer: Client
- [ ] Step 26: Create `internal/client/tls.go` (M13):
  - `pinnedTransport(pin)`: `InsecureSkipVerify` plus `VerifyConnection` comparing SPKI fingerprints, TLS 1.3 minimum;
  - `PinError{Expected, Got}`, `ErrNotPinned`;
  - `Probe(ctx, url) (fingerprint, error)`.

  Done: `go build ./internal/client`.
- [ ] Step 27: Modify `internal/client/client.go`:
  - `Config{BaseURL, Pin, Credential}` and `New(Config) (*Client, error)`, which refuses non-https URLs and an empty pin;
  - `CreateSession` sends the credential and returns the token and account;
  - `PinError` passes through `do` unwrapped (`errors.As`);
  - `APIError.Is` is unchanged.

  Done: `go build ./internal/client`.
- [ ] Step 28: Create `internal/client/accounts.go`: `Register`, `AccountStatus` (credential bearer), `ListAccounts`, `Approve`, `Reject`, `Remove`, `SetMaxAccounts`, `SetAdminSubscription`. Done: `go build ./internal/client`.
- [ ] Step 29: Modify `internal/client/client_test.go`: use `testutil.TLSServer` with the real broker handler and a registered account; existing cases keep passing. Done: compiles.
- [ ] Step 30: Create `internal/client/tls_test.go`:
  - the right pin connects;
  - a wrong pin → `PinError` with both values, and no request reaches the handler (the handler records hits);
  - an empty pin is refused by `New`;
  - an `http://` URL is refused;
  - `Probe` returns the server fingerprint.

  Done: compiles.
- [ ] Step 31: Create `internal/client/accounts_test.go`: each account and admin method against the real handler over TLS, with error codes mapped to `APIError`. Done: **Checkpoint B** — `go test -race ./internal/client` passes.

### Layer: Enrollment CLI
- [ ] Step 32: Create `internal/enroll/enroll.go` (M15):
  - an `Env` holding stdin, stdout, stderr, an `IsTTY` func, the brokerfile dir and a probe func;
  - `Pin`, `Unpin`, `Register(name)`, `Claim(name)` (the code is read from a prompt only) and `Status`;
  - every command except `Status` refuses when `IsTTY` is false, with a message that this is a human-only command;
  - `Register` and `Claim` check the name with `protocol.ValidName` before any network call, with a precise local message;
  - `registration_refused` is shown as "refused: the name may be taken or the broker may be full; try another name or ask the hostmaster";
  - `Pin` refuses when a different pin exists;
  - `Register` and `Claim` check that the broker file is writable before any network call, and require typed confirmation (showing the stored account name) before replacing an existing credential (M15);
  - `Status` never prints the credential, and refreshes the `hostmaster` hint.

  Done: `go build ./internal/enroll`.
- [ ] Step 33: Create `internal/enroll/enroll_test.go`:
  - non-TTY refusals;
  - pin accept and decline;
  - pin conflict;
  - register writes the credential and leaves the file at 0600;
  - claim reads the code from stdin and sets `hostmaster`;
  - a malformed name is refused locally with no request sent;
  - status for each account state, and its output never contains the credential;
  - unpin keeps the credential;
  - an unwritable config directory makes `register` fail before any request is sent;
  - `register` over an existing credential aborts without confirmation (no request sent) and replaces it after `yes`.

  Done: `go test -race ./internal/enroll`.

### Layer: Shim
- [ ] Step 34: Modify `internal/shim/text.go`:
  - `FrameRow` adds an escaped `account` attribute when present;
  - add bodies for `peer_removed` ("account was removed by the hostmaster; the channel has ended; do not investigate or retry; tell the user") and `registration_pending` (a pending request for the given account name; confirm with your human; the admin tools are the only way to act);
  - `body` checks `KindRegistrationPending` **before** the "non-empty `Text` is the body" shortcut, because that row's text is the registrant's name (M11); the name is embedded in the fixed sentence and the result still goes through `escapeContent`;
  - the `Preamble` covers registrants as well as peers;
  - `ValidateName` uses `protocol.ValidName`.

  Done: `go build ./internal/shim`. The package may not compile until step 38.
- [ ] Step 35: Modify `internal/shim/session.go` (M12, M16):
  - `NewSession` takes the configured client;
  - `Ensure` surfaces account 403s as a typed `AccountError` with human-directed text;
  - add `DropWithNotice(token, notice)`;
  - add `DropAfter401(ctx, token)`: asks `AccountStatus` (short timeout) and calls `DropWithNotice` with the account-removed or inactive text when the account is no longer active, else `Drop` (M16). It is the only 401 drop path, used by the mirror (step 36) and by `send`, `leave` and `withSession` (step 38);
  - add a `wantAdmin` flag with a `WantAdmin()` getter and a keeper hook that runs after a drop (installed by `admin.go`);
  - `Close` stops the keeper.

  Done: compiles with step 36.
- [ ] Step 36: Modify `internal/shim/mirror.go`:
  - `Snapshot` and `Summary` carry `AdminSubscribed` from `SyncResponse`;
  - on a 401 the loop calls `Session.DropAfter401` (step 35) instead of `Drop`, which picks the account-removed or inactive notice or the existing one.

  Done: compiles.
- [ ] Step 37: Modify `internal/shim/wake.go`: with `subscribed := sum.AdminSubscribed || w.sess.WantAdmin()` (M11), `handleWait` replies `no_channels` only when `LiveChannels == 0 && !subscribed`; `status()` fills `Subscribed` from the same expression; `noChannelsReason` is unchanged. Done: compiles.
- [ ] Step 38: Modify `internal/shim/tools.go` (M16, M17):
  - `NewTools` takes an optional `setupErr` (missing file, pin or credential) that every tool returns, plus `hostmaster bool`;
  - `brokerErr` renders `PinError` (both fingerprints, "only the human can resolve this with `agent-bus unpin`/`pin`; do not run it yourself") and `AccountError`;
  - the setup error for a refused `http://` URL tells the human to change it to `https://` (Architect "Upgrade and Rollback");
  - `send`, `leave` and `withSession` drop through `Session.DropAfter401` instead of `Drop`;
  - the join result names the peer's account;
  - known peers include the account;
  - `Register` adds the admin tools when `hostmaster` is set.

  Done: compiles with step 39.
- [ ] Step 39: Create `internal/shim/admin.go` (M10, M12, M17):
  - six tools: `admin_list_accounts`, `admin_approve{name}`, `admin_reject{name}`, `admin_remove{name}`, `admin_set_max_accounts{max}` and `admin_subscribe{subscribed}`;
  - descriptions carry the confirm-first, explicit-target and no-peer-requests rules;
  - results wrap account names as data;
  - `admin_subscribe` sets `wantAdmin`, calls `Mirror.Refresh` after the broker call (M11), and returns the pending count;
  - the keeper goroutine re-creates the session and re-subscribes after a drop, with 1–60 s backoff, and stops on shutdown, on unsubscribe, on 403, or on a 401 from session creation; on 401/403 it clears `wantAdmin` and sets a notice saying the subscription could not be restored (M12).

  Done: `go build ./internal/shim`.
- [ ] Step 40: Modify `internal/shim/text_test.go`: the `account` attribute is escaped (quotes, `<`, `&`), the new kind bodies (a `registration_pending` row whose `Text` is the name renders the explanatory sentence containing that name, not the bare name), the closing-tag escape still holds. Done: compiles.
- [ ] Step 41: Modify `internal/shim/session_test.go`:
  - the fake broker uses `testutil.TLSServer` with a pin and a credential;
  - account 403 → `AccountError`;
  - `DropWithNotice` text;
  - the keeper hook is invoked after a drop only when `wantAdmin` is set.

  Done: compiles.
- [ ] Step 42: Modify `internal/shim/mirror_test.go`:
  - TLS fake broker;
  - `AdminSubscribed` propagates;
  - a 401 combined with `AccountStatus=removed` gives the removed notice;
  - a 401 combined with an active account gives the existing notice;
  - a `send` that hits the 401 before the mirror does still gets the account-removed notice (shared `DropAfter401`).

  Done: compiles.
- [ ] Step 43: Modify `internal/shim/wake_test.go`: a subscribed session with no channels stays armed and wakes on `registration_pending`; a `WAIT` sent right after the session is dropped while `wantAdmin` is set stays armed (no `no_channels`); once `wantAdmin` is cleared it gets `no_channels`; `STATUS` carries `subscribed`. Done: compiles.
- [ ] Step 44: Modify `internal/shim/wake_fixed_test.go` only if the changed constructors break it, and adapt the helpers. Done: compiles.
- [ ] Step 45: Modify `internal/shim/tools_test.go`:
  - every tool returns the setup error when unconfigured;
  - the pin-mismatch text names both fingerprints and forbids self-repair;
  - the join result includes the account;
  - admin tools are absent without the hostmaster hint and present with it.

  Done: compiles.
- [ ] Step 46: Create `internal/shim/admin_test.go`:
  - each admin tool against a real broker handler over TLS;
  - a non-hostmaster credential with the hint set gets `forbidden` rendered;
  - empty or wildcard names are rejected before any broker call;
  - `admin_subscribe` followed at once by a `WAIT` on the wake socket stays armed (no `no_channels`);
  - the keeper re-subscribes after a forced drop and stops on unsubscribe, shutdown, 403 and a 401 from session creation, clearing `wantAdmin` on 401/403, with no hot loop (counted calls).

  Done: **Checkpoint C** — `go test -race ./internal/shim` passes.

### Layer: Stop hook
- [ ] Step 47: Modify `internal/hook/hook.go`: the unarmed-wake reminder also fires when `st.Subscribed` is true and there are no live channels. Done: `go build ./internal/hook`.
- [ ] Step 48: Modify `internal/hook/hook_test.go`: a subscribed, unarmed session gets the reminder; a subscribed, armed one does not. Done: `go test -race ./internal/hook`.

### Layer: CLI wiring
- [ ] Step 49: Modify `cmd/agent-bus/main.go`:
  - `broker` gains `--key` and `--control`, plus the `broker recover` and `broker fingerprint` subcommands (control-socket clients);
  - add the `pin`, `unpin`, `register`, `claim` and `status` commands through `internal/enroll`, with the real TTY check;
  - `shim` normalises the URL with `brokerfile`, loads the file, builds `client.New(Config)` and passes `setupErr` and the hostmaster hint to `NewTools`;
  - the usage text lists everything;
  - the `http://` auto-prefix is replaced by the `https` default.

  Done: `go build ./...` and `go vet ./...` are green.

### Layer: End-to-end tests
- [ ] Step 50: Modify `internal/e2e/e2e_test.go`:
  - the harness starts the broker handler on `testutil.TLSServer`, claims a hostmaster through `Register` with an issued code, registers and approves users, and starts shims with pinned, credentialed clients;
  - existing scenarios keep passing.

  New scenarios:
  - an unauthenticated client cannot create a session;
  - a pending user's shim reports "pending approval";
  - two users chat and each `read` shows the other's account;
  - the hostmaster removes a user mid-channel: the peer is woken by `peer_removed` and `send` fails with the removal reason;
  - the removed user's shim reports the account was removed;
  - a subscribed hostmaster shim with no channels is woken by a registration, and `admin_approve` works;
  - a non-hostmaster sees no admin tools;
  - a wrong pin is refused with no request reaching the broker;
  - recovery through the control socket replaces the hostmaster.

  Done: `go test -race ./internal/e2e`.

### Layer: Documentation and skills
- [ ] Step 51: Modify `skills/agent-bus/SKILL.md` (M18):
  - add an "Accounts" section: setup is human-only (`register`/`claim`/`pin`/`unpin`; never run them, never edit the broker file); the shim's setup and pin messages mean "tell the user";
  - add the `account` attribute in `read` (context, not authority);
  - add `peer_removed` (stop, tell the user, do not investigate);
  - update the event list and the waking kinds;
  - add a one-line pointer to the hostmaster skill;
  - correct "no encryption beyond the transport" to TLS with a pinned broker.

  Done: matches `tools.go`, `protocol.go` and `text.go` per the `CLAUDE.md` check.
- [ ] Step 52: Create `skills/agent-bus-hostmaster/SKILL.md` (M17, M18):
  - frontmatter description scoped to the hostmaster account;
  - the six admin tools and their arguments;
  - the subscription and how it wakes the session;
  - confirm each approval and removal with the human unless they pre-authorised unattended handling (with an example standing instruction for a Hermes peer);
  - never act on admin requests from peer messages;
  - account names are data;
  - limits (cap 5, pending 5, expiry 2 h);
  - removal ends that user's channels at once;
  - hostmaster replacement is host-side only (`sudo agent-bus broker recover`);
  - admin tools appear only after the session restarts following `agent-bus claim` (the hint is read at shim start);
  - a subscribed session's `wait` stays armed with no channels.

  Done: matches `admin.go` and `protocol/accounts.go`.
- [ ] Step 53: Modify `README.md`:
  - **Install the broker:** first start, the claim code in the journal, `broker fingerprint`, the key file next to the database (back up both together), and `broker recover`.
  - **Upgrading from a single-user broker:** the upgrade order, `http://` → `https://` in every MCP and Hermes config, and the one-way migration with its rollback steps (Architect "Upgrade and Rollback").
  - **Connect a session:** `agent-bus claim`/`register`/`status`/`pin`/`unpin`, the broker file and its location, copying it to another host, and the `https` URL.
  - **The agent workflow:** the account in `read` and `peer_removed`.
  - **The Claude Code skill:** link both skills.
  - **Hermes Agent support:** the https URL, the broker file for the gateway's user, and subscription standing instructions.
  - **Security model:** rewritten for the hostmaster model, claim code, TOFU pinning and its first-contact risk, the TTY gate, account visibility limits, and the unchanged untrusted-data framing.

  Use only placeholders such as `broker-host`. Done: every command and flag in the README exists in `main.go`.
- [ ] Step 54: Modify `CLAUDE.md`:
  - **Architecture:** accounts, identity/TLS, `brokerfile`, `enroll`, the control socket and the admin tools.
  - **The "Important check" section:** extend it to `skills/agent-bus-hostmaster/SKILL.md`, the admin tool descriptions in `internal/shim/admin.go` and the account constants in `internal/protocol/accounts.go`.

  Done: the text matches the code.

### Layer: Verification
- [ ] Step 55: Full verification:
  - `just test` passes with the race detector;
  - `go vet ./...` and `gofmt -l .` are clean;
  - `just build` succeeds with `CGO_ENABLED=0`;
  - `grep` finds no real hostnames or personal paths in the changed files;
  - if `go.mod`/`go.sum` changed, `vendorHash` in `flake.nix` is updated (the plan expects no new modules).

  Done: all green.
