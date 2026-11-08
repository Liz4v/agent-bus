# Tasks: Name addressing (drop the channel ID)

> Phase 2 output — implementation checklist.
> Each step is atomic: one file created, one migration, one component.
> Dependencies and parallelisation opportunities are documented in Architect.md.
> Design references (N1–N17) point to Architect.md.
> The tree will not compile between some steps inside a track; each checkpoint says when it must be green again.

### Layer: Protocol
- [ ] Step 1: Modify `internal/protocol/protocol.go` (N9, N15):
  - delete `ChannelTTL`; the codes `bad_label`, `no_open_channel`, `same_label`, `duplicate_peer` and `channel_expired`; the kinds `peer_joined` and `channel_expired`; every `State*`; `LabelPattern`/`ValidLabel`; `AdminHandle`; `KnownPeer`; `ErrorDetail.KnownPeers`; `Channel`; `CreateChannel*`, `JoinChannel*` and `LeaveRequest`;
  - add the codes `bad_name`, `name_taken`, `name_held`, `no_name`, `self_send` and `upgrade_required`;
  - add `ClaimNameRequest{Name}`, `ClaimNameResponse{Name}` and `ReleaseNameResponse{Released}`;
  - change `Message`: no `Handle`; `From *string` with JSON `from`; `FirstContact bool` with JSON `first_contact,omitempty`;
  - change `SyncResponse`: no `Channels`; add `Name string` with JSON `name`;
  - change `StatusReply`: `Named bool` with JSON `named` replaces `LiveChannels`;
  - rename `WaitNoChannels` → `WaitNoName = "no_name"`;
  - `IsWaking`: `text`, `peer_left`, `peer_removed`, `registration_pending`.

  Done: `go build ./internal/protocol`.
- [ ] Step 2: Modify `internal/protocol/accounts.go` (N2): add `NameKey(s string) string` (`strings.ToLower`) and `ReservedName(s string) bool` (true for the keys `agent-bus` and `admin`). Update the `ValidName` doc comment. Done: `go build ./internal/protocol`.
- [ ] Step 3: Modify `internal/protocol/protocol_test.go`:
  - an `IsWaking` table covering every kind (`peer_disconnected` and `peer_reconnected` false);
  - `NameKey` (mixed case);
  - `ReservedName` (`admin`, `Admin`, `AGENT-BUS` true; `admins`, `bob` false);
  - delete the `ValidLabel` tests.

  Done: `go test -race ./internal/protocol`.

### Layer: Broker store
- [ ] Step 4: Modify `internal/broker/store.go` (N1, N4, N9, N11):
  - keep `schemaV2` as history and add `schemaV3` exactly as in *Data Model Changes*;
  - make `migrate` stepwise (`<2` → V2, `<3` → V3, then `user_version = 3`);
  - `Error`: drop `KnownPeers`;
  - error vars: drop `ErrNoOpen`, `ErrBadLabel`, `ErrSameLabel`, `ErrDuplicate` and `ErrExpired`; add `ErrBadName` (400), `ErrReservedName` (400 `bad_name`, reserved wording), `ErrNameTaken` (409), `ErrNoName` (409, the N7 wording including the upgrade tail for old shims), `ErrSelfSend` (400); `errNameHeld(name)` (409); reword `ErrPeerLeft` and `ErrPeerRemoved` without "channel" (N5);
  - `enqueue(session, from, fromAccount *string, kind, text string, firstContact bool)`;
  - delete `member`, `removeReason`'s channel wording, `removeMember`, `sessionMembers` and `channelList`, keeping the `reasonLeft`/`reasonRemoved` enum;
  - `deleteSession`: `tx.depart(session, reason)`, then `DELETE FROM sessions`, then `wakeOnly`;
  - `Sync`: select `from_name`, `first_contact`; fill `resp.Name` from `sessions.name` (`""` if NULL); no channel list.

  Done: compiles once Step 6 lands.
- [ ] Step 5: Modify `internal/secret/secret.go` and `internal/secret/secret_test.go`: delete `NewChannelID` and its test, and update the package comment. Done: `go test -race ./internal/secret`.
- [ ] Step 6: Create `internal/broker/names.go` and delete `internal/broker/channels.go` (N3–N8):
  - `sessionAccount` (moved), `sessionName(session) (name, key string, ok bool, err)` and `quote` (moved);
  - `(s *Store) ClaimName(ctx, session, name) (string, error)`, rules N8 in order; delete `departed` rows for the key; INFO log `name claimed` with session, account and name;
  - `(s *Store) ReleaseName(ctx, session) (string, error)`: `depart(reasonLeft)`, clear `name`/`name_key`, `DELETE FROM inbox WHERE session_id = ? AND kind <> 'registration_pending'`, bump, INFO log; returns the released name or `""`;
  - `(s *Store) Send(ctx, session, to, text) error`, rules N7 in order; `INSERT OR IGNORE INTO partners(lo, hi)` with `lo = min(caller, recipient)` and `hi = max(...)`, **before** the `enqueue`, and pass `firstContact = rowsAffected == 1` to `enqueue`;
  - `(tx *Tx) depart(session, reason)`, per N4: partners in both directions, ordered by session ID; enqueue `peer_left`/`peer_removed` with from = the departing name and account; `INSERT OR REPLACE INTO departed`; bump each partner; then `DELETE FROM partners WHERE lo = ? OR hi = ?`. It does nothing when the session holds no name.

  Done: `go build ./internal/broker` once Steps 7–9 land.
- [ ] Step 7: Modify `internal/broker/sweeper.go` (N6):
  - `setDisconnected` targets = partners of `id` (from = the session's name and account; skip if unnamed);
  - delete `expireChannels` (both methods) and its call in `Sweep`;
  - update the `Sweep` doc comment.

  Done: `go build ./internal/broker` once Step 9 lands (`server.go` still calls the deleted channel methods until then).
- [ ] Step 8: Modify `internal/broker/accounts.go` (N17): the `registration_pending` `enqueue` call uses the new signature (no handle, `firstContact=false`). Update the `deleteAccount` comment ("partners get peer_removed"). Done: `go build ./internal/broker` once Step 9 lands.
- [ ] Step 9: Modify `internal/broker/server.go` (API Contract, N12):
  - register `POST /v1/name` → `claimName` (200 `ClaimNameResponse`) and `DELETE /v1/name` → `releaseName` (200 `ReleaseNameResponse`);
  - register `POST /v1/channels`, `POST /v1/channels/join` and `POST /v1/leave` → one unauthenticated `gone` handler returning 410 `upgrade_required` with the N12 text;
  - delete `createChannel`, `joinChannel` and `leave`;
  - `writeErr` without `KnownPeers`;
  - logging comment: drop "channel IDs".

  Done: `go build ./internal/broker` (not `go vet` yet: vet type-checks the test files, which use channel symbols until Steps 10–14; vet runs at Checkpoint A).

### Layer: Broker tests
- [ ] Step 10: Create `internal/broker/names_test.go` and delete `internal/broker/channels_test.go`. Cases:
  - claim succeeds; same name again is a no-op (also with different case: holding `bob`, claiming `Bob` returns 200 with `bob`); a different name gives `name_held` with the held name; `Bob` vs `bob` gives `name_taken`; `admin`/`Agent-Bus` give the reserved `bad_name`; invalid gives `bad_name`; concurrent claims → exactly one winner (goroutines on one store);
  - send without a name gives `no_name`; to self (case-insensitive) gives `self_send`; to an unheld name gives `unknown_peer` and stores nothing; a delivered row carries from, account and `first_contact` true on the first delivery only (and not on the reply);
  - release: partners get a waking `peer_left` row; the releaser's unread peer rows are gone but `registration_pending` stays; the releaser can claim again;
  - after a release, a former partner's send gives `peer_left`, an unrelated session's send gives `unknown_peer`, and after a third session claims the name, the former partner's send reaches the new holder with `first_contact` true;
  - a new holder is not a partner of the old holder's partners (no presence events reach them);
  - sync returns `name`.

  Done: verified at Checkpoint A (the package's other test files must compile first); after Step 14, `go test -race -run 'Name|Send|Release|Claim' ./internal/broker`.
- [ ] Step 11: Modify `internal/broker/store_test.go`:
  - migration tests: a v2 database with accounts, settings, retired, sessions and channels opens as v3 with accounts, settings and retired intact and no sessions; a fresh database is v3; reopening a v3 database is idempotent;
  - `DeleteSelf` notifies partners with `peer_left`;
  - rewrite the channel fixtures to claim and send.

  Done: at Checkpoint A, `go test -race ./internal/broker -run Store`.
- [ ] Step 12: Modify `internal/broker/sweeper_test.go`:
  - disconnect and reconnect emit non-waking events to partners only (not to unrelated named sessions);
  - a reap after the grace period frees the name (another session can claim it) and gives partners `peer_left` plus a `departed` row;
  - delete the channel-expiry tests.

  Done: at Checkpoint A, `go test -race ./internal/broker -run Sweep`.
- [ ] Step 13: Modify `internal/broker/server_test.go`:
  - HTTP round trips for `POST`/`DELETE /v1/name` (status codes and error codes per the API Contract);
  - `POST /v1/messages` error mapping;
  - the three removed routes return 410 `upgrade_required` without auth;
  - the sync JSON has `name` and messages have `from`/`first_contact` and no `handle`;
  - delete the channel route tests.

  Done: at Checkpoint A, `go test -race ./internal/broker -run Server`.
- [ ] Step 14: Modify `internal/broker/accounts_test.go` and `internal/broker/notify_test.go`:
  - removing an account gives each partner a waking `peer_removed`, frees the names, and a former partner's send gets `peer_removed`, an unrelated sender's gets `unknown_peer`;
  - the `registration_pending` row has no handle;
  - fix the fixtures in `notify_test.go` if it uses channels.

  **Checkpoint A:** `go test -race ./internal/protocol ./internal/secret ./internal/broker` is green, and `go vet` is clean on those packages.

### Layer: Client
- [ ] Step 15: Modify `internal/client/client.go`:
  - add `ClaimName(ctx, token, name) (protocol.ClaimNameResponse, error)` and `ReleaseName(ctx, token) (protocol.ReleaseNameResponse, error)`;
  - delete `CreateChannel`, `JoinChannel` and `Leave`;
  - `APIError` loses `KnownPeers` (also in `do`).

  Done: `go build ./internal/client`.
- [ ] Step 16: Modify `internal/client/client_test.go`:
  - `ClaimName`/`ReleaseName` against a test broker;
  - a plain-text 404 from an unknown route yields `APIError{Code: "http_404"}` (fixture for N12);
  - delete the channel tests.

  Done: `go test -race ./internal/client`.

### Layer: Shim
- [ ] Step 17: Modify `internal/shim/text.go` (N2, N14):
  - delete `BuildLabel`;
  - `ValidateName(arg, name string) error` names the argument in its messages and rejects reserved names;
  - `SenderOf` uses `m.From`;
  - `FrameRow` without `handle`; it adds `first-contact="true"` and the fixed first-contact sentence for first-contact text rows;
  - `body` per N14 (delete the `peer_joined` and `channel_expired` cases);
  - the `Preamble` is unchanged.

  Done: `go build ./internal/shim` once Steps 19–28 land.
- [ ] Step 18: Modify `internal/shim/text_test.go`:
  - framing without handle;
  - the first-contact attribute and sentence present only when `FirstContact` is set and kind is `text`;
  - a peer cannot fake the sentence or attribute via text or name (escaping);
  - reserved and invalid names in `ValidateName`;
  - the new event bodies;
  - delete the `BuildLabel` tests.
- [ ] Step 19: Modify `internal/shim/mirror.go` (N15):
  - `Snapshot.Name`, `Summary.Named` (`Name != ""`), `DropReport.Name`;
  - `applyLocked` stores `resp.Name`;
  - `resetLocked` clears it;
  - add `DiscardPeerRows()`: under the lock, drop every row whose kind is not `registration_pending`, keep `cursor` and `after`, advance a discard epoch, kick the loop and call `notifyLocked` (N4, Critic R1 B1);
  - `loop` records the discard epoch with each request; if the epoch changed by the time the response arrives, it drops the response like an interrupted one (nothing applied, waiters kept, retry at once with the same `after`) (N4, Critic R2 W1);
  - `drop` reports `HadState` as `Name != "" || len(rows) > 0`;
  - delete the channel fields and the `LiveChannels` counting.
- [ ] Step 20: Modify `internal/shim/mirror_test.go`: replace the channel-state fixtures with `name` in the sync responses; assert `Summary.Named` and `DropReport.Name`; `DiscardPeerRows` drops `text`/`peer_*` rows, keeps `registration_pending`, leaves the cursor alone, and a later sync does not bring the dropped rows back; a sync response in flight during `DiscardPeerRows` (use the test broker's `delay` hook to hold it) applies neither its rows nor its stale `name`.
- [ ] Step 21: Modify `internal/shim/session.go` and `internal/shim/session_test.go` (N15): `accountGoneNotice` and `dropNotice` wording (the name is released; claim it again with `claim_name`; partners were told); the credential-unrecognised notice in `session.go` ("…so your channels are gone…", currently line 195) → "your name was released"; the `DropReport.HadState` comment ("held a name or unread rows"); the tests assert the new texts, including the name. In the shared `testBroker` harness in `session_test.go`, rename `failChannels` to `failName` and point it at `/v1/name`; rewrite the `CreateChannel`/`JoinChannel` fixtures (`TestCloseMakesPeerReadPeerLeftImmediately`, `TestDropWithNoticeAppends` and the others) to claim and send, so a partnership exists where a `peer_left` is expected, and assert `r.Name` instead of `r.Channels[0].State` (Critic R2 W3).
- [ ] Step 22: Modify `internal/shim/wake.go` (N15):
  - `status()` sets `Named`;
  - `handleWait` waits while `sum.Named || w.subscribed(sum)` and otherwise replies `WaitNoName`;
  - `noChannelsReason` → `noNameReason` with the N15 texts;
  - update the comments that mention channels.
- [ ] Step 23: Modify `internal/shim/wake_test.go` and `internal/shim/wake_fixed_test.go`:
  - a wait with no name → `no_name` at once, with the reason;
  - with a name it stays armed until a waking row arrives;
  - release while armed → `no_name`;
  - a subscribed hostmaster with no name stays armed;
  - STATUS JSON has `named`.
- [ ] Step 24: Modify `internal/shim/tools.go` (N12, N13):
  - `NewTools` drops `hostname`;
  - the tool set and descriptions per N13. Descriptions:
    - `claim_name`: "Claim a broker-wide name for this session so other sessions can message you by it. Any approved account that knows the name can send to you. A session holds one name; release_name frees it.";
    - `send`: "Send a text message to another session, addressed by its name.";
    - `release_name`: "Release this session's name. Conversation partners are told; unread peer messages are discarded.";
    - `read`/`wake_command`: wording kept;
  - arg structs `claimArgs{Name}` and `sendArgs{Peer, Text}` (peer = "the other session's name, e.g. bob"); delete `createArgs`, `joinArgs` and `leaveArgs`;
  - `noChannels` → `noName`;
  - `brokerErr`: delete the `KnownPeers` and "You have no channels" branches; add the N12 mapping (`http_404`/`http_405` → "broker must be upgraded" text); render code `no_name` as the `noName` text (N13, Critic R2 N1);
  - `withSession` comment "(claim only)";
  - `release_name`: after a successful `DELETE /v1/name`, call `t.m.DiscardPeerRows()` before `Refresh` (N4); a 401 drops the session like `send` does;
  - the `Tools` doc comment ("six MCP tools" → five).

  Done: `go build ./internal/shim` once Step 26 lands.
- [ ] Step 25: Modify `internal/shim/tools_test.go`:
  - the tool list is exactly the 5 (plus admin when hostmaster);
  - `claim_name` registers lazily and its result text names the session;
  - claim refusals render the broker messages;
  - send before claim → "claim_name first" with no broker call;
  - send errors (`unknown_peer`, `peer_left`, `peer_removed`, `self_send`) render; a send after `release_name` (token kept, no name) gets the `noName` text, without the old-shim upgrade tail;
  - `TestCreateFailureAfterRegistrationReusesSession` becomes the claim equivalent, using the harness's `failName` (Step 21);
  - `release_name` with and without a name; after a release, `read` shows no earlier peer rows (they were in the mirror before the release) and a connected `WAIT` answers `no_name`;
  - an old-broker 404 on `claim_name` → the upgrade text;
  - read renders `from`/`account`/first-contact;
  - delete the channel tests.
- [ ] Step 26: Modify `internal/shim/admin.go`: the `admin_subscribe` description and result text ("even when you hold no name"); the keeper comment (`no_name`). No logic change.
- [ ] Step 27: Modify `internal/shim/admin_test.go`: fixtures and expected texts for Step 26; a subscribed session with no name is still woken by `registration_pending`.
- [ ] Step 28: Modify `cmd/agent-bus/main.go`: the `shim.NewTools` call without `os.Hostname`; any help or usage text that mentions channels or channel IDs.

  **Checkpoint B** (needs Checkpoint A; `session_test.go` imports the broker): `go build ./internal/client ./internal/shim`, `go vet ./internal/client ./internal/shim` and `go test -race ./internal/client ./internal/shim` are green. `cmd/agent-bus` imports `wait`, `hook` and `hermeswake`, so it cannot build until Steps 29–34 (Critic R2 W2). A repo-wide `go vet ./...` waits for Step 35 (the e2e tests still use removed symbols).

### Layer: Wake consumers
- [ ] Step 29: Modify `internal/wait/wait.go` (N15, N16): `ExitNoChannels` → `ExitNothing` (value 1); `WaitNoName`; `FilterLabels` → `FilterNames`, using `protocol.ValidName` plus the system sender; the "shim went away" texts are unchanged.
- [ ] Step 30: Modify `internal/wait/wait_test.go`: the `no_name` reply prints the reason and exits 1; the filter drops values that fail `ValidName` (for example `a/b`, `<x>`).
- [ ] Step 31: Modify `internal/hook/hook.go` (N16): the reminder condition uses `st.Named`; `labels` uses `ValidName`.
- [ ] Step 32: Modify `internal/hook/hook_test.go`: named and unarmed → reminder; unnamed and unsubscribed → silent; a status JSON without `named` (old shim) → silent; filter cases.
- [ ] Step 33: Modify `internal/hermeswake/run.go` and `internal/hermeswake/post.go` (N16): `NoChannelsSleep` → `NoNameSleep` and `DefaultNoChannelsSleep` → `DefaultNoNameSleep` (default unchanged, 10 s); `WaitNoName`; log text; the label filter in `post.go` uses `ValidName` (via `wait.FilterNames` if it already shares it).
- [ ] Step 34: Modify `internal/hermeswake/run_test.go`, `internal/hermeswake/sock_test.go` and `internal/hermeswake/post_test.go`: the renamed config field and status; `no_name` → sleeps `NoNameSleep`; the filter.

  **Checkpoint C:** `go build ./...` and `go vet ./cmd/... ./internal/wait ./internal/hook ./internal/hermeswake` are clean, and `go test -race ./internal/wait ./internal/hook ./internal/hermeswake` is green.

### Layer: End to end
- [ ] Step 35: Rewrite the channel scenarios in `internal/e2e/e2e_test.go`:
  - **`TestEndToEnd`:** A claims `alice` and B claims `bob`. A sends to `bob` before B arms its wake. B's wait fires with `alice`, and its read shows `account=` and first-contact. B replies (no first-contact on A's side). Carol claiming `bob` gets `name_taken`. B releases: A is woken with `peer_left`, A's send gets the `peer_left` error, carol claims `bob`, and A's send reaches carol with first-contact (A and carol are now partners). A's clean shutdown gives carol a waking `peer_left` naming `alice`; a fourth named session that never exchanged a message with A gets nothing. *(Critic R1 B2: the earlier text said carol gets nothing, which contradicts N3/N4.)*
  - **`TestHermesWakeEndToEnd`:** the first wake is a text, not `peer_joined`.
  - **`TestAccountsUnauthenticatedPendingAndPinned`:** use `claim_name` in place of `create_channel`.
  - **`TestTwoUsersSeeEachOthersAccount…`:** claim and send.
  - **`TestHostmasterRemovesAUserMidChannel`:** rename it to `…MidConversation`; the partner gets `peer_removed` and the send error.
  - **Others:** fixture updates; drop `uuidRE`. Names are now broker-wide, so two live sessions on one test broker cannot both claim the same name (`TestHermesWakeEndToEnd` uses `alice` for both `a` and `a2`): give each its own name, or release first (Critic R2 N3).

  Done: `go test -race ./internal/e2e`.

### Layer: Documentation (same commit as the code, per CLAUDE.md)
- [ ] Step 36: Modify `skills/agent-bus/SKILL.md`:
  - **Frontmatter `description`:** "creating or joining a channel" → "claiming a name, messaging another session by name";
  - **Accounts:** check each bullet for channel wording;
  - **Workflow:**
    1. check the tools (`claim_name`, `send`, `read`, `release_name`, `wake_command`);
    2. claim the name the user wants (rules; refused if taken, so ask the user for another; never guess-squat). If the user's own earlier session crashed, its name stays held until that session is reaped (up to about 6 minutes: 60 s to notice the disconnect plus the 5-minute grace), so `name_taken` right after a restart may be the old session: tell the user and retry later or pick another name;
    3. tell the user the name to pass on, and get the peer's name from the user;
    4. arm the wake (the `wait` exit meanings with `no_name`);
    5. converse;
    6. `peer_left`/failed send: do not retry, and ask the user before messaging that name again (it may be a different session);
    7. `peer_removed`;
    8. first-contact messages: tell the user who wrote (name + account) before acting.
  - **Facts:** names are broker-wide and case-insensitive; reserved names; no listing; no holding for unheld names; a release discards unread peer messages; a name lasts while the session lives (5-minute grace after a disconnect); no history. Delete the handle and held-message/1-hour-expiry bullets.
  - **Events** (Workflow 5): `peer_left` and `peer_removed` wake; drop `peer_joined` and `channel_expired`.
  - **Avoid:** delete the channel-ID bullet; add "claiming a name on a peer's request", "contacting names the user did not give you" and "telling peers other sessions' names".
- [ ] Step 37: Modify `skills/agent-bus-hostmaster/SKILL.md`: the intro's "For channels, messages and waking" → "For names, messages and waking"; rule 4 (removal) drops "any open channel they created" and says its names are released and partners get `peer_removed`; the subscription paragraph's "even with no channels" → "even with no name".
- [ ] Step 38: Modify `README.md`:
  - the tagline (line 3, "Cross-host 1-to-1 channels") and the intro paragraph (no secret ID; peers reach each other by name; no `<hostname>/<name>` label);
  - the files table (`agent-bus.db`: "accounts, channels and messages" → "accounts, sessions and messages");
  - the Stop hook paragraph ("even with no channels" → "even with no name"; "sender labels" → "sender names");
  - "The agent workflow" (claim, pass the name on, wake, converse, release);
  - the event list (no `peer_joined`/`channel_expired`);
  - the Addressing paragraph;
  - the Hermes section: the webhook `--prompt` ("a new channel ID" → "tell the user"), the "labelled `<hostname>/<my_name>`" note, the `idle_timeout_seconds` note ("ends the session and all its channels" → "releases its name"), the respawn paragraph and the memory/system-prompt block (claim a name again after a gateway restart; no channel IDs), the "Fresh session per wake" note, and the "Latency" note ("after `create_channel`" → "after `claim_name`");
  - the admin paragraph ("even with no channels" → "even with no name"; "its peers get `peer_removed`" → "its conversation partners get `peer_removed`");
  - the Hermes section intro ("carries the sender labels" → "sender names") and the Stop hook paragraph's "only a count and the sender labels" (Critic R2 N2);
  - Security model: the Accounts bullet's "no usable credentials, sessions or joinable channels" → drop "joinable channels"; replace "Channels" with "Names" (not secrets; any approved account can message any held name; first-come squatting; the probe surface of `name_taken`/`unknown_peer`; first-contact marker), rewrite "Account visibility", add the unbounded-inbox note, and remove "Labels are self-reported" from Unchanged;
  - the upgrade note (clean break; upgrade broker and shims together; copy `agent-bus.db` before upgrading, because rolling back means restoring that copy, per N11). Update the earlier multi-user upgrade list ("drops all channels, sessions and messages") so it does not contradict the new note.
- [ ] Step 39: Modify `CLAUDE.md`: the architecture summary (the opening paragraph: no channel or secret ID; routes `/v1/name`; "Stores only SHA-256 hashes of channel IDs, …" → drop channel IDs; "five tools"; the `names.go`, partners and departed description; the sweeper without channel TTL; `wait` "prints only sender names"; `internal/protocol` "label pattern" → "name rules"); "Behaviours that span several files" (addressing by bare broker-wide name; the waking list); the doc-sync paragraph ("channel expiry" in the list of restated behaviour) and checklist bullets (tool list and `claim_name`'s `name` rules instead of `my_name`; `wait` exits on a pending message or no name; the timings list without the channel TTL).
- [ ] Step 40: Modify `deploy/agent-bus.service`: Description "cross-host peer channels" → "cross-host peer messaging". Modify `flake.nix`: `description` "cross-host 1-to-1 channels between Claude Code sessions" → "cross-host 1-to-1 messaging between Claude Code sessions".

### Layer: Verification
- [ ] Step 41: Final verification:
  - `just test` and `just build` are green; `go vet ./...` is clean; `gofmt -l .` is empty;
  - `grep -rniE 'channel|handle|ch-|uuid|host/name|label|my_name|ValidLabel|peer_joined|LiveChannels' --include=*.go --include=*.md --include=*.nix --include=*.service . | grep -v '^./specs/'` shows only intended hits (Go `chan` and context words, `notify.go` waiter channels, and internal identifiers such as `UnreadLabels` if kept). Review each remaining hit; no user-facing text may still say "label" for a peer address;
  - re-read both `SKILL.md` files against `tools.go`, `wake.go`, `wait.go` and `protocol.go` (CLAUDE.md checklist);
  - no hostnames or personal paths in the diff.
