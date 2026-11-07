# Critic Review: Multi-User Trusted Broker

> Phase 3 — Plan stress-test, in rounds. Newest round first.

## Round 1 — 2026-11-07

**Summary:** The plan fits the codebase well: accounts own sessions, revocation reuses the `deleteSession`/`removeMember` path, new events are ordinary inbox rows, and every cited existing symbol (`Store.update`, `Tx.enqueue`, `removeMember`, `deleteSession`, `channelList`, `Tracker`, `Mirror`, `Wake.handleWait`, `hook.run`, `client.do`, `secret.*`) exists where the spec says. The main risks were in the shim's subscription path and in rendering: the planned `registration_pending` body could never be shown, and a hostmaster's `wait` would exit with `no_channels` right after subscribing or during the keeper's re-establishment gap. Tests in other packages also had no exported way to get a claim code. Smaller gaps: a misleading drop notice when a tool (not the mirror) sees the revocation 401 first, deleted sessions left in the liveness tracker, the keeper looping forever on an unknown credential, no audit log of admin actions, no upgrade/rollback guidance, and a credential that could be lost to a write error or silently overwritten. Lenses with nothing found: Scope (no creep beyond Analyst + recorded Phase 2 decisions), Parallelisation (file locks and chains are consistent; the added edits stay inside already-declared files). Data integrity: migration order (children dropped first, `accounts` before `sessions`) is sound; only the rollback gap was found. Security: nothing beyond the items below and the already-accepted residual risks.

### Carried over from Round 0
None (first round).

### Blockers (applied)
| ID | Lens | Finding | Where fixed |
|----|------|---------|-------------|
| R1-B1 | Correctness / Verification | M11 stores the registrant's name as the row's `text`, but `shim.body` returns non-empty `Text` verbatim before its kind switch (`text.go:68–71`). The `registration_pending` body planned in step 34 would never render, so the agent would see only a bare name without the confirm-with-your-human guidance. | Architect M11 (rendering bullet); tasks step 34, step 40 test |
| R1-B2 | Verification (symbols) / Testing | Steps 23, 29, 41 and 50 need tests in `client`, `shim` and `e2e` to claim a hostmaster "through `Register` with an issued code", but the claim state is an unexported `claims` field on `Store` and the code is only logged. No exported way to get a code existed. | Architect M4 (`Store.IssueClaim`); tasks steps 12, 16 |
| R1-B3 | Correctness | After `admin_subscribe`, the mirror learns `admin_subscribed` only on the next sync (no version bump, 20 s long-poll), and after a drop the mirror is reset to empty while the M12 keeper re-subscribes. In both windows `handleWait` sees `LiveChannels == 0 && !AdminSubscribed` and replies `no_channels`, so a Claude Code hostmaster's `wait` exits and the session stops listening (Analyst: admin events must wake a subscribed idle session). | Architect M11 (bump + refresh; subscribed = broker flag OR `WantAdmin()`); tasks steps 13, 35, 37, 39, 43, 46 |

### Warnings
| ID | Lens | Finding | Where fixed / why skipped |
|----|------|---------|---------------------------|
| R1-W1 | Correctness | Only the mirror's 401 path checks `AccountStatus`. `send`/`leave`/`withSession` call `Drop` directly (`tools.go:153,214,230`), so if a tool hits the revocation 401 first, the agent gets "memberships expired while this host was disconnected" instead of "account removed" (Analyst edge: "shim reports the account is no longer valid"). | Architect M16 (`Session.DropAfter401`); tasks steps 35, 36, 38, 42 |
| R1-W2 | Architecture fit | `Remove` and the recovery claim delete sessions inside the store, but only the HTTP layer owns the `Tracker`. `deleteSelf` and reaping call `Forget`; the new paths did not, so deleted IDs stay in `seen` (and a late deferred `Touch` re-adds them). | Architect M8; tasks steps 13, 15, 21, 24 |
| R1-W3 | Correctness | The M12 keeper stopped only on 403. A credential whose retired row was pruned gets 401 from `POST /v1/sessions`, so the keeper would retry forever at 60 s, and `wantAdmin` (now used by the wake socket) would keep `wait` armed for nothing. | Architect M12; tasks steps 39, 46 |
| R1-W4 | Operational (rollback) | Migration 2 is one-way and the transport changes on both sides, but there was no upgrade order or rollback procedure. An old binary on a v2 DB skips migration and fails on the new `NOT NULL` columns. | Architect "Upgrade and Rollback" (new section); tasks step 53 |
| R1-W5 | Correctness / Security | M7 sends a request with a claim code straight to the M4/M6 path at step 2, before `ValidName` (step 3). M6 checks the name is free, but nothing applied the name pattern to a claim, so the unauthenticated API could create a hostmaster with an arbitrary name. | Architect M7 step 2; tasks steps 13, 21 |
| R1-W6 | Data integrity | `enroll.Register`/`Claim` get the credential exactly once. A failed `Save` after a successful register loses it (the user must ask the hostmaster to clean up), and re-running `register`/`claim` silently replaced an existing credential (for a hostmaster, irrecoverably without host access). | Architect M15; tasks steps 32, 33 |
| R1-W7 | Operational (observability) | Admin mutations can be performed by an agent, possibly unattended, but only register refusals were logged. No audit trail of approve/reject/remove/cap/subscribe. | Architect M8; tasks steps 15, 24 |

### Notes
| ID | Lens | Finding | Where fixed / why skipped |
|----|------|---------|---------------------------|
| R1-N1 | Correctness | Pending expiry is lazy only in `Register`; `Approve` could activate a request up to one sweep interval (15 s) past `PendingExpiry`. | Architect M7 (approve/reject/list expire first); tasks steps 13, 21 |
| R1-N2 | Operational | The logging middleware labels the bearer short-hash `session`; on credential routes it is a credential hash. | Architect (logging paragraph before the service-unit note); tasks step 17 |
| R1-N3 | Completeness (Delta) | Delta.md did not list the `http://` → `https://` shim URL change or the newly authenticated `POST /v1/sessions`. | Delta.md MODIFIED |
| R1-N4 | Correctness | Step 21 said recovery gives "old sessions" `peer_removed`; the old sessions are deleted, their peers get the event. | tasks step 21 (reworded) |
| R1-N5 | Operational | Control socket path had no length check (the wake socket has one). | Architect M5; tasks step 16 |
| R1-N6 | UX / docs | The hostmaster hint is read at shim start, so admin tools appear only after a restart following `claim`. | Architect M17; tasks step 52 |
| R1-N7 | Build | `flake.nix` pins `vendorHash`; any new module would break the Nix build. | tasks step 55 |
| R1-N8 | Correctness (pre-existing) | Labels can now collide across users (same hostname + same name), and `JoinChannel` then answers with a confusing `same_label`. Channel model is out of scope. | Skipped as a change; recorded as Architect risk 12 |

### Decisions (for the user)
| ID | Severity | Finding | Options | Recommendation |
|----|----------|---------|---------|----------------|
| — | — | None. Every finding had a fix that does not change scope, trade-offs the user would feel, or a recorded decision. | — | — |

### Overrides earlier rounds
- none

### Residual risks (couldn't be verified offline)
- That Go's `http.Server` returns the "client sent an HTTP request to an HTTPS server" 400 when serving a `tls.NewListener` listener via `Serve` (it should, since connections are `*tls.Conn`); confirm in a server test.
- Root reaching a 0600 control socket owned by a `DynamicUser` inside `/var/lib/private` (expected via `CAP_DAC_OVERRIDE`); confirm on a real systemd host.
- Timing equality of `registration_refused` across causes (Architect risk 3) can only be checked roughly in tests.

### Verdict
Blockers fixed, verify next round
