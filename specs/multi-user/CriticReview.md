# Critic Review: Multi-User Trusted Broker

> Phase 3 — Plan stress-test, in rounds. Newest round first.

## Round 2 — 2026-11-07

**Summary:** All round-1 fixes are present and consistent between Architect.md and tasks.md: `IssueClaim` (M4, steps 12/16/25), the `registration_pending` rendering order (M11, steps 34/40), the bump + `Refresh` + `WantAdmin()` subscription path (M11, steps 13/35/37/39/43/46), `DropAfter401` (M16, steps 35/36/38/42), tracker `Forget` (M8, steps 13/15/24), the keeper's 401 stop (M12, steps 39/46), the upgrade/rollback section (step 53), claim-path name checks (M7, steps 13/21), credential safety in enroll (M15, steps 32/33), the admin audit log, and the round-1 notes. No blockers this round. The fresh findings are mostly on the edges of the round-1 subscription fix: a `wait` held armed only by `wantAdmin` was never woken when the keeper gave up; the R1-B3 change made step 37 depend on 35/36 while the parallelisation section still called it independent; and `Ensure` would surface a pruned credential's 401 as a "session expired" error. Others: peer events carried no account (so a channel creator only learned the joiner's account from its first text), a running shim never re-reads the broker file after the human fixes it, and the start-time claim issue could be read as also issuing a logged `recover` code. Lenses with nothing found: Scope (no creep; the Phase 2 decisions are respected), Data integrity (migration, cascades and `ON DELETE RESTRICT` remain sound), Testing (every new behaviour has a test step; the added behaviours got test lines in existing steps). The step count stays at 55.

### Carried over from Round 1
| Item | User's answer | Where fixed |
|------|---------------|-------------|
| — | No open decisions from round 1. | — |

### Blockers (applied)
| ID | Lens | Finding | Where fixed |
|----|------|---------|-------------|
| — | — | None found. | — |

### Warnings
| ID | Lens | Finding | Where fixed / why skipped |
|----|------|---------|---------------------------|
| R2-W1 | Correctness | `handleWait` blocks on `Mirror.Changed()`. When the M12 keeper gives up (401/403) it clears `wantAdmin` on a dropped session, but nothing changes in the mirror, so a `wait` held armed only by `wantAdmin` never re-evaluates. It stays armed forever, never shows the "subscription could not be restored" notice, and the Stop hook sees it as armed and stays silent. | Architect M12 (`Mirror.Poke()` on every clear of `wantAdmin`); Files to Modify (`mirror.go`); tasks steps 36, 39, 43, 46 |
| R2-W2 | Correctness (Analyst: account shown wherever a peer is identified) | Only text rows got `from_account`. `peer_joined` is how a channel creator learns who joined, and with no `peers` tool (Phase 2 decision) the creator saw the joiner's account only on the first text message. The same applied to `peer_left`/`peer_removed`/disconnect events. | Architect M9 (`enqueue` takes the account; all peer events fill it); Files to Modify (`sweeper.go`); tasks steps 10, 11, 14, 19, 50, 51 |
| R2-W3 | Correctness | A 401 from `POST /v1/sessions` (credential unknown, e.g. retired row pruned after 7 days) is an `APIError` that `errors.Is(…, client.ErrUnauthorized)` matches. `Ensure` returned it as is, so tools showed "session unknown to the broker (expired or reaped)" instead of an account message (Analyst edge: "the shim reports the account is no longer valid"), and the keeper's stop condition depended on telling the two 401s apart. | Architect M16 (`Ensure` maps it to `AccountError`), M12 (keeper stops on `AccountError`); tasks steps 35, 39, 41 |
| R2-W4 | Correctness / UX | The shim reads the broker file once at start, and the pin and credential are fixed in the client. After the human runs `register`/`pin`/`unpin` as the setup message tells them, the agent would keep getting the same setup or pin error until restart, and nothing said so (M17 covered only the hostmaster hint). | Architect M16; tasks steps 38, 45, 51, 53 |
| R2-W5 | Parallelisation | After R1-B3, step 37 (`wake.go`) reads `Session.WantAdmin()` (step 35) and `Summary.AdminSubscribed` (step 36), but the parallelisation section still listed 34 and 37 as independent. Run in parallel, step 37 would not compile. | Architect "Parallelisation Opportunities"; tasks step 37 |
| R2-W6 | Security | `IssueClaim` picks `recover` mode when a hostmaster exists, and step 12 said `Run` uses it while step 17 said "when there is no hostmaster". Read the first way, every restart of a claimed broker would log a working recovery code to the journal, so anyone who can read logs could replace the hostmaster. | Architect M4 (start issues only `claim` codes, after `HasHostmaster`); tasks steps 12, 17, 25 |

### Notes
| ID | Lens | Finding | Where fixed / why skipped |
|----|------|---------|---------------------------|
| R2-N1 | Correctness | `Drop` runs its callbacks while holding `Session.reg`, and the keeper calls `Ensure`, which takes `reg`. A hook that calls `Ensure` directly would deadlock. | Architect M12; tasks step 35 |
| R2-N2 | Correctness | `DropAfter401` sets the account-removed notice, then the keeper sets "subscription could not be restored". With one notice string the second would overwrite the first. | Architect M12; tasks steps 35, 41 |
| R2-N3 | Completeness (Delta) | Delta's waking-kinds line named only "an administrative event kind" and missed `peer_removed`; the `wait` change for subscribed sessions was not listed. | Delta.md MODIFIED |
| R2-N4 | UX | `noChannelsReason` says channels ended because "the peer left or the channel expired"; step 37 kept it unchanged although `peer_removed` now also ends channels. | Architect Files to Modify (`wake.go`); tasks step 37 |
| R2-N5 | Correctness | With an unusable broker file `client.New` fails, but the shim must still start its MCP server and wake socket to return the setup message. The plan did not say the client may be absent. | tasks step 49 |
| R2-N6 | Correctness | `PathFor` keys the file by host. A mixed-case host (`Broker-Host` vs `broker-host`) would give two files and two independent pins for the same broker. | Architect M14; tasks steps 7, 8 |
| R2-N7 | Security / Operational | The register refusal log carries the "real reason". For a bad name, logging the name lets an unauthenticated caller put arbitrary text (up to 1 MiB) into the journal. | Architect M7; tasks step 13 |

### Decisions (for the user)
| ID | Severity | Finding | Options | Recommendation |
|----|----------|---------|---------|----------------|
| — | — | None. R2-W4 could also be fixed by reloading the broker file on each call; the restart instruction matches the already approved M17 behaviour for the hostmaster hint and needs no client rebuild, so it was decided here. | — | — |

### Overrides earlier rounds
- R1-W3's keeper stop on "a 401 from `POST /v1/sessions`" now arrives as an `AccountError` from `Ensure` (R2-W3). The behaviour is unchanged, only the signal: the keeper stops on any `AccountError` or a 403 from subscribe.
- Architect "Parallelisation Opportunities" no longer lists step 37 as independent (R2-W5); this corrects the round-1 B3 fix's side effect.

### Residual risks (couldn't be verified offline)
- That `errors.As` with a `*client.PinError` target finds an error returned from `tls.Config.VerifyConnection` through `crypto/tls`, `*url.Error` and the client's `%w` wrapping (step 30 tests it).
- That a pinned `http.Transport` with a custom `TLSClientConfig` and the server's `tls.NewListener` agree on HTTP/1.1 for the long-poll (Go disables HTTP/2 in both cases unless configured); harmless either way, but confirm in the TLS client tests.
- The round-1 residual risks (plain-HTTP 400 on a TLS listener, root reaching the control socket under `DynamicUser`, refusal timing) still apply.

### Verdict
Approved

---

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
