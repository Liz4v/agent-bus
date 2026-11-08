# Critic Review: Name addressing (drop the channel ID)

> Phase 3 — Plan stress-test, in rounds. Newest round first.

## Round 2 — 2026-11-07

**Summary:** The Round 1 fixes hold: `DiscardPeerRows` closes the stale-mirror hole for rows already on the host, the e2e expectation now matches N3/N4, the per-step Done lines no longer claim builds that cannot pass inside the broker track, and `send`'s partner insert comes before the `enqueue`. This round re-read every cited symbol against the code (`store.go`, `channels.go`, `sweeper.go`, `accounts.go` `deleteAccount`/`notifyAdmins`, `server.go` `sync`, `mirror.go` loop and `applyLocked`, `tools.go`, `wake.go`, `session.go`, `client.go` `do`, `wait.go`, `hook.go`, `hermeswake`, `cmd/agent-bus/main.go`) and found no blockers. Three warnings: (W1) a narrower form of R1 B1 remains. A sync response computed before the release and applied after `DiscardPeerRows` brings back a just-deleted message and the old name. (W2) Checkpoint B still cannot pass, because `cmd/agent-bus` imports the wake-consumer packages that are only rewritten in Steps 29–34. (W3) The shared `testBroker` harness in `session_test.go` (`failChannels` on `/v1/channels`, `CreateChannel`/`JoinChannel` fixtures) was not assigned to any step. Lenses: Security: none found. `name_taken`/`unknown_peer` are the only probes, `departed` answers only former partners, the account comes from the session join, and `agent-bus`/`admin` are reserved case-insensitively. Data integrity: none found. FKs, cascades, the `departed(name_key)` and `partners(hi)` indexes and the v2→v3 drop order are sound, and `first_contact` scans into a Go bool through database/sql's string conversion. Scope: no creep. Upgrade paths: old shim → 410 text and new shim → `http_404` mapping both match `client.do`'s plain-text handling. No `copilot-instructions.md`, `specs/STATE.md` or `docs/` exist in this repo.

### Carried over from Round 1
None. Round 1 left no decisions open.

### Blockers (applied)
None.

### Warnings
| ID | Lens | Finding | Where fixed / why skipped |
|----|------|---------|---------------------------|
| W1 | Correctness (race) | `server.sync` recomputes after a version bump, so a message arriving just before `release_name` can sit in a response that is already on the wire. If the loop applies it after `DiscardPeerRows`, `read` shows a discarded message and the mirror briefly says `Named` again. Its ID is above `after`, so the follow-up `Refresh` doesn't remove it. This breaks the Analyst's "discard that session's unread messages" in a narrow window. | Architect N4 (new bullet), Files to Modify (mirror); tasks Step 19 (discard epoch, loop drops pre-discard responses), Step 20 (test using the harness's `delay` hook). |
| W2 | Verification (build) | Checkpoint B ran `go build ./...` and `go vet ./cmd/...` after Step 28. `cmd/agent-bus/main.go` imports `internal/wait`, `internal/hook` and `internal/hermeswake`, which still reference `WaitNoChannels`, `LiveChannels` and `ValidLabel` until Steps 29–34, so it would fail when run in step order. | tasks Checkpoint B (shim and client only), Checkpoint C (`go build ./...` and vet of `./cmd/...`); Architect *Dependencies*. |
| W3 | Test-fixture compatibility | `internal/shim/session_test.go` holds the `testBroker` harness that every shim test uses. Its `failChannels` hook targets `/v1/channels` (which becomes the 410 route), `TestCloseMakesPeerReadPeerLeftImmediately` and `TestDropWithNoticeAppends` call `CreateChannel`/`JoinChannel`, and one test asserts `r.Channels[0].State`. Step 21 covered only notice wording, and `tools_test.go`'s `TestCreateFailureAfterRegistrationReusesSession` depends on `failChannels`. | tasks Step 21 (harness and fixtures), Step 25 (claim equivalent of the retry test); Architect Files to Modify (session row). |

### Notes
| ID | Lens | Finding | Where fixed / why skipped |
|----|------|---------|---------------------------|
| N1 | UX | A new shim can still hold a token without a name (after `release_name`, or a session registered only by `admin_subscribe`), so `send` reaches the broker's `no_name`, whose text ends with an upgrade tail meant for old shims. The agent would be told its agent-bus may be outdated. | Architect N13 (`brokerErr` maps `no_name` to `noName`); tasks Step 24, Step 25 test. |
| N2 | Docs naming the module | Step 41's grep did not search for `label` or `my_name`, so README's Hermes intro ("carries the sender labels") and the hook paragraph's "sender labels" could survive. The admin paragraph's "its peers get `peer_removed`" should say partners. | tasks Step 38 (both paragraphs), Step 41 (grep adds `label|my_name`). |
| N3 | Testing | `TestHermesWakeEndToEnd` claims `alice` from two live sessions (`a`, `a2`) on one broker. With broker-wide names, the second claim gets `name_taken`. | tasks Step 35 (Others). |
| N4 | Correctness | Release keeps the token, so a released session's `read` still syncs. That's fine because no peer can address it, and `registration_pending` is the only kind that can still arrive. | No change needed. |

### Decisions (for the user)
None.

### Overrides earlier rounds
- none. Round 1 B1 is extended by W1, not undone.

### Residual risks (couldn't be verified offline)
- W1's epoch check relies on the loop holding `m.mu` both when it reads the epoch for a request and when it applies the response. Step 20's delayed-sync test must actually interleave the release between the two.
- Round 1's residual risks stand: concurrent `ClaimName` overlap, `schemaV3` drop order with populated `members`, and mixed-version Hermes/hook against an old shim.

### Verdict
Approved

---

## Round 1 — 2026-11-07

**Summary:** The design is sound and fits the store's existing pattern: one IMMEDIATE transaction per mutation with a bump set, inbox rows as events, and the mirror/wake split. The `departed` table is a good fit for the Analyst's "precise error without a listing" rule. Partner-only `peer_removed` errors are the right reading of the Analyst, because a removal error for a non-partner would reveal that a name existed. The main defect is in the shim. Release deletes unread rows only on the broker, but the shim's mirror already holds every synced unread row and never re-fetches below `after`. So after `release_name`, `read` would still show the discarded messages and `wait` would answer `pending` instead of `no_name` (B1). The e2e step also encoded an expectation that contradicts N3/N4 (B2). Several per-step "Done" checks could not pass because Go compiles and vets all of a package's test files together. The doc steps missed about a dozen channel mentions (README Hermes section, flake.nix, skill frontmatter, CLAUDE.md lines). Lenses: Security: no new hole found; the probe surface is as the Analyst accepted, the account on every row comes from the session join, and reserving `agent-bus` stops a peer from spoofing the system sender. Data integrity: FKs and cascades are complete, the migration order is FK-safe under `foreign_keys(1)`, and rollback was undocumented (W5). Scope: no creep. There are no `copilot-instructions.md`, `specs/STATE.md`, `docs/business-rules` or `docs/UX-Documents` in this repo.

### Carried over from Round 0
None (first round).

### Blockers (applied)
| ID | Lens | Finding | Where fixed |
|----|------|---------|-------------|
| B1 | Correctness | `ReleaseName` deletes the caller's unread rows on the broker only. `Mirror` keeps every synced row above the cursor (`mirror.go` `applyLocked`), and later syncs pass `After`, so rows already on the host survive. `read` would show "discarded" messages, and the wake would answer `pending` instead of `no_name`. That violates the Analyst's "discard that session's unread messages" and N15's "release while armed → no_name". | Architect N4 (new bullet), N13 `release_name` row, Files to Modify (mirror); tasks Step 19 (`DiscardPeerRows`), Step 20 (tests), Step 24 (call it after `DELETE /v1/name`), Step 25 (read and WAIT after release). |
| B2 | Correctness / Testing | Step 35 said "A's clean shutdown gives carol nothing (no partnership with A yet)", but A's send to carol in the line before creates the partnership (N3), so carol must get `peer_left` (N4). The coder would either write a failing test or bend the code to match it. | tasks Step 35. |

### Warnings
| ID | Lens | Finding | Where fixed / why skipped |
|----|------|---------|---------------------------|
| W1 | Cross-file consistency | N7 step 5 said "enqueue, then insert the partner row", but `first_contact` is set from that insert's `RowsAffected` (N3, Step 6). Also, `lo`/`hi` ordering was not stated in Step 6. | Architect N7.5; tasks Step 6. |
| W2 | Verification (build) | Steps 7 and 8 claimed `go build ./internal/broker` before Step 9 removes `server.go`'s calls to deleted methods. Step 9 claimed `go vet`, which type-checks the not-yet-rewritten test files. Steps 10–13 ran `go test` while sibling test files still used channel symbols. Checkpoint B ran repo-wide `go vet ./...` before the e2e and wake-consumer tests were updated. | tasks Steps 6–13 Done lines, Checkpoint B; Architect *Dependencies*. |
| W3 | Completeness | Step 21 missed the third channel notice in `session.go` (the credential-unrecognised drop, "…so your channels are gone…") and the `HadState` comment. Step 33 missed `DefaultNoChannelsSleep`. Step 24 missed the `Tools` doc comment ("six MCP tools") and 401 handling in `release_name`. | tasks Steps 21, 24, 33; Architect N13. |
| W4 | Docs naming the module | Channel wording outside the doc steps: `flake.nix` description; README tagline, files table, Stop hook paragraph, admin paragraph, Hermes notes (label, `idle_timeout_seconds`, respawn, memory block, fresh session, latency), the Accounts bullet ("joinable channels") and the earlier multi-user upgrade list; the skill's frontmatter description, Facts (handles, held messages, 1-hour expiry) and events line; the hostmaster skill's intro and rule 4 ("any open channel they created"); CLAUDE.md's opening paragraph, "hashes of channel IDs", `wait` "labels", "label pattern", "channel expiry" and the `my_name` checklist bullet. | tasks Steps 36–41 (and the Step 41 grep now covers `*.nix` and `*.service`); Architect Files to Modify (`flake.nix`). |
| W5 | Rollback | Rollback was undocumented. An old binary on a v3 database skips its migration (`v >= 2`) and fails at runtime on the missing tables. | Architect N11 (Rollback); tasks Step 38 (copy the DB first). |
| W6 | Correctness (UX) | After a crash, the old session keeps the name for about 6 minutes (60 s disconnect detection plus the 5-minute grace), so the user's restarted session gets `name_taken` on its own name. The Analyst fixed the lifetime, so this is guidance, not a design change. | tasks Step 36 (Workflow 2). |
| W7 | Parallelisation | The shim track was declared independent of the broker track, but `internal/shim/session_test.go` imports `internal/broker`, so Checkpoint B needs Checkpoint A. | Architect *Dependencies*; tasks Checkpoint B. |

### Notes
| ID | Lens | Finding | Where fixed / why skipped |
|----|------|---------|---------------------------|
| N1 | Upgrade | An old shim can still call `POST /v1/messages` with the session token it got just before `create_channel` was refused with 410, and would get "claim a name with claim_name first", a tool it does not have. | Architect N7.2 (wording tail); tasks Step 4. |
| N2 | Accuracy | N12 said "`send` gets the same mapping", but an old broker answers `/v1/messages` with a JSON `unknown_peer`, never a plain 404. | Architect N12 reworded. |
| N3 | Testing | Claiming a different-case spelling of the held name was untested. | tasks Step 10. |
| N4 | Security | The first-contact sentence sits inside the `<peer-message>` frame next to untrusted text. A peer can type the same sentence in a later message, but that only adds caution, and its absence can't be faked because the attribute is set by the broker. | Skipped: the user confirmed this design in Phase 2, and there is no exploit. |
| N5 | Data | A releasing session keeps its own `departed` rows, so after it claims again a send to an old partner's name gives `peer_left` instead of `unknown_peer`. | Skipped: harmless, and both answers are true. |

### Decisions (for the user)
None. Architect Risk 2 (refusing a former partner's first send to a new holder) is already settled by the Analyst's edge case ("The send reaches carol's session"). Risk 6 (upgrade notice) needs no new text: running shims are old binaries until their sessions restart, and those get the old 401 notice and then the 410 upgrade message.

### Overrides earlier rounds
- none

### Residual risks (couldn't be verified offline)
- Concurrent `ClaimName` under `SetMaxOpenConns(4)` and `busy_timeout(5000)`: N1 relies on the UNIQUE constraint or the IMMEDIATE lock. Step 10's goroutine test must actually overlap the transactions.
- `DROP TABLE` order in `schemaV3` under `foreign_keys(1)`: inbox → held → members → channels → sessions is FK-safe on paper (members RESTRICT → sessions). Step 11's v2→v3 test with populated members rows confirms it.
- Hermes sidecar and Stop hook at a different binary version than the running shim: an unknown `no_name` status makes Hermes back off ("unexpected WAIT status"), and the hook fails open. Acceptable, but only seen at runtime.

### Verdict
Blockers fixed, verify next round
