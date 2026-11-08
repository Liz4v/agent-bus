# Eval Report: Name addressing (drop the channel ID)

> Phase 5 — Post-implementation evaluation. Run after sdd-coder completes.
> Evaluator had no access to the coder's reasoning — fresh context only.

## Computational Sensors

| Check | Result |
|-------|--------|
| `go vet ./...` | ✅ 0 issues |
| `gofmt -l .` | ✅ empty |
| `go test -race -cover ./...` | ✅ all packages pass |
| Coverage (changed packages) | ✅ broker 85.1%, client 92.9%, shim 92.1%, wait 86.8%, hook 82.3%, hermeswake 95.3%, protocol 100% (all ≥ 80%) |
| Frontend | n/a |

## Semantic Evaluation

| Criterion | Verdict | Detail |
|-----------|---------|--------|
| CONTRACT | ✅ | `names.go` replaces `channels.go`; `/v1/name` POST/DELETE and 410 `upgrade_required` routes registered in `server.go`; schemaV3 migration present; `NewChannelID` removed; client/shim/wait/hook/hermeswake renamed as specified. |
| BEHAVIOUR | ✅ | Claim rules in order (invalid, reserved, held, taken), idempotent re-claim, release with `peer_left` and inbox discard (keeping `registration_pending`), send with first-contact via `partners` insert, `depart` for partners, wake semantics (`no_name`). |
| EDGE CASES | ✅ | Concurrent claim test, case-insensitive uniqueness, self-send, departed/`peer_left`/`peer_removed` answers, name re-claimed by another session, subscribed hostmaster without name, old/new broker upgrade errors. |
| TESTS | ✅ | Tests exist for every new broker, client, shim, wait, hook, hermeswake and e2e path; error branches covered. |
| PATTERNS | ✅ | Matches repo conventions; vet and gofmt clean. |
| COVERAGE | ✅ | All touched packages ≥ 80%. |
| NO EXTRAS | ✅ | No channel remnants; remaining grep hits are Go `chan`, HTTP handlers, the migration's history (`schemaV2`, drop of old tables) and intended upgrade text. Coder deviations in Architect.md are minor and consistent with the Analyst. No hostnames or personal paths in the diff. SKILL.md files and CLAUDE.md consistent with `tools.go`, wake and protocol. |

## Findings

None.

## Triage

| ID | Criterion | Finding | Decision |
|----|-----------|---------|----------|
| — | — | No findings | — |

## Verdict

- [x] **PASS** — all criteria ✅, ready to archive
- [ ] **PARTIAL PASS**
- [ ] **FAIL**
