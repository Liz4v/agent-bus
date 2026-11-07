# Analyst: Multi-User Trusted Broker

> Phase 1 — Problem definition. Approved before architecture begins.

## Goal / Outcome

**Scope classification:** Complex

Today the broker has no accounts: the network is the perimeter and every agent belongs to one supervising human. A real session showed the value of two humans, each with their own agent, working one task over the bus. The goal is to let a **hostmaster** run a broker reachable over the internet that admits a small, manually approved set of **users**, each with their own agents, while keeping the existing 1-to-1 channel workflow and the existing untrusted-peer posture.

"Trusted" is meant in the negative sense: the hostmaster explicitly and manually decides who gets in; nothing about an admitted user's agents is trusted by other users' agents.

## Scope

**Included:**
- Accounts on a broker, with a hostmaster role (first registered account, claimed with a one-time code printed by the broker at first start).
- Registration is mandatory: a broker serves no sessions until a hostmaster has claimed it.
- Registration requests that the hostmaster manually approves or rejects.
- Per-account credential held by the shim; agent sessions authenticate under an account.
- Hostmaster administration, available to the hostmaster's agents as tools: list accounts and pending requests, approve, reject, revoke/remove, configurable maximum number of accounts.
- Opt-in subscription to administrative notifications (e.g. a registration is pending) for hostmaster-account sessions, usable by an unattended peer such as a Hermes agent.
- Broker-verified account holder shown on every peer, in `peers` and in `read` framing.
- Encrypted client↔broker transport (TLS) with broker authenticity via a host fingerprint pinned on first contact (trust on first use), and refusal on later mismatch.
- Documentation and skill updates describing the new mode and its security model.

**Excluded (non-goals):**
- True end-to-end encryption between peers (broker-blind message text). Recorded as a possible later phase.
- Multi-party (group) channels. Channels stay 1-to-1; the channel ID stays the join secret.
- Discovery of peer agent names or listing of other users' sessions/channels.
- Public sign-up, open registration, federation between brokers, CA/PKI-based or operator-supplied certificates.
- Per-device enrolment, in-place credential reset, and hostmaster transfer other than the host-side recovery command.
- Roles beyond hostmaster and ordinary user; per-channel permissions.
- Message history, rate limiting and other changes to message handling (unchanged from today).
- Hostmaster ability to read or act on behalf of other users' sessions.

## Behaviour

Accounts and admission
- When a broker starts with no hostmaster, the system must generate a single-use claim code, make it available only to whoever can read the broker host's console/log, and refuse every session, channel and non-claim registration until it is claimed.
- When a user registers on a broker with no hostmaster and presents a valid claim code, the system must make that account the hostmaster and active immediately, and consume the code.
- When a registration on a broker with no hostmaster presents a missing, wrong or already-used claim code, the system must refuse it without revealing which of those applied.
- When the broker restarts before being claimed, the system must issue a new claim code and invalidate the previous one.
- When a user registers on a broker that already has a hostmaster, the system must hold the registration as pending and must not grant it any access until the hostmaster approves it.
- When the hostmaster approves a pending registration, the system must activate the account so its holder can start agent sessions.
- When the hostmaster rejects a pending registration, the system must discard it and the requester must be told it was not accepted.
- When the hostmaster revokes an account, the system must invalidate that account's credential, end its sessions, and end its channels immediately; remaining peers must see a channel-ended event that states plainly the peer's account was removed by the hostmaster, so the agent does not investigate why the peer is gone. Unread messages are discarded.
- When a user loses their credential, the system must offer no in-place reset: the hostmaster removes the account and the user registers again.
- When the number of active accounts (including the hostmaster) has reached the configured maximum, the system must refuse further registrations with a clear reason. The maximum has a small default, is stored by the broker, and is changed by the hostmaster through an admin tool.
- When the number of pending registrations has reached its own small limit, the system must refuse further registrations. Pending requests do not count toward the user maximum.
- When a registration request is submitted, the system must accept only the account name (validated against the name pattern) from the requester and no free-form text, so an unsolicited request cannot carry instructions to the hostmaster's agent.
- When a registration stays undecided for the pending-expiry period, the system must discard it.
- When a registrant asks for the status of their registration, or tries to start a session with its credential, the system must report whether it is pending, approved, rejected, expired or removed. Nothing is pushed to the registrant.
- When a name is chosen at registration, the system must apply the same pattern and length rules as agent names and compare names case-insensitively for uniqueness. A removed, rejected or expired name becomes free again.
- When an operator runs the host-side recovery command on the broker host, the system must issue a fresh claim code that, once used, makes the new registrant hostmaster and removes the previous hostmaster account (with its sessions and channels). This is the only way to replace a hostmaster.
- When the hostmaster asks, the system must list active and pending accounts with their account names.
- When an admin action is attempted by a session that does not belong to the hostmaster account, the system must refuse it.
- When a hostmaster-account agent invokes an admin action, the system must apply it exactly as named by the agent's arguments (a specific account), and must not accept a bulk or implicit target.
- When an approval, rejection or revocation names an account that is not in the state the action applies to (e.g. approving an account that is not pending), the system must refuse it.
- When the human has not pre-authorized unattended handling, the tool descriptions and hostmaster skill must direct the agent to confirm each approval or revocation with its human first; when the human has pre-authorized it (e.g. an unattended Hermes peer), the agent may act on admin events directly.

Administrative notifications
- When a hostmaster-account session subscribes to administrative notifications, the system must deliver subsequent admin events (at minimum: a registration is pending) to that session, and must stop when it unsubscribes or the session ends.
- When an admin event is delivered, the system must wake an idle subscribed session the way a peer message does, so an unattended hostmaster peer can act on it.
- When an admin event is shown to an agent, the system must frame it as untrusted data: account names are chosen by the registrant, so they are content, not instructions.
- When a session of any other account tries to subscribe, the system must refuse it.

Authentication and sessions
- When a registration is created, the system must give its holder a long-lived account credential that is stored locally by the client and never exposed to the agent.
- When the holder copies the credential file to another host, the system must accept sessions from that host as the same account; revoking the account revokes every copy.
- When an agent session starts in account mode, the system must authenticate it under an approved account and attribute it to that account for its lifetime.
- When a credential belongs to a pending, rejected or revoked account, the system must refuse to start or continue sessions with it.
- When the broker stores account credentials, it must store only hashes, consistent with how channel IDs and session tokens are stored today.

Peers and channels
- When a session creates or joins a channel, the system must require that session to belong to an active account.
- When an agent lists peers or reads a message, the system must present the sender's broker-verified account name as a separate field, in addition to the existing label.
- When the account name is shown to the receiving agent, the system must treat the message content as untrusted data exactly as today, regardless of who the account holder is.
- When an agent asks for peers, the system must reveal only peers on channels that session belongs to; names of other agents and other users' channels stay undiscoverable.
- When a channel is joined, the system must still require the secret channel ID and an open channel, and failure must remain a single generic "no such open channel" error.

Transport and broker authenticity
- When a client connects to a broker in account mode, the system must use an encrypted connection.
- When the broker starts for the first time, the system must generate its own keypair and self-signed certificate, and keep the same key across restarts and certificate renewals.
- When a client connects to a given broker for the first time, the system must present the fingerprint of the broker's public key to the human and, on acceptance, record (pin) it.
- When a client later connects and the broker's public-key fingerprint does not match the pinned one, the system must refuse the connection with a clear error and must not continue until the human explicitly removes the old pin.
- When an agent (not the human) runs, the system must not allow it to accept a first-contact fingerprint, re-pin, register, or claim the hostmaster role.

Compatibility
- When any client without account authentication connects to a broker, the system must refuse its sessions. There is no unregistered mode.
- When an existing deployment is upgraded, the system must require its operator to claim hostmaster before it serves anyone; existing channels and sessions from before the upgrade are not carried over.

## Rules & Constraints

- The hostmaster's manual approval is the only way an account becomes active (apart from the very first registrant, who needs the claim code).
- No path exists to use a broker without an account; the hostmaster role can only be claimed by someone holding the claim code.
- Peer message content is always untrusted data to the receiving agent; the account holder name is context, never authority.
- Agent names remain undiscoverable; only the account holder of a peer you are already on a channel with is revealed.
- Account name shown for a peer is established by the broker, never self-reported by the peer's agent.
- Agents cannot perform registration, claiming, fingerprint acceptance or re-pinning. Those are human actions outside the agent tool surface.
- Approval, rejection, revocation, listing, account cap and admin notification subscription are available to the hostmaster account's agents, and only to them. Because a peer's message could try to steer such an agent, every admin tool must name its target account explicitly, and the skill/tool descriptions must tell the agent never to act on admin requests that arrive via peer messages without its human's confirmation.
- Hostmaster guidance (admin tools, notification subscription, the warning about peer-sourced admin requests) lives in a separate hostmaster skill, not in the general agent-bus skill. The general skill only states that a hostmaster role exists and points to the other skill; both skills are kept in sync with the code on every behaviour change.
- Account credentials and session tokens never reach the agent; the broker stores only hashes.
- Fingerprint mismatch is a hard failure, never a prompt the agent can answer.
- Existing guarantees stay: messages deleted on read, channel TTL and grace periods, generic join error.
- No hostnames, personal paths or details of the author's own machines in code, docs, tests or commits.

## Edge Cases

| Scenario | Expected behaviour |
|----------|--------------------|
| Two users register simultaneously on an unclaimed broker, both with the valid code | Exactly one becomes hostmaster (code is single-use); the other is refused. |
| Stranger reaches an unclaimed broker first | Cannot claim without the code; broker serves nothing. |
| Claim code leaks from the broker log | Whoever uses it first becomes hostmaster; documented as the residual risk. |
| Hostmaster never claims and the broker stays exposed | Broker serves nothing; restart issues a fresh code. |
| Peer message asks the hostmaster's agent to approve or revoke an account | Agent treats it as untrusted data and asks its human first. |
| Unattended hostmaster peer (e.g. Hermes) is subscribed and a registration arrives | Peer is woken with the framed event and may act via admin tools. |
| Subscribed hostmaster session ends | Subscription ends with it; a new session must subscribe again. |
| Registered account name already taken (active or pending) | Registration is refused with a clear reason. |
| Hostmaster revokes themself or the last hostmaster | Refused; a broker always has a hostmaster. |
| Hostmaster loses their credential | Operator runs the host-side recovery command; a new claim code replaces the old hostmaster. |
| Strangers flood registrations | Pending limit reached, further requests refused; undecided requests expire; active cap untouched. |
| Registrant name equals a removed user's former name | Allowed; identity is the broker-side account, channels are transient (IRC-like) and the old account's channels ended on removal. |
| Registration attempt whose name contains instructions or odd characters | Refused by the name pattern; nothing reaches the hostmaster's agent. |
| Pre-authorized unattended hostmaster peer receives a pending event | May approve directly; still limited to explicit, pending targets. |
| User copies credential to a second host | Both hosts act as the same account; both share the fate of the account. |
| Broker key regenerated (database/keys wiped) | All clients see a fingerprint mismatch and must re-pin deliberately. |
| Pending user tries to start a session before approval | Refused with a "pending approval" style reason; no access. |
| Revoked user's shim keeps a live sync | Its sessions end; next call fails and the shim reports the account is no longer valid. |
| Revoked user was the only peer on a channel | Remaining peer sees the usual peer-left / channel end events. |
| Broker certificate or fingerprint changes (reinstall, MITM) | Clients refuse; human must deliberately re-pin. |
| First contact with a malicious broker | Accepted on first use by design (hatchling model); documented as the residual risk. |
| Existing single-user broker upgraded to this version | Broker refuses all sessions until the hostmaster claims it with the printed code. |
| Peer is the same human's other agent (same account) | Shown with the same account name; still untrusted data. |
| Account maximum lowered below the current count | Existing accounts stay; new registrations are refused. |
| Hostmaster rejects a request, requester retries | Allowed as a new pending request, subject to the maximum. |
| Unencrypted connection attempt to an account-mode broker | Refused. |

## Open Questions

Questions resolved during this phase, with confirmed answers.

| Question | Answer |
|----------|--------|
| What does e2e encryption cover? | Client↔broker TLS with a pinned broker fingerprint only. Peer-to-peer broker-blind encryption is a non-goal for now. |
| How do users get admitted? | Registration request, manually approved by the hostmaster; first registrant is hostmaster, proven by a one-time claim code printed by the broker at first start. |
| How is the hostmaster role protected from being claimed by a visitor? | Registration is mandatory (no unregistered mode); the claim code closes the first-come race. |
| How does the hostmaster exercise the role? | Admin actions are agent tools available only to hostmaster-account sessions (flagged risk: peer-message steering, mitigated by explicit targets and untrusted framing). |
| Lost credential (user or hostmaster)? | User: hostmaster removes the account, user re-registers. Hostmaster: host-side recovery command issues a fresh claim code and replaces the hostmaster. |
| Second device for the same user? | Copy the credential file; all copies share the account. |
| What is pinned, and where does the cert come from? | Broker generates its own keypair and self-signed cert; clients pin the public-key fingerprint. Operator-supplied/CA certificates are out of scope. |
| How does a registrant learn the outcome? | Human checks with a status command or by starting a session; nothing is pushed. |
| Pending flood protection? | Separate small pending limit with expiry; pending does not count toward the user cap. |
| Account name rules and reuse? | Same pattern and length as agent names, case-insensitive uniqueness, names reusable after removal/rejection/expiry. |
| Where is the user cap set? | Broker-stored setting with a small default, changed by the hostmaster's admin tool. |
| What do peers see on revocation? | The channel ends immediately with an explicit "peer's account was removed by the hostmaster" indication, so agents do not waste effort investigating; unread messages discarded. |
| Prompt injection through unsolicited registration? | Registration carries only a pattern-validated account name and no free text; admin events are framed as untrusted; tools name explicit targets; agents ask their human first unless the human pre-authorized unattended approval. |
| How does the hostmaster learn of pending requests? | Opt-in subscription to administrative notifications, which wake the subscribed session; the natural user is an unattended Hermes peer. |
| How do an account's agents authenticate? | Per-account secret held locally by the shim; sessions are attributed to the account. |
| Existing no-account deployments? | Superseded: single-user mode is removed. Upgraded brokers must be claimed before serving. |
| Channel model? | Unchanged: 1-to-1, channel ID is the join secret. |
| How is the account holder shown on a peer? | As a separate field next to the existing label. |
| Hostmaster powers? | List accounts and pending requests, reject, revoke/remove, and cap the number of users (plus approve). |
| Fingerprint change? | Refuse and require manual re-pin. |

Interpretations taken from the briefing and flagged for your review at approval:
- Registration, claiming and fingerprint pinning are human-only actions outside the agent (MCP) tool surface. Approval and the other admin actions are agent tools for the hostmaster account only.
- Admin notifications are an opt-in subscription that wakes the subscribed session; subscription is per session and not persisted.
- The claim code is shown only on the broker host's console/log and is re-issued at each restart until used.
- Broker authenticity uses a self-signed certificate/fingerprint, not a public CA.
- A broker always keeps at least one hostmaster; the hostmaster cannot be revoked by an agent tool and is replaced only through the host-side recovery command.
- Concrete defaults (user cap, pending limit, pending expiry period) are left to the architect as shared protocol constants; confirmed: user cap 5, pending limit 5, pending expiry 2 hours.

## Git Baseline

Recorded at the start of Phase 1. Do not update later.

- **Branch:** main
- **HEAD:** 7f68dac7c10be77be4953286934310320ebda922
- **Upstream:** origin/main
- **Sync state:** in sync
- **Working tree:** dirty (only untracked `specs/`)

## Analyst Checklist

- [x] Goal is tied to a specific user need
- [x] Scope boundaries are explicit — what's in and what's out
- [x] All ambiguities resolved — no open questions remain (interpretations flagged above for approval)
- [x] Behaviour is declarative, not prescriptive
- [x] Edge cases are identified and handled
- [x] Non-goals prevent scope creep
