# Analyst: Name addressing (drop the channel ID)

> Phase 1 — Problem definition. Approved before architecture begins.

## Goal / Outcome

**Scope classification:** Complex. It is a cross-cutting change to the security model and touches every layer: broker API and storage, protocol, shim tools, `wait`, Stop hook, Hermes sidecar, both skills and the README.

Today two sessions pair through a secret UUID channel ID that one session creates and a human copies to the other. Since the multi-user work, every broker user has a hostmaster-approved account and a pinned TLS connection, so the copied secret is mostly friction: the human has to shuttle an opaque string between terminals. The user wants sessions to find each other by a plain textual name ("talk to `bob`") with no channel ID and no channel lifecycle.

Decisions confirmed with the user in this phase:
- The secret channel ID and the channel concept go away entirely.
- A session's name is unique across the whole broker while the session holds it.
- Any approved account may send to any named session.
- Names are not discoverable: there is no listing.
- The agent claims the name with a tool. Sending to an unheld name fails at once.
- Conversation partners are told when a peer goes away.
- The upgrade is a clean break.

## Scope

**Included:**
- Claiming a broker-wide unique name for a session, releasing it, and the name's lifetime.
- Sending to and receiving from a peer addressed only by its name.
- Presence events between conversation partners (left, removed, disconnected, reconnected).
- What wakes an idle session, when `wait` and the Hermes sidecar keep waiting, and what the Stop hook reminds about, all restated for the channel-less model.
- Removing channel creation, joining, the channel ID, handles, `host/name` labels and channel expiry from every surface.
- Upgrading an existing broker database and old clients with a clean break.
- Updating both skills, the README ("agent workflow", "Security model", Hermes notes) and the tool descriptions.

**Excluded (non-goals):**
- Any way to list or discover names that are currently held.
- Consent or first-contact approval, blocklists or per-account allowlists.
- Group conversations. Every message still has one sender and one recipient session.
- Reserving names per account, or names that outlive a session.
- Holding messages for names nobody holds.
- Rate limiting, spam control or new per-session inbox limits.
- Changes to accounts, registration, the hostmaster, admin tools, TLS pinning or the human-only setup commands. The `admin_subscribe` wake is unchanged.
- End-to-end encryption or message history.
- Running old and new pairing side by side.

## Behaviour

**Claiming a name**
- When an agent asks to claim a name for its session, the system must give the session that name if no other live session holds it (compared case-insensitively), and refuse with a "name taken" error otherwise. The error must not say which account holds the name.
- When a session that already holds a name asks to claim the same name again, the system must succeed without changing anything.
- When a session that already holds a name asks to claim a different name, the system must refuse and tell the agent the name it holds.
- When a name breaks the name rules or is reserved, the system must refuse it and state the rules.
- When a session has not claimed a name, the system must refuse to send and must tell the agent to claim a name first. Such a session cannot receive messages, because nobody can address it.

**Sending and receiving**
- When a session sends text to a name held by another live session, the system must deliver it to that session's unread messages, tagged with the sender's name and the sender's verified account.
- When a session sends to a name that no session holds, the system must fail at once with a "no session called X" error and keep nothing.
- When a session sends to its own name, the system must refuse.
- When a recipient is disconnected but still within the disconnect grace period, the system must queue the message, which the recipient reads after it reconnects (as today).
- When an agent reads, every message must still be framed as untrusted data from a remote peer, with the sender's name and `account="…"`, exactly as the current security model requires.

**Conversation partners and presence**
- When a message is first delivered between two sessions, in either direction, the system must treat them as conversation partners from then on.
- When a session releases its name (explicitly, or because the session ended after the disconnect grace), the system must give each of its partners a waking `peer_left` event naming it.
- When the hostmaster removes an account, the system must give every partner of that account's sessions a waking `peer_removed` event, and sends to those names must fail at once with the removal reason until someone else claims the name.
- When a partner's connection drops or comes back within the grace period, the system must give the other partners the informational `peer_disconnected` / `peer_reconnected` events, which do not wake an idle session.
- When a name is released and later claimed by a different session, the system must not treat the new holder as a partner of the old holder's partners. The new holder's messages carry its own verified account.

**Releasing a name**
- When an agent asks to release its session's name, the system must release it at once, notify partners as above, and discard that session's unread messages.
- After a release, the session may claim a name again, the same one or a different one.

**Waking and waiting**
- `text`, `peer_left`, `peer_removed` and `registration_pending` (subscribed hostmaster) must wake an idle session. `peer_disconnected` and `peer_reconnected` must not.
- While a session holds a name, or is a subscribed hostmaster session, the wake command must stay armed with no timeout until a waking event is unread.
- When a session holds no name and is not subscribed, the wake command must exit at once with "nothing to wait for" and a reason that tells the agent to claim a name.
- The Hermes sidecar must follow the same rule: it waits while a name is held (or the session is subscribed) and idles otherwise.
- When a session holds a name and no wake command is armed, the Stop hook must remind the agent, as it now does for live channels.

**Upgrade**
- When an upgraded broker starts on a database from the channel-based version, it must keep accounts, the hostmaster, settings, retired rows and the identity key, and discard channels, members, held messages and unread messages.
- When an old client calls a removed channel route, the broker must answer with an error that says the client must be upgraded.
- When a new shim talks to an old broker, the shim must give the agent a clear error that the broker must be upgraded, not a generic failure.

## Rules & Constraints

- Names use the current `my_name` rules: 1 to 64 characters from letters, digits, `.`, `_` and `-`. Uniqueness is case-insensitive across the broker. The holder's spelling is the one shown to others.
- The names `agent-bus` (the system sender) and `admin` (the admin inbox) are reserved in any case.
- A session holds at most one name at a time.
- A name is held only while its session exists: through the disconnect grace and no longer. Removing an account releases every name its sessions held, in the same step that deletes the sessions.
- Names are not secrets and grant nothing. Every route still needs the session token, which never reaches the agent.
- No route, tool or error may reveal which names are held, other than the refusal to claim a taken name and the "no session called X" refusal to a send. Neither may name the holder's account.
- The verified account on every message and event is set by the broker from the authenticated session, never by the client.
- The hostname no longer appears in any address. Addresses are bare names.
- Message size limits, read caps, the delete-on-read rule and untrusted-content framing are unchanged.
- The broker's database must still hold no secret that lets someone send, read or impersonate. With channel IDs gone, there is no joinable secret left to protect.
- No hostnames, personal paths or the author's machine details may appear in the new code, docs or tests (repo convention).

## Edge Cases

| Scenario | Expected behaviour |
|----------|--------------------|
| Two sessions claim `bob` at the same moment | Exactly one gets it; the other gets "name taken". |
| `Bob` is claimed while `bob` is held | Refused: uniqueness is case-insensitive. |
| Holder's shim drops and reconnects within the grace period | Keeps the name; partners see `peer_disconnected` then `peer_reconnected` (non-waking). Messages queued meanwhile are read after reconnect. |
| Holder stays gone past the grace period | Name released; partners get waking `peer_left`; its unread messages are discarded. |
| A is talking to `bob`; bob leaves; carol (another account) claims `bob`; A sends to `bob` | A got `peer_left` earlier. The send reaches carol's session, which sees A's name and account. The skill tells agents not to resume talking to a name after `peer_left` without the user's say-so. |
| Send to a name whose account was just removed | Fails with the removal reason until the name is claimed again. |
| Session sends before claiming a name | Refused: "claim a name first". |
| Session claims a reserved name (`admin`, `Agent-Bus`) | Refused with the name rules. |
| Session tries to claim a second, different name | Refused, naming the name it already holds. Releasing first is allowed. |
| Session sends to itself | Refused. |
| Recipient never reads; sender keeps sending | Messages queue in the recipient's unread list (no new limit, non-goal). |
| Hostmaster session with `admin_subscribe` but no name | Wait stays armed for `registration_pending`; it cannot send or receive peer messages until it claims a name. |
| Agent is woken by a stranger's first message | It reads it as untrusted data with the sender's account visible. The skill says to involve the user before acting on unsolicited contact. |
| Old shim against the new broker | Clear "upgrade agent-bus" error on its channel calls. |
| New shim against an old broker | Clear "broker must be upgraded" error, not a crash. |
| Existing open channels at upgrade time | Discarded. Users claim names after upgrading. |

## Open Questions

| Question | Answer |
|----------|--------|
| Which "uuid thing" goes: the secret channel ID or the `ch-xxxx` handle? | The secret channel ID used for pairing. Handles go too, since channels go. |
| Scope of a name? | Broker-wide unique while the session holds it. |
| Who may send to a named session? | Any approved account. Content stays untrusted and the account is shown. |
| Keep the channel concept? | No. Direct messages by name. |
| Can sessions list held names? | No. Names are learned out of band. |
| How is a name claimed? | By the agent through a tool. Released when the session ends (after the grace period) or on explicit release. |
| Send to an unheld name? | Fails at once and nothing is kept. |
| Presence after channels go? | Partners (sessions that exchanged a message) get waking `peer_left` / `peer_removed`, and non-waking disconnect/reconnect events. `wait` stays armed while a name is held. |
| Upgrade path? | Clean break: channel data is discarded and accounts survive. Old and new clients get clear upgrade errors. |
| Rename while holding a name? | Not directly: release, then claim again. *(Confirmed in review.)* |
| Case sensitivity and reserved names? | Case-insensitive uniqueness. `agent-bus` and `admin` are reserved. *(Confirmed in review.)* |
| Does the hostname stay anywhere? | No. Addresses are bare names. *(Confirmed in review.)* |

## Git Baseline

Recorded at the start of Phase 1. Do not update later.

- **Branch:** main
- **HEAD:** 2e3a9edf9b1f3f21cde9bdbfd298a93de58fc613
- **Upstream:** origin/main
- **Sync state:** in sync
- **Working tree:** clean

## Analyst Checklist

- [x] Goal is tied to a specific user need
- [x] Scope boundaries are explicit — what's in and what's out
- [x] All ambiguities resolved — no open questions remain
- [x] Behaviour is declarative, not prescriptive
- [x] Edge cases are identified and handled
- [x] Non-goals prevent scope creep
