# Delta: Name addressing (drop the channel ID)

> Specification delta — what changes relative to the current system.
> Only exists when this feature modifies existing behaviour.

## ADDED

- A session claims one broker-wide unique name (case-insensitive) through an agent tool. A name that is taken, breaks the rules or is reserved is refused.
- A session can release its name explicitly. Partners are notified and the session's unread messages are discarded.
- Reserved names: `agent-bus` and `admin`.
- Conversation partners: two sessions become partners once a message is delivered between them. Presence events go to partners.
- "Claim a name first" refusal for sending without a name.
- "No session called X" refusal for sending to an unheld name. Nothing is kept.
- Upgrade errors: old clients on removed routes get "upgrade agent-bus", and a new shim on an old broker gets "broker must be upgraded".
- Database upgrade that keeps accounts, the hostmaster, settings, retired rows and the key, and discards channel data.

## MODIFIED

- **Pairing:** session A creates a channel and a human copies the secret UUID channel ID to session B, which joins → **each session claims a name, and any session sends to any held name with no human-carried secret.**
- **Addressing:** `peer` is a bare name resolved through the caller's own channels, or a full `host/name` label on ambiguity, or a `ch-xxxx` handle → **`peer` is a bare broker-wide name, resolved against the names held on the broker. There is no ambiguity case and no handle.**
- **Labels:** self-reported `host/name`, which can collide between users → **a bare name, unique while held. The hostname appears nowhere.**
- **Who can reach a session:** only the holder of that session's channel ID → **any approved account that knows the name.**
- **`peer_left`:** the peer left a channel → **a partner released its name (explicitly, or because its session ended after the grace period).**
- **`peer_removed`:** the peer's account was removed and the channel ended → **a partner's account was removed. Sends to that name fail with the removal reason until the name is claimed again.**
- **`peer_disconnected` / `peer_reconnected`:** sent to the channel peer → **sent to partners. Still non-waking.**
- **Waking kinds:** `text`, `peer_joined`, `peer_left`, `peer_removed`, `channel_expired`, `registration_pending` → **`text`, `peer_left`, `peer_removed`, `registration_pending`.**
- **`wait` / Hermes sidecar "nothing to wait for":** the session has no live channels and is not subscribed → **the session holds no name and is not subscribed. The reason tells the agent to claim a name.**
- **Stop hook unarmed-wake reminder:** fires when there are live channels → **fires when a name is held** (or when subscribed, as today).
- **Sessions ending after the disconnect grace:** their channels end and peers get `peer_left` → **their name is released and partners get `peer_left`.**
- **Account removal:** sessions deleted, channels ended, peers get `peer_removed` → **sessions deleted, names released, partners get `peer_removed`.**
- **Security model ("Channels"):** "the channel ID is the secret" → **names are not secrets. Access rests on hostmaster-approved accounts. Any account can message any held name, the recipient sees the sender's verified account, and content stays untrusted. First-come names can be squatted by other users.**
- **Account visibility:** account names are seen only for channels you are in → **account names are seen on messages and events you receive. The only probes of held names are a refused claim and a refused send, and neither reveals the holder's account.**
- **Skills / README / tool descriptions:** the channel-ID workflow → **the claim-a-name workflow. After `peer_left`, the agent asks the user before messaging that name again. Unsolicited first contact is surfaced to the user.**
- **Tool set:** `create_channel`, `join_channel`, `send`, `read`, `leave`, `wake_command` → **a tool to claim a name, `send`, `read`, a tool to release the name, and `wake_command`.** Names and arguments are for Phase 2 to decide.

## REMOVED

- The secret UUID channel ID, creating it, showing it to the human and joining with it.
- Channels as a concept: channel states (open, joined, ended, expired, removed), channel handles (`ch-xxxx`), the 1-hour TTL for an unjoined channel and the `channel_expired` event.
- Holding messages sent before a peer joins.
- The `peer_joined` event.
- `host/name` labels and the "two peers share a name, use the full label" ambiguity error.
- The generic "no such open channel" join error and its security rationale.
- The broker's storage of channel ID hashes.
- Existing channel, member, held and unread data at upgrade time.
