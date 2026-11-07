# Delta: Multi-User Trusted Broker

> Specification delta — what changes relative to the current system.

## ADDED

- Accounts, a hostmaster role, and manual approval/rejection of registration requests.
- One-time claim code printed by the broker at first start; required to register the first (hostmaster) account.
- Hostmaster administration as agent tools for hostmaster-account sessions: list accounts and pending requests, approve, reject, revoke/remove, maximum account count.
- Opt-in administrative notification subscription for hostmaster-account sessions; events wake the subscribed session and are framed as untrusted data.
- A channel-ended reason stating the peer's account was removed by the hostmaster.
- Pending-registration limit (5) and expiry (2 hours), registration status check, and a host-side hostmaster recovery command that issues a fresh claim code.
- Per-account credential held by the shim (hash-only on the broker); sessions attributed to an account.
- Broker-verified account name shown as a separate field on peers and in `read` framing.
- Encrypted client↔broker transport with a host fingerprint pinned on first contact; hard refusal on mismatch and a human-only re-pin.
- A separate hostmaster skill for admin tools and notifications; the sync-with-code check in CLAUDE.md extends to it.
- Rule that registration, claiming and fingerprint acceptance/re-pinning are human actions, unavailable to agents.

## MODIFIED

- **Broker has no accounts; the network is the perimeter** → **Every session must authenticate under an approved account; an unclaimed broker serves nothing.**
- **Peers are known by label only (self-reported, display and addressing)** → **Peers also carry a broker-verified account name; the label still grants no access.**
- **Transport encryption is whatever the operator provides (VPN, LAN)** → **The broker requires an encrypted connection and authenticates itself by fingerprint.**
- **Security model in README ("no accounts", "no encryption beyond the transport you provide")** → **Updated to describe the hostmaster model, claim code, TOFU pinning and the residual first-contact risk.**
- **The shim accepts `http://` broker URLs and adds `http://` to a bare `host:port`** → **The default scheme is `https://`, `http://` is refused with a message telling the human to switch, and existing MCP/Hermes configs must be updated (see Architect "Upgrade and Rollback").**
- **`POST /v1/sessions` needs no authentication** → **It requires the account credential as a bearer value.**
- **Joining requires only the secret channel ID** → **Joining also requires an active account; the generic "no such open channel" error is unchanged.**
- **Event kinds that wake an idle session: `text`, `peer_joined`, `peer_left`, `channel_expired`** → **Plus an administrative event kind for subscribed hostmaster sessions.**

## REMOVED

- Single-user (no-account) mode. Existing deployments must be claimed before they serve again, and sessions/channels from before the upgrade are not carried over.
