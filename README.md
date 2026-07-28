# WhatsApp Connection Service

A small, self-hosted HTTP service that keeps a **persistent WhatsApp Web connection** alive (via [Baileys](https://github.com/WhiskeySockets/Baileys)) and exposes it over a plain **REST API**: QR pairing, health/status, sending messages, basic group management, and forwarding inbound messages to your own webhook.

It is built to survive the messy realities of a long-lived WhatsApp socket: an **auto-reconnect loop** with exponential backoff, an in-process **watchdog** that detects a silently-dropped ("zombie") socket and re-establishes it, and explicit handling of WhatsApp's disconnect reasons (`401` logged-out, `405` blocked, `440` conflict, `515` restart-required, `408` timeout).

Runs on anything that supports long-lived WebSocket connections — Railway, Render, Fly.io, a VPS, or your laptop.

> **Heads up:** this uses an unofficial WhatsApp Web library. It is not affiliated with or endorsed by WhatsApp/Meta. Automating a WhatsApp account carries a risk of being rate-limited or banned — use a number you control and understand the risk.

## Features

- 🔌 **Persistent socket** with multi-file auth state persisted to disk (point `AUTH_DIR` at a persistent volume).
- ♻️ **Auto-reconnect** with exponential backoff and disconnect-reason awareness.
- 🐶 **Watchdog** that forces a reconnect when the socket goes silent past a threshold — only during configurable active hours, so it stays quiet overnight.
- 📤 **Send** text or image messages to groups or direct chats, with optional quoted replies and @mentions.
- 👥 **Group helpers**: join via invite link, resolve invite → JID, add participants, approve/reject join requests.
- 📥 **Inbound webhook**: every incoming message is POSTed to your backend as JSON.
- 🩺 **Diagnostics**: `/health`, `/liveness`, `/status`, and an SSE `/events` stream.

## Quick start

```bash
npm install
cp .env.example .env    # then edit .env
npm start
```

Then pair your number:

1. `POST /init` (or just start the service — it auto-inits unless `WHATSAPP_AUTO_INIT=0`).
2. `GET /qr` to fetch the current QR code, and scan it from **WhatsApp → Linked devices**.
3. `GET /status` should now report `connected: true`. Credentials are saved under `AUTH_DIR` and reused on restart.

## Configuration

All configuration is via environment variables. See [`.env.example`](.env.example) for the full list with defaults.

| Variable | Default | Description |
|---|---|---|
| `PORT` | `3001` | HTTP port (most PaaS hosts assign this automatically). |
| `AUTH_DIR` | `./whatsapp_auth` | Where Baileys stores session credentials. **Use a persistent volume in production** or you'll have to re-scan the QR after every deploy. |
| `WHATSAPP_WEBHOOK_URL` | — | URL that inbound messages are POSTed to. If unset, inbound forwarding is skipped. |
| `WHATSAPP_WEBHOOK_SECRET` | — | Sent as the `x-whatsapp-secret` header on every webhook POST so your backend can verify the caller. |
| `ALLOWED_ORIGINS` | `*` | Comma-separated CORS allowlist. |
| `WHATSAPP_AUTO_INIT` | `1` | Set `0` to not auto-connect on startup (connect manually via `POST /init`). |
| `BAILEYS_LOG_LEVEL` | `error` | Pino level for Baileys internals. Kept restrictive to avoid leaking session internals into logs. |
| `WHATSAPP_LOG_INCOMING_GROUP_MESSAGES` | `0` | Debug: log inbound group messages. |
| `WHATSAPP_LOG_OWN_MESSAGES` | `0` | Debug: log your own/outbound messages. |

**Watchdog** (all optional):

| Variable | Default | Description |
|---|---|---|
| `WHATSAPP_WATCHDOG_PAUSED` | `0` | Set `1` to disable automatic reconnects entirely. |
| `WHATSAPP_WATCHDOG_INTERVAL_MS` | `60000` | How often the watchdog checks. |
| `WHATSAPP_WATCHDOG_MAX_SILENCE_MS` | `900000` | Silence (no inbound message) before a "zombie" socket is force-reconnected. |
| `WHATSAPP_WATCHDOG_COOLDOWN_MS` | `300000` | Base cooldown between forced reconnects (doubles each consecutive recovery). |
| `WHATSAPP_WATCHDOG_MAX_COOLDOWN_MS` | `3600000` | Cap for the exponential backoff. |
| `WHATSAPP_WATCHDOG_QUIET_RESET_MS` | `7200000` | Stability window after which the backoff counter resets. |
| `WHATSAPP_WATCHDOG_ACTIVE_START` | `7` | Start of the active-hours window (local hour). |
| `WHATSAPP_WATCHDOG_ACTIVE_END` | `24` | End of the active-hours window (`24` = through midnight). |
| `WHATSAPP_WATCHDOG_UTC_OFFSET` | `0` | Whole hours east of UTC for the active-hours window (e.g. `-5` US Eastern, `+1` Central Europe). |

## API

### Connection & diagnostics

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/` · `/health` | Basic health check (reports `connected`). |
| `GET` | `/liveness` | Honest liveness: timestamp of the last real inbound message. |
| `GET` | `/status` | Detailed connection status. |
| `GET` | `/events` | Server-Sent Events stream of connection state changes. |
| `GET` | `/qr` | Current QR code (while pairing). |
| `POST` | `/init` | Initialize the socket / (re)generate a QR. |
| `POST` | `/force-reconnect` | Tear down and re-establish the socket. |
| `POST` | `/clear-session` | Delete saved auth (logs out; a new QR scan is required). |
| `POST` | `/cleanup-auth` | Prune old Signal protocol key files from `AUTH_DIR`. |

### Messaging

| Method | Path | Body |
|---|---|---|
| `POST` | `/send` | `{ groupId, message?, imageUrl?, quotedMessageId?, quotedText?, quotedParticipant?, quotedRemoteJid?, mentions? }` — `message` or `imageUrl` required. |
| `POST` | `/send-direct` | `{ senderJid, message?, imageUrl?, quotedMessageId?, quotedText?, quotedParticipant?, quotedRemoteJid? }` |
| `POST` | `/revoke-message` | `{ remoteJid, messageId, participant? }` — delete-for-everyone (only your own messages, within WhatsApp's window). |
| `POST` | `/resolve-lid` | `{ lid, groupId? }` — resolve a privacy `@lid` to its phone JID. |

### Groups

| Method | Path | Body |
|---|---|---|
| `POST` | `/group/join` | `{ groupUrl }` — join via an invite link. |
| `POST` | `/group/get-jid` | `{ groupUrl }` — resolve an invite link to a group JID. |
| `POST` | `/group/add-participant` | `{ groupId, userJid }` |
| `POST` | `/group/approve-join` | `{ groupId, userJid }` |
| `POST` | `/group/reject-join` | `{ groupId, userJid }` |
| `POST` | `/group/pending-requests` | `{ groupId }` |

### Inbound webhook payload

Every inbound message is POSTed to `WHATSAPP_WEBHOOK_URL` (with the `x-whatsapp-secret` header). Group messages:

```json
{
  "messageId": "…",
  "groupId": "1203…@g.us",
  "groupName": "My group",
  "senderJid": "5215…@s.whatsapp.net",
  "senderName": "Alice",
  "text": "hello",
  "quotedText": null,
  "quotedMessageId": null,
  "quotedSenderJid": null,
  "timestamp": "2026-01-01T00:00:00.000Z",
  "type": "notify"
}
```

Direct messages use `groupId: "direct_<senderJid>"`, `groupName: "Direct Message"`, and add `senderJidAlt` (the sender's phone JID, resolved from a privacy `@lid` when needed).

## How reconnection works

- **Disconnect reasons** are handled explicitly on `connection.close`: `401` (logged out) clears the session and stops; `405` (blocked) stops auto-reconnect and waits; `515` (restart-required, expected right after pairing) restarts quickly; transient drops (`440` conflict, `408` timeout) back off and retry. The service is careful **not** to spawn a competing socket while one is already connecting — that race is exactly what produces the `440` conflict that breaks pairing.
- **The watchdog** runs independently: if the socket claims `open` but no message has arrived in `WHATSAPP_WATCHDOG_MAX_SILENCE_MS`, or the socket isn't initialized at all, it forces a reconnect — but only inside the active-hours window, and with exponential backoff so a bad night doesn't turn into a reconnect storm.

## Deployment notes

- **Bind `0.0.0.0`** and honor the assigned `PORT` (the service already does this).
- **Persist `AUTH_DIR`** on a volume. On ephemeral filesystems (e.g. a container's `/tmp`) you lose the session — and the QR — on every deploy.
- Keep `BAILEYS_LOG_LEVEL=error` in production to avoid logging cryptographic/session internals.

## License

MIT — see [LICENSE](LICENSE).
