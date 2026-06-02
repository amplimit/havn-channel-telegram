# havn-channel-telegram

Channel adapter daemon that bridges **Telegram Bot API** ↔ **havn's
`/api/v1/channel` WebSocket protocol** ([spec §3](https://github.com/amplimit/havn/blob/main/platform-spec.md)).
A user DMs a Telegram bot; havn agent replies. That's it.

This is an **external repo** — it doesn't depend on the havn workspace
and ships independently. havn-core has zero Telegram-aware code; the
adapter does the platform-specific translation in its own process,
in its own trust zone (a compromised adapter daemon cannot read the
gateway's credential store or escape into agent namespaces).

## What this is, what it isn't

- ✅ A standalone Rust daemon. One config file, one binary, runs forever
  reconnecting on transient failures.
- ✅ Works with **any** Telegram bot you create via `@BotFather`. No
  webhook setup required — uses long-poll `getUpdates`.
- ✅ Bidirectional in v0.2: user → agent → user round-trips.
- ❌ Not a havn-core feature. Removing this pack is `systemctl stop`
  + `rm` of the binary; havn keeps running unaffected.
- ❌ Not (yet) supporting Telegram groups, supergroups, or channels.
  v0.2 is 1:1 DMs. Group-chat support adds member-disambiguation
  semantics that don't fit cleanly into the (account, sender) model.

## Prerequisites

1. A havn gateway running v0.8+ (channel API ships there). Verify
   with `havn version` — must be ≥ 0.8.0.
2. A Telegram bot. Create one with `@BotFather` (`/newbot`); save the
   token (looks like `1234567890:AAH...`).
3. havn config (`gateway.toml`) declaring the channel account:

   ```toml
   [[channels.telegram.accounts]]
   id                = "alice-tg-bot"
   adapter_token_ref = "secret:channel:telegram:alice-tg-bot"
   dm_policy         = "allowlist"   # or "open" / "owner_only"
   allow_from        = ["tg:123456789"]   # your Telegram user id

   [[bindings]]
   agent_id   = "<UUID of the havn agent that should answer DMs>"
   channel    = "telegram"
   account_id = "alice-tg-bot"
   ```

4. An adapter token in havn's credential store, stable across daemon
   restarts:

   ```bash
   # Generate a strong random token
   ADAPTER_TOKEN="$(openssl rand -base64 32 | tr -d '\n')"
   # Store it in havn (age-encrypted at rest)
   havn credential add channel:telegram \
       --name alice-tg-bot \
       --token "$ADAPTER_TOKEN"
   # Save the same token for the daemon's config (see below)
   echo "$ADAPTER_TOKEN" > /etc/havn-channel-telegram/adapter-token
   chmod 600 /etc/havn-channel-telegram/adapter-token
   ```

## Install

```bash
git clone https://github.com/amplimit/havn-channel-telegram /tmp/havn-channel-telegram
cd /tmp/havn-channel-telegram
cargo build --release
sudo install -m 755 target/release/havn-channel-telegram /usr/local/bin/
```

## Config

```toml
# /etc/havn-channel-telegram/config.toml
bot_token       = "1234567890:AAH..."     # from BotFather
havn_gateway    = "ws://127.0.0.1:8100"   # or wss:// for TLS
adapter_token   = "<paste the same token you ran `havn credential add` with>"
channel_id      = "telegram"               # must match `[[channels.<channel_id>.accounts]]`
account_id      = "alice-tg-bot"           # must match the account `id` above

# Optional. Telegram allows up to 50; default 25.
poll_timeout_seconds = 25
```

Make the file root-owned and operator-readable only — the bot token
and adapter token are both secrets:

```bash
sudo chown root:root /etc/havn-channel-telegram/config.toml
sudo chmod 600 /etc/havn-channel-telegram/config.toml
```

## Run

```bash
havn-channel-telegram --config /etc/havn-channel-telegram/config.toml
```

For systemd, drop a unit file:

```ini
# /etc/systemd/system/havn-channel-telegram.service
[Unit]
Description=havn channel adapter for Telegram
After=network.target havn-gateway.service
Wants=havn-gateway.service

[Service]
ExecStart=/usr/local/bin/havn-channel-telegram --config /etc/havn-channel-telegram/config.toml
Restart=always
RestartSec=5
# Don't run as root in production — create a dedicated user with no
# shell and no home directory; chown the config file to it.
User=havn-tg
NoNewPrivileges=true
ProtectSystem=strict
ProtectHome=true
PrivateTmp=true

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now havn-channel-telegram.service
sudo journalctl -u havn-channel-telegram -f
```

## Verify

DM your bot from Telegram. The daemon's log shows:

```
INFO havn_channel_telegram: havn WS connected
INFO havn_channel_telegram: havn-channel-telegram starting
```

Then when you send `hello`:

```
DEBUG havn_channel_telegram: telegram inbound (translated to ChannelInbound)
DEBUG havn_channel_telegram: havn outbound (sending Telegram sendMessage)
```

The bot replies in Telegram with whatever the bound havn agent says.

If the bot never replies:

1. **`havn doctor`** — confirms the gateway is up and credentials decrypt.
2. **`havn agent status <agent_id>`** — confirms the bound agent is running.
3. Check the daemon log for `havn WS: connect failed` (auth issue —
   adapter_token mismatch with what's in `havn credential add`) or
   `dropping inbound: no [[bindings]] entry` (binding misconfigured).

## How this fits the havn architecture

havn's spec §3 separates inbound message ingest and outbound delivery
from the gateway core via a stable HTTP+WS protocol. This daemon
implements the **adapter** half of that contract:

```
   Telegram user ────┐
                     │ messages
                     ▼
       Telegram Bot API (api.telegram.org)
                     │
                     │ getUpdates (long-poll)
                     ▼
       ┌───────── havn-channel-telegram ─────────┐
       │  - translate Telegram Update → ChannelInbound│
       │  - translate ChannelOutbound → Telegram sendMessage│
       └─────────────────┬──────────────────────┘
                         │ WebSocket /api/v1/channel
                         │ (X-Adapter-Token header)
                         ▼
       ┌─────────── havn gateway ─────────────┐
       │  - resolve binding (channel, account_id) → agent_id│
       │  - forward to agent runtime over Unix socket       │
       └─────────────────┬─────────────────────┘
                         ▼
                   havn agent (in its namespace)
```

Adapter ↔ gateway is a **stable protocol**, not an unstable internal
ABI. A future Slack or Discord adapter is a separate repo
implementing the same protocol; havn-core stays unchanged.

## Limitations (v0.2)

- **Group / supergroup / channel chats are dropped** with a debug log.
- **Image / file content from agent replies is flattened to text**
  (`[image: <url>]`). Telegram has dedicated `sendPhoto` / `sendDocument`
  endpoints; we'll wire them when an agent emits non-text content in
  practice.
- **Reconnect drops in-flight messages.** havn's outbound replay
  buffer (spec §3.2 — last 5 minutes per account) lands in a
  follow-up — Layer D2. Today, when the daemon reconnects after a
  disconnect longer than the round-trip, any agent reply emitted
  during the gap is dropped at the gateway.
- **No webhook mode.** Long-poll only. Webhook (Telegram pushes to a
  public TLS endpoint) lowers latency but requires public DNS + cert;
  out of scope for self-host first cut.

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE).
