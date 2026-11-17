# havn-channel-telegram

A daemon that connects a Telegram bot to a [Havn](https://github.com/amplimit/havn) gateway: people message the bot in Telegram, and the Havn agent bound to it answers there.

It runs as its own process, apart from the gateway, and talks to it over the gateway's channel protocol (a WebSocket at `/api/v1/channel`). It holds the bot token and an adapter token; it cannot read the gateway's credentials or reach the agents. Stopping it stops the bot and nothing else.

## What it does

- **Private chats** with the bot (1:1). Messages in groups, supergroups and channels are dropped.
- **Text**, and **voice messages** transcribed with an OpenAI-compatible Whisper API when one is configured; otherwise, or when transcription fails, the voice message reaches the agent as an audio file with a note saying why.
- **Locations, venues and contacts**, passed to the agent as such.
- **Stickers** (their emoji and set) and **polls** (the question and options), as text.
- **Files both ways.** A photo (its largest size), a document, a video, a video note, an audio file or an animation someone sends is downloaded from Telegram and saved in the agent's inbox by the gateway, which also reads the text of a PDF, Word document or spreadsheet; the caption comes with it. A file the agent sends is posted with its caption; a caption over Telegram's 1024 characters goes on in a message after the file. Telegram lets bots download files of up to 20 MB; the gateway takes up to 25 MB. A file that cannot be passed on is named in the message, so the agent can say so.
- **A typing indicator** when a message arrives (Telegram shows it for about 5 seconds).
- **The command menu.** The bot's `/` menu is set from the commands the gateway offers (`/new`, `/stop`, `/retry`, `/model`, ...), whenever the daemon connects.
- **Long replies** are split into several messages at paragraph, line or word breaks, in order, under Telegram's 4096 characters each.
- **Nothing is dropped silently:** any other kind of message (a game, a dice roll, a story) reaches the gateway as a note, so a sender who is not paired is answered and the agent can tell one who is.
- **Long polling** (`getUpdates`): no public address, domain or certificate is needed.

Who may use the bot, what the commands do and how chats work is decided by the gateway: see [Channel Adapters](https://github.com/amplimit/havn#channel-adapters), [Chat commands](https://github.com/amplimit/havn#chat-commands) and [Invite codes](https://github.com/amplimit/havn#invite-codes) in the Havn README.

## Requirements

- A Havn gateway, 0.2.2 or later for the command menu (`havn --version`).
- A Telegram bot: create one with [@BotFather](https://t.me/BotFather) (`/newbot`) and keep its token (`1234567890:AAH...`).
- Rust, to build the daemon.

## Setup

### 1. Declare the bot's account in the gateway

In the gateway's `config.toml` (`~/.config/havn/config.toml` for the user the gateway runs as, `/root/.config/havn/config.toml` under the systemd unit), add the account and bind it to an agent:

```toml
[[channels.telegram.accounts]]
id                = "acme-bot"
adapter_token_ref = "secret:channel:telegram:acme-bot"
dm_policy         = "allowlist"          # or "open": anyone who finds the bot
allow_from        = ["tg:123456789"]     # Telegram user ids allowed to talk to it
admins            = ["tg:123456789"]     # may use the admin commands (/model, /approve, ...)

[[bindings]]
agent_id   = "<id of the agent that answers, from `havn agent list`>"
channel    = "telegram"
account_id = "acme-bot"
```

A sender id is `tg:` and the person's numeric Telegram user id (bots such as @userinfobot show it). Instead of listing everyone in `allow_from`, you can hand out one-time invite codes; see below.

Changes to `[[channels]]` and `[[bindings]]` apply when the gateway restarts: `systemctl restart havn` under the systemd unit, `havn gateway restart` for a gateway started with `havn gateway start`.

### 2. Give the gateway and the daemon a shared adapter token

```bash
ADAPTER_TOKEN="$(openssl rand -base64 32 | tr -d '\n')"
havn credential add channel:telegram "$ADAPTER_TOKEN" --name acme-bot
```

The `--name` is the account id, so the gateway finds it as `secret:channel:telegram:acme-bot`. Keep the token for the daemon's config.

### 3. Build and install the daemon

```bash
git clone https://github.com/amplimit/havn-channel-telegram
cd havn-channel-telegram
cargo build --release
sudo install -m 755 target/release/havn-channel-telegram /usr/local/bin/
```

### 4. Configure the daemon

Create a user for it, and its config readable by that user only (it holds both tokens):

```bash
sudo useradd --system --no-create-home --shell /usr/sbin/nologin havn-tg
sudo install -d -m 755 /etc/havn-channel-telegram
sudo install -m 600 -o havn-tg -g havn-tg config.example.toml /etc/havn-channel-telegram/config.toml
sudoedit /etc/havn-channel-telegram/config.toml
```

```toml
bot_token     = "1234567890:AAH..."         # from BotFather
havn_gateway  = "ws://127.0.0.1:8080"       # the gateway's `listen` address; wss:// behind TLS
adapter_token = "<the ADAPTER_TOKEN above>"
channel_id    = "telegram"                  # as in [[channels.telegram.accounts]]
account_id    = "acme-bot"                  # the account's id

# Seconds Telegram holds a long poll open (at most 50; default 25).
poll_timeout_seconds = 25

# Voice messages, transcribed with an OpenAI-compatible Whisper API.
# Without a key, voice messages are dropped.
# openai_api_key   = "..."
# whisper_base_url = "https://api.groq.com/openai/v1"   # default: OpenAI
# whisper_model    = "whisper-large-v3-turbo"           # default: whisper-1
```

### 5. Run it under systemd

```ini
# /etc/systemd/system/havn-channel-telegram.service
[Unit]
Description=havn channel adapter for Telegram
After=network.target havn.service
Wants=havn.service

[Service]
ExecStart=/usr/local/bin/havn-channel-telegram --config /etc/havn-channel-telegram/config.toml
Restart=always
RestartSec=5
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
sudo systemctl enable --now havn-channel-telegram
sudo journalctl -u havn-channel-telegram -f
```

### 6. Let people in

With `dm_policy = "allowlist"`, only senders in `allow_from` and senders admitted with an invite code may use the bot. Create a code on the gateway host and give it to the person; they send it to the bot once. These commands open the gateway's database through the config of the user who runs them, so run them as the user the gateway runs as (root under the systemd unit: `sudo -i` first):

```bash
havn invite create --channel telegram --account acme-bot              # expires in 24h
havn access list --channel telegram --account acme-bot                # who may talk to it
havn access revoke --channel telegram --account acme-bot tg:123456789 # take it back
```

## Check that it works

The log shows the connection to the gateway:

```
INFO havn_channel_telegram: havn WS connected
```

Then message the bot in Telegram; the bound agent answers, and typing `/` shows its commands.

If it does not answer:

1. `havn doctor` checks the gateway; `havn agent list` shows whether the bound agent exists and runs (as the gateway's user, as above).
2. In the daemon's log, a refused connection means the adapter token does not match the credential, or `channel_id` / `account_id` do not match an account in the gateway's config.
3. In the gateway's log (`journalctl -u havn`), a message from a sender who is not allowed is refused there: add them to `allow_from` or give them an invite code.

## Limitations

- Private chats only; groups, supergroups and channels are dropped.
- An edited message is not passed on: the agent answered the original, and acting on edits would repeat its work. Send a new message instead.
- Replies the agent sends while the daemon is disconnected from the gateway are lost: the gateway keeps them for 5 minutes, but this daemon does not ask for them yet when it reconnects.
- Images in a reply that are not files the agent sent are shown as text (`[image: <url>]`).
- Long polling only, no webhook.

## License

Apache License 2.0; see [LICENSE](LICENSE).
