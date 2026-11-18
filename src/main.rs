//! `havn-channel-telegram` — bridges Telegram Bot API ↔ havn's
//! `/api/v1/channel` WebSocket protocol (spec §3).
//!
//! Two long-running tasks plus one connection-management task in
//! [`havn_ws`]:
//!
//! - **Telegram inbound**: `getUpdates` long-poll loop. Each new
//!   user message becomes a [`types::ChannelFrame::Inbound`] sent
//!   to havn over the WS.
//! - **havn outbound**: drain WS-received [`types::ChannelFrame::
//!   Outbound`] frames and call Telegram's `sendMessage` to
//!   deliver to the right user.
//!
//! Group/supergroup/channel messages are dropped with a debug log —
//! v0.2 supports 1:1 DMs only. Non-text content is also dropped on
//! the outbound side; agents that emit `Image` / `File` won't see a
//! delivery error today (the WS `deliver` succeeds gateway-side, the
//! daemon just doesn't translate). Add when a real user asks.

#![forbid(unsafe_code)]

mod config;
mod gateway;
mod havn_ws;
mod markdown;
mod telegram;
mod types;
mod whisper;

use std::collections::HashMap;
use std::fmt::Write as _;
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context as _, Result};
use clap::Parser;
use tracing::{debug, info, warn};
use tracing_subscriber::EnvFilter;

use crate::config::Config;
use crate::gateway::Gateway;
use crate::havn_ws::{WsClient, WsConfig};
use crate::telegram::Telegram;
use crate::types::{
    Attachment, ChannelFrame, ChannelInbound, ChannelMessageContent, ChannelOutbound,
};
use crate::whisper::Whisper;

#[derive(Debug, Parser)]
#[command(
    name = "havn-channel-telegram",
    about = "havn channel adapter for Telegram (spec §3)",
    version
)]
struct Cli {
    /// Path to the daemon's TOML config file. See README for the
    /// schema. Defaults to /etc/havn-channel-telegram/config.toml
    /// for system installs; ./config.toml otherwise.
    #[arg(
        long,
        env = "HAVN_TG_CONFIG",
        default_value = "/etc/havn-channel-telegram/config.toml"
    )]
    config: PathBuf,
}

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();
    let cli = Cli::parse();
    let cfg = Config::load(&cli.config)
        .with_context(|| format!("loading config from {}", cli.config.display()))?;
    info!(
        config = %cli.config.display(),
        gateway = %cfg.havn_gateway,
        channel = %cfg.channel_id,
        account = %cfg.account_id,
        "havn-channel-telegram starting"
    );

    let telegram =
        Telegram::new(&cfg.bot_token, cfg.poll_timeout_seconds).context("Telegram client init")?;
    let whisper = match cfg.openai_api_key.as_ref() {
        Some(k) if !k.is_empty() => Some(
            Whisper::new(
                k.clone(),
                cfg.whisper_base_url.clone(),
                cfg.whisper_model.clone(),
            )
            .context("Whisper init")?,
        ),
        _ => {
            info!("no openai_api_key configured — voice messages will be dropped");
            None
        }
    };
    let gateway = Gateway::new(
        &cfg.havn_gateway,
        &cfg.adapter_token,
        &cfg.channel_id,
        &cfg.account_id,
    )
    .context("havn gateway client init")?;
    let typing = {
        let tg = telegram.clone();
        Typing::new(move |chat_id| {
            let tg = tg.clone();
            Box::pin(async move { tg.send_typing(chat_id).await })
        })
    };
    let ws = WsClient::spawn(WsConfig {
        gateway_url: cfg.havn_gateway.clone(),
        adapter_token: cfg.adapter_token.clone(),
        channel_id: cfg.channel_id.clone(),
        account_id: cfg.account_id.clone(),
    });

    let WsClient {
        outbound_tx,
        inbound_rx,
    } = ws;

    // Inbound (Telegram → havn) and outbound (havn → Telegram) tasks
    // run concurrently. Either error is logged but doesn't tear down
    // the daemon — the WsClient already reconnects on its own, and
    // Telegram getUpdates is naturally restartable.
    let inbound_task = tokio::spawn(telegram_inbound_loop(
        telegram.clone(),
        whisper.clone(),
        gateway.clone(),
        outbound_tx.clone(),
        cfg.account_id.clone(),
        typing.clone(),
    ));
    let outbound_task = tokio::spawn(havn_outbound_loop(
        telegram.clone(),
        gateway,
        inbound_rx,
        typing,
    ));

    // Wait for shutdown signal. Dropping `outbound_tx` triggers
    // graceful WS close + connection_loop exit.
    tokio::select! {
        _ = tokio::signal::ctrl_c() => info!("received Ctrl-C, shutting down"),
        r = inbound_task => warn!(result = ?r, "telegram inbound loop exited unexpectedly"),
        r = outbound_task => warn!(result = ?r, "havn outbound loop exited unexpectedly"),
    }
    drop(outbound_tx);
    Ok(())
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,havn_channel_telegram=debug"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(true)
        .init();
}

/// Telegram → havn. Long-poll forever, translating each `Update.message`
/// into `ChannelFrame::Inbound`. Per-process monotonic seq counter; spec
/// §3.2 says inbound seq is adapter-side bookkeeping (gateway logs
/// duplicates but doesn't enforce), so a per-process counter is
/// sufficient.
async fn telegram_inbound_loop(
    tg: Telegram,
    whisper: Option<Whisper>,
    gateway: Gateway,
    outbound_tx: tokio::sync::mpsc::Sender<ChannelFrame>,
    account_id: String,
    typing: Typing,
) -> Result<()> {
    let seq = Arc::new(AtomicU64::new(1));
    let mut offset: i64 = 0;
    let mut albums = Albums::default();
    loop {
        // While an album waits for its parts, polls are brief, so it goes
        // on soon after its last part.
        let updates = match tg.get_updates(offset, !albums.is_empty()).await {
            Ok(u) => u,
            Err(e) => {
                warn!(error = %e, "telegram getUpdates failed; backing off 5s");
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                Vec::new()
            }
        };
        // Messages to pass on, in order: one each, or an album's parts.
        let mut ready: Vec<Vec<telegram::Message>> = Vec::new();
        for update in updates {
            offset = update.update_id + 1;
            let Some(msg) = update.message else {
                continue;
            };
            let private = msg.chat.kind == "private";
            // Typing shows from before translate (which may do STT) until
            // the reply comes (issue #12); only for private chats, as
            // group messages are dropped by translate. An album is one
            // message, with one reply.
            let later_part = msg
                .media_group_id
                .as_deref()
                .is_some_and(|group| albums.holds(msg.chat.id, group));
            if private && !later_part {
                typing.start(msg.chat.id);
            }
            match msg.media_group_id.clone() {
                Some(group) if private => {
                    if let Some(album) = albums.add(group, msg, std::time::Instant::now()) {
                        ready.push(album);
                    }
                }
                _ => {
                    // An album before this message goes first.
                    if private {
                        ready.extend(albums.take_chat(msg.chat.id));
                    }
                    ready.push(vec![msg]);
                }
            }
        }
        ready.extend(albums.take_quiet(std::time::Instant::now()));
        for msgs in ready {
            let chat_id = msgs[0].chat.id;
            let frame = translate(&msgs, &tg, whisper.as_ref(), &gateway, &account_id, &seq).await;
            // A service message gets no reply.
            if frame.is_none() {
                typing.replied(chat_id);
            }
            if let Some(f) = frame
                && outbound_tx.send(f).await.is_err()
            {
                info!("outbound channel closed; telegram inbound loop exiting");
                return Ok(());
            }
        }
    }
}

/// How long an album waits for another part, and the most parts it has
/// (Telegram's limit).
const ALBUM_QUIET: Duration = Duration::from_millis(1500);
const ALBUM_PARTS: usize = 10;

/// Album parts waiting for the rest of their album (issue #14): Telegram
/// sends each part as its own message, one right after another, with the
/// album's `media_group_id`.
#[derive(Default)]
struct Albums {
    waiting: Vec<Album>,
}

struct Album {
    chat_id: i64,
    group: String,
    parts: Vec<telegram::Message>,
    last: std::time::Instant,
}

impl Albums {
    fn is_empty(&self) -> bool {
        self.waiting.is_empty()
    }

    /// Whether parts of album `group` of `chat_id` wait already.
    fn holds(&self, chat_id: i64, group: &str) -> bool {
        self.waiting
            .iter()
            .any(|a| a.chat_id == chat_id && a.group == group)
    }

    /// Hold `msg`, a part of album `group`; the album, when it is full.
    fn add(
        &mut self,
        group: String,
        msg: telegram::Message,
        now: std::time::Instant,
    ) -> Option<Vec<telegram::Message>> {
        let chat_id = msg.chat.id;
        let found = self
            .waiting
            .iter()
            .position(|a| a.chat_id == chat_id && a.group == group);
        let at = if let Some(at) = found {
            at
        } else {
            self.waiting.push(Album {
                chat_id,
                group,
                parts: Vec::new(),
                last: now,
            });
            self.waiting.len() - 1
        };
        let album = &mut self.waiting[at];
        album.parts.push(msg);
        album.last = now;
        (album.parts.len() >= ALBUM_PARTS).then(|| self.waiting.remove(at).parts)
    }

    /// The albums of `chat_id`, for a message that came after them.
    fn take_chat(&mut self, chat_id: i64) -> Vec<Vec<telegram::Message>> {
        self.take(|a| a.chat_id == chat_id)
    }

    /// The albums no part came for in [`ALBUM_QUIET`].
    fn take_quiet(&mut self, now: std::time::Instant) -> Vec<Vec<telegram::Message>> {
        self.take(|a| now.duration_since(a.last) >= ALBUM_QUIET)
    }

    fn take(&mut self, done: impl Fn(&Album) -> bool) -> Vec<Vec<telegram::Message>> {
        let (taken, kept): (Vec<Album>, Vec<Album>) = std::mem::take(&mut self.waiting)
            .into_iter()
            .partition(|a| done(a));
        self.waiting = kept;
        taken.into_iter().map(|a| a.parts).collect()
    }
}

/// `msgs`, one message or an album's parts, as one inbound frame.
async fn translate(
    msgs: &[telegram::Message],
    tg: &Telegram,
    whisper: Option<&Whisper>,
    gateway: &Gateway,
    account_id: &str,
    seq: &Arc<AtomicU64>,
) -> Option<ChannelFrame> {
    let msg = msgs.first()?;
    if msg.chat.kind != "private" {
        debug!(
            chat_kind = %msg.chat.kind,
            chat_id = msg.chat.id,
            "dropping non-private chat message (v0.2 supports 1:1 DMs only)"
        );
        return None;
    }
    let from = msg.from.as_ref()?;

    let timestamp = chrono::DateTime::from_timestamp(msg.date, 0).unwrap_or_else(chrono::Utc::now);
    let sender_id = format!("tg:{}", from.id);
    let mut attachments = Vec::new();

    // Dispatch by content shape. Telegram messages are mutually
    // exclusive between text / voice / location / contact at the
    // Bot API level (a single Update.message carries one); venue is
    // the only overlap (it includes both location and venue fields).
    let content = if let Some(voice) = &msg.voice {
        let caption = msg.caption.as_deref();
        translate_voice(
            tg,
            whisper,
            gateway,
            &sender_id,
            voice,
            caption,
            &mut attachments,
        )
        .await
    } else if let Some(venue) = &msg.venue {
        ChannelMessageContent::Location {
            latitude: venue.location.latitude,
            longitude: venue.location.longitude,
            accuracy_meters: venue.location.horizontal_accuracy,
            name: Some(venue.title.clone()),
            address: Some(venue.address.clone()),
        }
    } else if let Some(loc) = &msg.location {
        ChannelMessageContent::Location {
            latitude: loc.latitude,
            longitude: loc.longitude,
            accuracy_meters: loc.horizontal_accuracy,
            name: None,
            address: None,
        }
    } else if let Some(c) = &msg.contact {
        let display_name = match &c.last_name {
            Some(last) => format!("{} {last}", c.first_name),
            None => c.first_name.clone(),
        };
        ChannelMessageContent::Contact {
            display_name,
            phone_number: Some(c.phone_number.clone()),
            vcard: c.vcard.clone(),
        }
    } else if let Some(text) = &msg.text {
        ChannelMessageContent::Text { text: text.clone() }
    } else if msgs.len() > 1 {
        album_content(msgs, tg, gateway, &sender_id, &mut attachments).await
    } else {
        media_content(msg, tg, gateway, &sender_id, &mut attachments).await?
    };
    let content = with_context(content, &context_lines(msg));

    let inbound = ChannelInbound {
        seq: seq.fetch_add(1, Ordering::Relaxed),
        account_id: account_id.to_string(),
        channel_message_id: format!("tg:chat:{}:msg:{}", msg.chat.id, msg.message_id),
        sender_id,
        content,
        timestamp,
        // Stash the original payload for audit and so the agent can
        // inspect telegram-specific bits (username, first_name,
        // forward_from, …) if a future skill teaches it to.
        raw: serde_json::to_value(msg).ok(),
        attachments,
    };
    Some(ChannelFrame::Inbound(inbound))
}

/// An album's parts as one message (issue #14): all their files, and their
/// captions and notes.
async fn album_content(
    parts: &[telegram::Message],
    tg: &Telegram,
    gateway: &Gateway,
    sender_id: &str,
    attachments: &mut Vec<Attachment>,
) -> ChannelMessageContent {
    let mut texts = Vec::new();
    for part in parts {
        if let Some(ChannelMessageContent::Text { text }) =
            media_content(part, tg, gateway, sender_id, attachments).await
            && !text.trim().is_empty()
        {
            texts.push(text);
        }
    }
    ChannelMessageContent::Text {
        text: texts.join("\n\n"),
    }
}

/// `content` with `lines` (what it replies to or passes on) before it.
fn with_context(content: ChannelMessageContent, lines: &[String]) -> ChannelMessageContent {
    if lines.is_empty() {
        return content;
    }
    let head = lines.join("\n");
    let joined = |text: String| {
        if text.is_empty() {
            head.clone()
        } else {
            format!("{head}\n{text}")
        }
    };
    match content {
        ChannelMessageContent::Text { text } => ChannelMessageContent::Text { text: joined(text) },
        ChannelMessageContent::Audio {
            transcript,
            duration_seconds,
            url,
        } => ChannelMessageContent::Audio {
            transcript: joined(transcript),
            duration_seconds,
            url,
        },
        other => other,
    }
}

/// Characters of a replied-to message the agent is shown.
const REPLIED_CHARS: usize = 500;

/// What `msg` replies to, quotes or passes on, for the agent (issue #13):
/// marked lines, its sender-supplied parts quoted and escaped.
fn context_lines(msg: &telegram::Message) -> Vec<String> {
    let mut lines = Vec::new();
    if let Some(origin) = &msg.forward_origin {
        lines.push(format!(
            "[Forwarded from {} (user-supplied, untrusted)]",
            origin_name(origin)
        ));
    }
    let quoted = msg.quote.as_ref().map(|q| quoted(&q.text));
    if let Some(replied) = &msg.reply_to_message {
        let whose = if replied.from.as_ref().is_some_and(|u| u.is_bot) {
            "the bot's message"
        } else {
            "their own earlier message"
        };
        let what = quoted.unwrap_or_else(|| summary(replied));
        lines.push(format!(
            "[Replying to {whose} (user-supplied, untrusted): {what}]"
        ));
    } else if let Some(external) = &msg.external_reply {
        let what = quoted.unwrap_or_else(|| {
            content_kind(external.content.keys().map(String::as_str))
                .unwrap_or("a message")
                .to_string()
        });
        lines.push(format!(
            "[Replying to a message from {} in another chat (user-supplied, untrusted): {what}]",
            origin_name(&external.origin)
        ));
    }
    lines
}

/// `text` cut to [`REPLIED_CHARS`], as a quoted, escaped string.
fn quoted(text: &str) -> String {
    let mut cut: String = text.chars().take(REPLIED_CHARS).collect();
    if text.chars().count() > REPLIED_CHARS {
        cut.push('…');
    }
    serde_json::to_string(&cut).unwrap_or_default()
}

/// A replied-to message: its text or caption, quoted; else what it is.
fn summary(msg: &telegram::Message) -> String {
    if let Some(text) = msg.text.as_deref().or(msg.caption.as_deref()) {
        return quoted(text);
    }
    let value = serde_json::to_value(msg).unwrap_or_default();
    let keys = value.as_object().into_iter().flat_map(|o| {
        o.iter()
            .filter(|(_, v)| !v.is_null())
            .map(|(k, _)| k.as_str())
    });
    content_kind(keys).unwrap_or("a message").to_string()
}

/// What a message with fields `keys` carries.
fn content_kind<'a>(keys: impl Iterator<Item = &'a str>) -> Option<&'static str> {
    let keys: Vec<&str> = keys.collect();
    [
        ("photo", "a photo"),
        ("video", "a video"),
        ("video_note", "a video note"),
        ("animation", "an animation"),
        ("audio", "an audio file"),
        ("voice", "a voice message"),
        ("document", "a file"),
        ("sticker", "a sticker"),
        ("poll", "a poll"),
        ("venue", "a place"),
        ("location", "a location"),
        ("contact", "a contact"),
        ("dice", "a dice roll"),
        ("game", "a game"),
        ("story", "a story"),
    ]
    .iter()
    .find(|(key, _)| keys.contains(key))
    .map(|(_, kind)| *kind)
}

/// Who wrote a forwarded message: their name, quoted and escaped.
fn origin_name(origin: &telegram::MessageOrigin) -> String {
    let q = |s: &str| serde_json::to_string(s).unwrap_or_default();
    match origin.kind.as_str() {
        "user" => origin.sender_user.as_ref().map_or_else(
            || "a user".to_string(),
            |u| {
                let name = [u.first_name.as_deref(), u.last_name.as_deref()]
                    .into_iter()
                    .flatten()
                    .collect::<Vec<_>>()
                    .join(" ");
                match &u.username {
                    Some(handle) => format!("{} (@{})", q(&name), q(handle).trim_matches('"')),
                    None => q(&name),
                }
            },
        ),
        "hidden_user" => origin.sender_user_name.as_deref().map_or_else(
            || "a user who hides their account".to_string(),
            |name| format!("{}, who hides their account", q(name)),
        ),
        "chat" => origin
            .sender_chat
            .as_ref()
            .and_then(|c| c.title.as_deref())
            .map_or_else(|| "a group".to_string(), |t| format!("the group {}", q(t))),
        "channel" => origin
            .chat
            .as_ref()
            .and_then(|c| c.title.as_deref())
            .map_or_else(
                || "a channel".to_string(),
                |t| format!("the channel {}", q(t)),
            ),
        _ => "an unknown sender".to_string(),
    }
}

/// A message that carries a file, a sticker, a poll or a kind of content
/// this bot cannot pass on, for the agent; `None` for a service message
/// (a pin, a timer change) that is no one's message to answer.
async fn media_content(
    msg: &telegram::Message,
    tg: &Telegram,
    gateway: &Gateway,
    sender_id: &str,
    attachments: &mut Vec<Attachment>,
) -> Option<ChannelMessageContent> {
    Some(if let Some(sizes) = &msg.photo {
        // The largest of the sizes Telegram keeps.
        let Some(best) = sizes
            .iter()
            .max_by_key(|p| u64::from(p.width) * u64::from(p.height))
        else {
            return Some(ChannelMessageContent::Text {
                text: "[The sender sent a photo that could not be read.]".to_string(),
            });
        };
        let received = receive_file(
            tg,
            gateway,
            sender_id,
            &best.file_id,
            "photo.jpg",
            "image/jpeg",
        )
        .await;
        file_message(received, msg.caption.as_deref(), "photo.jpg", attachments)
    } else if let Some(doc) = &msg.document {
        // An animation (GIF) comes as a document too.
        let name = doc.file_name.as_deref().unwrap_or("file");
        let media_type = doc
            .mime_type
            .as_deref()
            .unwrap_or("application/octet-stream");
        let received = receive_file(tg, gateway, sender_id, &doc.file_id, name, media_type).await;
        file_message(received, msg.caption.as_deref(), name, attachments)
    } else if let Some((media, name, media_type)) = media_file(msg) {
        // A video, video note or audio file, passed on like a document.
        let name = media.file_name.clone().unwrap_or_else(|| name.to_string());
        let media_type = media.mime_type.as_deref().unwrap_or(media_type);
        let received =
            receive_file(tg, gateway, sender_id, &media.file_id, &name, media_type).await;
        file_message(received, msg.caption.as_deref(), &name, attachments)
    } else if let Some(sticker) = &msg.sticker {
        // Its picture, for an agent that can see (issue #10): the sticker
        // itself, or the thumbnail of one that moves; the text line is
        // enough when neither comes.
        let picture = if sticker.is_animated || sticker.is_video {
            sticker.thumbnail.as_ref().map(|t| t.file_id.as_str())
        } else {
            Some(sticker.file_id.as_str()).filter(|id| !id.is_empty())
        };
        if let Some(file_id) = picture {
            match receive_image(tg, gateway, sender_id, file_id, "sticker").await {
                Ok(attachment) => attachments.push(attachment),
                Err(e) => {
                    warn!(error = %format!("{e:#}"), "could not pass a sticker's picture on");
                }
            }
        }
        ChannelMessageContent::Text {
            text: sticker_text(sticker),
        }
    } else if let Some(poll) = &msg.poll {
        ChannelMessageContent::Text {
            text: poll_text(poll),
        }
    } else if msg.unsupported_content() {
        // Not dropped silently: the gateway answers a sender who is not
        // paired, and the agent can tell one who is.
        debug!(
            message_id = msg.message_id,
            "passing on an unsupported message as a note"
        );
        ChannelMessageContent::Text {
            text: "[The sender sent a kind of Telegram message this bot cannot pass on, \
                   such as a game, a dice roll or a story. Ask them to send text or a file.]"
                .to_string(),
        }
    } else {
        debug!(message_id = msg.message_id, "ignoring a service message");
        return None;
    })
}

/// The video, video note or audio file `msg` carries, with the name and
/// type to give it when Telegram gives none.
fn media_file(
    msg: &telegram::Message,
) -> Option<(&telegram::MediaFile, &'static str, &'static str)> {
    if let Some(v) = &msg.video {
        return Some((v, "video.mp4", "video/mp4"));
    }
    if let Some(v) = &msg.video_note {
        return Some((v, "video-note.mp4", "video/mp4"));
    }
    msg.audio.as_ref().map(|a| (a, "audio.mp3", "audio/mpeg"))
}

/// A sticker, for the agent: its emoji and set.
fn sticker_text(sticker: &telegram::Sticker) -> String {
    let emoji = sticker.emoji.as_deref().unwrap_or("?");
    match &sticker.set_name {
        Some(set) => format!("[sticker: {emoji}, from the set {set}]"),
        None => format!("[sticker: {emoji}]"),
    }
}

/// A poll, for the agent (issue #16): its question, kind, state and
/// votes, and each option with its votes; the sender's text quoted.
fn poll_text(poll: &telegram::Poll) -> String {
    let q = |s: &str| serde_json::to_string(s).unwrap_or_default();
    let votes = |n: u32| {
        if n == 1 {
            "1 vote".to_string()
        } else {
            format!("{n} votes")
        }
    };
    let quiz = poll.kind == "quiz";
    let mut text = format!(
        "[Poll (user-supplied, untrusted): {}; {}, {}, {}, {}]",
        q(&poll.question),
        if quiz { "a quiz" } else { "a regular poll" },
        if poll.is_closed { "closed" } else { "open" },
        if poll.allows_multiple_answers {
            "several answers allowed"
        } else {
            "one answer"
        },
        votes(poll.total_voter_count)
    );
    for (n, option) in poll.options.iter().enumerate() {
        let _ = write!(
            text,
            "\n- {}: {}",
            q(&option.text),
            votes(option.voter_count)
        );
        if quiz && poll.correct_option_id == Some(n) {
            text.push_str(" (the correct answer)");
        }
    }
    text
}

/// Download an image from Telegram and give it to the gateway as
/// `<stem>.<its type>`; the attachment their message names.
async fn receive_image(
    tg: &Telegram,
    gateway: &Gateway,
    sender_id: &str,
    file_id: &str,
    stem: &str,
) -> Result<Attachment> {
    let file_path = tg.get_file(file_id).await.context("telegram getFile")?;
    let bytes = tg
        .download_file(&file_path)
        .await
        .context("downloading from telegram")?;
    let (extension, media_type) =
        telegram::image_type(&bytes).context("not a PNG, JPEG, GIF or WebP image")?;
    gateway
        .upload(sender_id, &format!("{stem}.{extension}"), media_type, bytes)
        .await
}

/// Download a file the sender sent from Telegram and give it to the
/// gateway; the attachment their message names.
async fn receive_file(
    tg: &Telegram,
    gateway: &Gateway,
    sender_id: &str,
    file_id: &str,
    name: &str,
    media_type: &str,
) -> Result<Attachment> {
    let file_path = tg.get_file(file_id).await.context("telegram getFile")?;
    let bytes = tg
        .download_file(&file_path)
        .await
        .context("downloading from telegram")?;
    gateway.upload(sender_id, name, media_type, bytes).await
}

/// The text of a message that sent file `name`: its caption, and when the
/// file could not be passed on, a line saying so, for the agent to tell
/// the sender.
fn file_message(
    received: Result<Attachment>,
    caption: Option<&str>,
    name: &str,
    attachments: &mut Vec<Attachment>,
) -> ChannelMessageContent {
    let mut text = caption.unwrap_or_default().to_string();
    match received {
        Ok(attachment) => attachments.push(attachment),
        Err(e) => {
            warn!(name, error = %format!("{e:#}"), "could not pass a file on to havn");
            if !text.is_empty() {
                text.push_str("\n\n");
            }
            // Only what went wrong, not the error itself: none of it may
            // carry a URL with the bot token to the agent.
            let _ = write!(
                text,
                "[The sender sent a file, {name}, that could not be received: {}]",
                failure(&e)
            );
        }
    }
    ChannelMessageContent::Text { text }
}

/// What kept a file from the agent, for the message to say.
fn failure(e: &anyhow::Error) -> &'static str {
    let chain = format!("{e:#}");
    // The gateway's reasons first: several share a status.
    if chain.contains("inbox is full") {
        "the agent's inbox is full"
    } else if chain.contains("disk space") {
        "the agent's host is out of disk space"
    } else if chain.contains("in the last hour") {
        "this sender sent too many files in the last hour"
    } else if chain.contains("file is too big") {
        "it is larger than the 20 MB Telegram lets bots download"
    } else if chain.contains("larger than") || chain.contains("over 25 MB") {
        "it is larger than 25 MB"
    } else if chain.contains("(403") {
        "the agent does not take files from this sender"
    } else if chain.contains("still running") {
        "too many files at once; it can be sent again in a while"
    } else {
        "it could not be downloaded from Telegram or saved"
    }
}

/// A Telegram voice message, for the agent: its transcript, through the
/// Whisper API when one is configured; otherwise, or when transcription
/// fails, the voice message itself as an audio file, with a note saying
/// why. Never dropped.
async fn translate_voice(
    tg: &Telegram,
    whisper: Option<&Whisper>,
    gateway: &Gateway,
    sender_id: &str,
    voice: &telegram::Voice,
    caption: Option<&str>,
    attachments: &mut Vec<Attachment>,
) -> ChannelMessageContent {
    let caption = caption.filter(|c| !c.trim().is_empty());
    let audio = match tg.get_file(&voice.file_id).await {
        Ok(path) => tg.download_file(&path).await,
        Err(e) => Err(e),
    };
    let audio = match audio {
        Ok(bytes) => bytes,
        Err(e) => {
            warn!(file_id = %voice.file_id, error = %format!("{e:#}"), "could not download a voice message");
            return ChannelMessageContent::Text {
                text: "[The sender sent a voice message that could not be downloaded. Ask them to \
                       send it again, or as text.]"
                    .to_string(),
            };
        }
    };
    let why = match whisper {
        None => "no transcription is set up for this bot",
        Some(whisper) => match whisper.transcribe(audio.clone(), "voice.ogg").await {
            Ok(transcript) => {
                debug!(
                    duration = voice.duration,
                    bytes = audio.len(),
                    transcript_len = transcript.len(),
                    "telegram voice → whisper transcript"
                );
                // Not the file's URL: it holds the bot token.
                return ChannelMessageContent::Audio {
                    transcript: match caption {
                        Some(caption) => format!("{caption}\n\n{transcript}"),
                        None => transcript,
                    },
                    duration_seconds: Some(voice.duration),
                    url: None,
                };
            }
            Err(e) => {
                warn!(file_id = %voice.file_id, error = %format!("{e:#}"), "whisper transcription failed");
                "it could not be transcribed"
            }
        },
    };
    let received = gateway
        .upload(sender_id, "voice.ogg", "audio/ogg", audio)
        .await;
    // Says "attached" only when it was; `file_message` adds why not.
    let attached = if received.is_ok() {
        ", attached as voice.ogg"
    } else {
        ""
    };
    let mut note = format!(
        "[A voice message of {} seconds{attached}; {why}.]",
        voice.duration
    );
    if let Some(caption) = caption {
        note = format!("{caption}\n\n{note}");
    }
    file_message(received, Some(&note), "voice.ogg", attachments)
}

/// Send `text`, the agent's Markdown, to `chat_id`, formatted, in as many
/// messages as Telegram's limit takes; the first replies to `reply_to`.
/// When a part cannot be sent after the first, the person is told the
/// rest did not come.
async fn send_text(tg: &Telegram, chat_id: i64, text: &str, reply_to: Option<i64>) -> Result<()> {
    let parts = markdown::split(text, telegram::MESSAGE_UNITS);
    for (n, part) in parts.iter().enumerate() {
        if let Err(e) = send_part(tg, chat_id, part, if n == 0 { reply_to } else { None }).await {
            if n > 0 {
                let note = format!(
                    "(The rest of this reply, {} more part(s), could not be delivered.)",
                    parts.len() - n
                );
                let _ = tg.send_message(chat_id, &note, false, None).await;
            }
            return Err(e);
        }
    }
    Ok(())
}

/// One part of a reply, formatted (issue #11); as plain text when
/// Telegram refuses the formatting, or when formatting leaves no text or
/// makes it longer than a message.
async fn send_part(
    tg: &Telegram,
    chat_id: i64,
    markdown: &str,
    reply_to: Option<i64>,
) -> Result<()> {
    let html = markdown::to_html(markdown);
    if (1..=telegram::MESSAGE_UNITS).contains(&markdown::visible_units(&html)) {
        match tg.send_message(chat_id, &html, true, reply_to).await {
            Err(e) if telegram::refused_markup(&e) => {
                warn!(error = %e, "telegram refused a reply's formatting; sending it as plain text");
            }
            sent => return sent,
        }
    }
    for (n, piece) in telegram::split_text(markdown, telegram::MESSAGE_UNITS)
        .iter()
        .enumerate()
    {
        tg.send_message(chat_id, piece, false, if n == 0 { reply_to } else { None })
            .await?;
    }
    Ok(())
}

/// Send file `bytes` with `caption`, the agent's Markdown: an image as a
/// photo and a GIF as an animation when Telegram takes them (issue #15),
/// else as a document; the caption formatted, else plain. What of the
/// caption does not fit goes on after the file.
async fn send_file(
    tg: &Telegram,
    chat_id: i64,
    bytes: Vec<u8>,
    filename: &str,
    caption: Option<&str>,
    reply_to: Option<i64>,
) -> Result<()> {
    let (head, mut rest) = caption.map_or((String::new(), None), telegram::caption_parts);
    let html = markdown::to_html(&head);
    let mut formatted = (1..=telegram::CAPTION_UNITS).contains(&markdown::visible_units(&html));
    // As plain text the caption is cut again: what formatting hid (the
    // fences a split adds) counts then.
    let mut plain = telegram::split_text(&head, telegram::CAPTION_UNITS).into_iter();
    let plain_head = plain.next().unwrap_or_default();
    let plain_rest: Vec<String> = plain.collect();
    let mut kind = telegram::file_kind(&bytes);
    loop {
        let caption = if formatted { &html } else { &plain_head };
        match tg
            .send_file(
                kind,
                chat_id,
                bytes.clone(),
                filename,
                Some(caption),
                formatted,
                reply_to,
            )
            .await
        {
            Ok(()) => break,
            Err(e) if formatted && telegram::refused_markup(&e) => {
                warn!(error = %e, "telegram refused a caption's formatting; sending it as plain text");
                formatted = false;
            }
            // Only when Telegram answered: a request that failed on the
            // way may have been taken, and would come twice.
            Err(e) if kind != telegram::FileKind::Document && telegram::refused(&e) => {
                warn!(filename, ?kind, error = %format!("{e:#}"), "telegram refused a file as shown; sending it as a document");
                kind = telegram::FileKind::Document;
            }
            Err(e) => return Err(e),
        }
    }
    if !formatted && !plain_rest.is_empty() {
        let mut more = plain_rest.join("\n");
        if let Some(rest) = rest {
            more.push('\n');
            more.push_str(&rest);
        }
        rest = Some(more);
    }
    if let Some(rest) = rest {
        send_text(tg, chat_id, &rest, None).await?;
    }
    Ok(())
}

/// The "typing…" indicator of each chat waiting for a reply (issue #12):
/// Telegram shows it for about five seconds, and hides it when the bot
/// sends something, so it is sent every [`TYPING_EVERY`] while replies
/// are due, for at most [`TYPING_FOR`] at a time. The gateway does not say
/// which reply answers which message, so a chat counts the messages it
/// waits on: one more for each that comes, one fewer for each reply.
#[derive(Clone)]
struct Typing {
    send: Arc<SendTyping>,
    chats: Arc<Mutex<HashMap<i64, Waiting>>>,
    next: Arc<AtomicU64>,
}

/// A chat waiting for replies: how many, and the task showing it, with
/// the number that tells it from a later one.
struct Waiting {
    replies: u32,
    task: (u64, tokio::task::AbortHandle),
}

/// How the indicator is sent to a chat.
type SendTyping = dyn Fn(i64) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync;

const TYPING_EVERY: Duration = Duration::from_secs(4);
const TYPING_FOR: Duration = Duration::from_secs(300);

impl Typing {
    fn new(
        send: impl Fn(i64) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync + 'static,
    ) -> Self {
        Self {
            send: Arc::new(send),
            chats: Arc::new(Mutex::new(HashMap::new())),
            next: Arc::new(AtomicU64::new(0)),
        }
    }

    fn chats(&self) -> std::sync::MutexGuard<'_, HashMap<i64, Waiting>> {
        self.chats
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// A message came to `chat_id`: one more reply is due, and the
    /// indicator shows from now.
    fn start(&self, chat_id: i64) {
        let task = self.spawn(chat_id);
        let mut chats = self.chats();
        let replies = chats.get(&chat_id).map_or(0, |w| w.replies) + 1;
        if let Some(earlier) = chats.insert(chat_id, Waiting { replies, task }) {
            earlier.task.1.abort();
        }
    }

    /// A reply was sent to `chat_id`, or a message came that gets none:
    /// one fewer is due. The indicator stops when none is, and shows again
    /// when one still is.
    fn replied(&self, chat_id: i64) {
        let mut chats = self.chats();
        let Some(waiting) = chats.get_mut(&chat_id) else {
            return;
        };
        waiting.replies = waiting.replies.saturating_sub(1);
        if waiting.replies == 0 {
            if let Some(done) = chats.remove(&chat_id) {
                done.task.1.abort();
            }
            return;
        }
        drop(chats);
        self.again(chat_id);
    }

    /// Something that is not a reply (a file) was sent to `chat_id`:
    /// Telegram hid the indicator, which shows again if a reply is due.
    fn sent(&self, chat_id: i64) {
        if self.chats().contains_key(&chat_id) {
            self.again(chat_id);
        }
    }

    fn again(&self, chat_id: i64) {
        let task = self.spawn(chat_id);
        let mut chats = self.chats();
        match chats.get_mut(&chat_id) {
            Some(waiting) => std::mem::replace(&mut waiting.task, task).1.abort(),
            None => task.1.abort(),
        }
    }

    /// A task showing the indicator in `chat_id` until [`TYPING_FOR`] ends;
    /// then the chat waits on nothing, so a reply that never came is not
    /// counted against the next message.
    fn spawn(&self, chat_id: i64) -> (u64, tokio::task::AbortHandle) {
        let id = self.next.fetch_add(1, Ordering::Relaxed);
        let send = Arc::clone(&self.send);
        let chats = Arc::clone(&self.chats);
        let task = tokio::spawn(async move {
            let until = tokio::time::Instant::now() + TYPING_FOR;
            loop {
                send(chat_id).await;
                if tokio::time::Instant::now() + TYPING_EVERY > until {
                    break;
                }
                tokio::time::sleep(TYPING_EVERY).await;
            }
            let mut chats = chats
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if chats.get(&chat_id).is_some_and(|w| w.task.0 == id) {
                chats.remove(&chat_id);
            }
        });
        (id, task.abort_handle())
    }
}

/// havn → Telegram. Drain WS-received frames and translate each
/// `ChannelFrame::Outbound` into a `sendMessage` call. Other frame
/// kinds (Pong from gateway-initiated keepalive, ignored Inbound /
/// Resume) are logged and dropped.
async fn havn_outbound_loop(
    tg: Telegram,
    gateway: Gateway,
    mut inbound_rx: tokio::sync::mpsc::Receiver<ChannelFrame>,
    typing: Typing,
) -> Result<()> {
    // The menu last set, so a reconnect does not set it again.
    let mut menu: Option<Vec<types::ChannelCommand>> = None;
    while let Some(frame) = inbound_rx.recv().await {
        match frame {
            ChannelFrame::Commands(c) => {
                if menu.as_ref() == Some(&c.commands) {
                    continue;
                }
                match tg.set_my_commands(&c.commands).await {
                    Ok(n) => {
                        info!(commands = n, "telegram command menu set");
                        menu = Some(c.commands);
                    }
                    Err(e) => warn!(
                        error = %e,
                        "telegram setMyCommands failed; the menu stays as it was"
                    ),
                }
            }
            ChannelFrame::Outbound(o) => {
                if let Err(e) = deliver_outbound(&tg, &gateway, &o).await {
                    warn!(error = %e, seq = o.seq, "telegram sendMessage failed; dropping");
                }
                // A file is sent while the reply is still coming; text is a
                // reply.
                if let Some(chat_id) = chat_of(&o.to) {
                    if matches!(o.content, ChannelMessageContent::File { .. }) {
                        typing.sent(chat_id);
                    } else {
                        typing.replied(chat_id);
                    }
                }
            }
            ChannelFrame::Pong => {} // server keepalive, ignore
            ChannelFrame::Inbound(_) => {
                warn!("dropping Inbound frame from gateway (wrong direction)");
            }
            ChannelFrame::Resume(_) => {
                warn!("dropping Resume frame from gateway (wrong direction)");
            }
            ChannelFrame::Ping => {
                warn!("Ping from gateway not yet handled; ignoring");
            }
        }
        // ChannelFrame is `#[non_exhaustive]` but lives in this crate,
        // so the compiler treats it as exhaustive at the match site.
        // A future variant lands as a compile error here — at which
        // point the right move is to handle it, not silently drop.
    }
    info!("havn outbound channel closed; telegram outbound loop exiting");
    Ok(())
}

/// The chat of an outbound `to` (`tg:<chat_id>`).
fn chat_of(to: &str) -> Option<i64> {
    to.strip_prefix("tg:")?.parse().ok()
}

async fn deliver_outbound(tg: &Telegram, gateway: &Gateway, o: &ChannelOutbound) -> Result<()> {
    let chat_id = chat_of(&o.to)
        .ok_or_else(|| anyhow::anyhow!("outbound `to` not in tg:<id> format: {:?}", o.to))?;

    let reply_to_message_id = o.in_reply_to.as_deref().and_then(parse_message_id);

    let text = match &o.content {
        ChannelMessageContent::Text { text } => text.clone(),
        ChannelMessageContent::Image { url, caption } => {
            // v0.2 doesn't translate Image to Telegram's photo
            // endpoint — agent gets a textual fallback. When a real
            // user asks for image delivery, add tg.send_photo here.
            match caption {
                Some(c) => format!("[image: {url}]\n{c}"),
                None => format!("[image: {url}]"),
            }
        }
        ChannelMessageContent::File {
            url,
            filename,
            caption,
        } => match gateway.fetch(url).await {
            Ok(bytes) => {
                match send_file(
                    tg,
                    chat_id,
                    bytes,
                    filename,
                    caption.as_deref(),
                    reply_to_message_id,
                )
                .await
                {
                    Ok(()) => return Ok(()),
                    Err(e) => {
                        warn!(filename, error = %format!("{e:#}"), "telegram could not take a file");
                        format!("(A file, {filename}, could not be delivered.)")
                    }
                }
            }
            Err(e) => {
                warn!(filename, error = %format!("{e:#}"), "could not fetch a file the agent sent");
                format!("(A file, {filename}, could not be delivered.)")
            }
        },
        // Audio / Location / Contact outbound: agents would emit
        // these for symmetry, but Telegram has dedicated APIs
        // (sendVoice, sendLocation, sendContact). Until a real agent
        // emits one of these, flatten to a textual marker. Gateway-
        // side agent_socket also flattens before we get here, so
        // these arms are belt-and-braces.
        ChannelMessageContent::Audio {
            transcript,
            duration_seconds,
            ..
        } => match duration_seconds {
            Some(d) => format!("[voice, {d}s] {transcript}"),
            None => format!("[voice] {transcript}"),
        },
        ChannelMessageContent::Location {
            latitude,
            longitude,
            name,
            ..
        } => {
            let label = name.clone().unwrap_or_default();
            let coords = format!("{latitude:.6},{longitude:.6}");
            if label.is_empty() {
                format!("[location: {coords}]")
            } else {
                format!("[location: {coords} ({label})]")
            }
        }
        ChannelMessageContent::Contact {
            display_name,
            phone_number,
            ..
        } => match phone_number {
            Some(ph) => format!("[contact: {display_name} {ph}]"),
            None => format!("[contact: {display_name}]"),
        },
        // ChannelMessageContent is `#[non_exhaustive]`; in-crate
        // future variants compile-error here. Handle them explicitly
        // when added.
    };

    send_text(tg, chat_id, &text, reply_to_message_id).await
}

/// Parse a `channel_message_id` of the form `tg:chat:<chat>:msg:<msg>`
/// back to the integer `<msg>`. Returns None on any parse failure;
/// outbound is sent without `reply_to_message_id` in that case (no
/// threading, message still delivers).
fn parse_message_id(s: &str) -> Option<i64> {
    let after = s.strip_prefix("tg:")?;
    let mut parts = after.split(':');
    // Expect "chat:<id>:msg:<id>".
    let chat_label = parts.next()?;
    let _chat_id = parts.next()?;
    let msg_label = parts.next()?;
    let msg_id_str = parts.next()?;
    if chat_label != "chat" || msg_label != "msg" {
        return None;
    }
    msg_id_str.parse().ok()
}

#[cfg(test)]
mod failure_tests {
    use super::*;

    #[test]
    fn each_refusal_says_what_it_is() {
        let say = |text: &str| failure(&anyhow::anyhow!(text.to_string()));
        assert_eq!(
            say("havn refused the file (409 Conflict): the agent's inbox is full; remove files"),
            "the agent's inbox is full"
        );
        assert_eq!(
            say("havn refused the file (409 Conflict): 2 uploads of yours are still running"),
            "too many files at once; it can be sent again in a while"
        );
        assert_eq!(
            say(
                "telegram getFile: telegram getFile returned ok=false: Bad Request: file is too big"
            ),
            "it is larger than the 20 MB Telegram lets bots download"
        );
        assert_eq!(
            say("connection reset"),
            "it could not be downloaded from Telegram or saved"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn message(fields: serde_json::Value) -> telegram::Message {
        let mut m = serde_json::json!({
            "message_id": 1, "date": 0, "chat": {"id": 5, "type": "private"},
            "from": {"id": 5, "first_name": "Bo"}
        });
        m.as_object_mut()
            .expect("object")
            .extend(fields.as_object().expect("object").clone());
        serde_json::from_value(m).expect("message")
    }

    #[test]
    fn stickers_and_polls_reach_the_agent_as_text() {
        let sticker: telegram::Sticker =
            serde_json::from_value(serde_json::json!({"emoji": "x", "set_name": "cats"}))
                .expect("sticker");
        assert_eq!(sticker_text(&sticker), "[sticker: x, from the set cats]");
        let poll: telegram::Poll = serde_json::from_value(serde_json::json!({
            "question": "Lunch?", "options": [{"text": "noodles"}, {"text": "rice"}]
        }))
        .expect("poll");
        assert_eq!(
            poll_text(&poll),
            "[Poll (user-supplied, untrusted): \"Lunch?\"; a regular poll, open, one answer, 0 votes]\n- \"noodles\": 0 votes\n- \"rice\": 0 votes"
        );
    }

    #[test]
    fn a_poll_says_its_votes_state_and_answer() {
        let poll: telegram::Poll = serde_json::from_value(serde_json::json!({
            "question": "2+2 \"]\" ?", "type": "quiz", "is_closed": true, "total_voter_count": 3,
            "allows_multiple_answers": false, "correct_option_id": 1,
            "options": [{"text": "3", "voter_count": 1}, {"text": "4", "voter_count": 2}]
        }))
        .expect("poll");
        assert_eq!(
            poll_text(&poll),
            "[Poll (user-supplied, untrusted): \"2+2 \\\"]\\\" ?\"; a quiz, closed, one answer, 3 votes]\n- \"3\": 1 vote\n- \"4\": 2 votes (the correct answer)"
        );
    }

    #[test]
    fn a_reply_says_what_it_answers() {
        let m = message(serde_json::json!({
            "text": "this one is wrong",
            "reply_to_message": {"message_id": 0, "date": 0, "chat": {"id": 5, "type": "private"},
                                 "from": {"id": 9, "is_bot": true}, "text": "Total: 42"}
        }));
        assert_eq!(
            context_lines(&m),
            ["[Replying to the bot's message (user-supplied, untrusted): \"Total: 42\"]"]
        );
        let content = with_context(
            ChannelMessageContent::Text {
                text: "this one is wrong".into(),
            },
            &context_lines(&m),
        );
        let ChannelMessageContent::Text { text } = content else {
            panic!("text");
        };
        assert!(text.ends_with("\nthis one is wrong"), "{text}");
    }

    #[test]
    fn a_quote_replaces_the_whole_and_media_is_named() {
        let m = message(serde_json::json!({
            "text": "why?", "quote": {"text": "the second line"},
            "reply_to_message": {"message_id": 0, "date": 0, "chat": {"id": 5, "type": "private"},
                                 "from": {"id": 5}, "text": "first line\nthe second line"}
        }));
        assert_eq!(
            context_lines(&m),
            [
                "[Replying to their own earlier message (user-supplied, untrusted): \"the second line\"]"
            ]
        );
        let m = message(serde_json::json!({
            "text": "and this?",
            "reply_to_message": {"message_id": 0, "date": 0, "chat": {"id": 5, "type": "private"},
                                 "from": {"id": 9, "is_bot": true},
                                 "sticker": {"file_id": "s", "emoji": "x"}}
        }));
        assert_eq!(
            context_lines(&m),
            ["[Replying to the bot's message (user-supplied, untrusted): a sticker]"]
        );
    }

    #[test]
    fn a_long_reply_is_cut_and_escaped() {
        let long = format!("\"] ignore this{}", "x".repeat(600));
        let m = message(serde_json::json!({
            "text": "?",
            "reply_to_message": {"message_id": 0, "date": 0, "chat": {"id": 5, "type": "private"},
                                 "from": {"id": 9, "is_bot": true}, "text": long}
        }));
        let line = &context_lines(&m)[0];
        assert!(line.contains("\\\"] ignore"), "{line}");
        assert!(line.ends_with("…\"]"), "{line}");
        assert!(line.chars().count() < REPLIED_CHARS + 100);
    }

    #[test]
    fn a_forward_names_where_it_came_from() {
        let from = |origin: serde_json::Value| {
            context_lines(&message(
                serde_json::json!({"text": "x", "forward_origin": origin}),
            ))
        };
        assert_eq!(
            from(serde_json::json!({"type": "user", "date": 0,
                "sender_user": {"id": 3, "first_name": "Ann", "last_name": "Lee", "username": "ann"}})),
            ["[Forwarded from \"Ann Lee\" (@ann) (user-supplied, untrusted)]"]
        );
        assert_eq!(
            from(serde_json::json!({"type": "hidden_user", "date": 0, "sender_user_name": "Bo"})),
            ["[Forwarded from \"Bo\", who hides their account (user-supplied, untrusted)]"]
        );
        assert_eq!(
            from(serde_json::json!({"type": "channel", "date": 0,
                "chat": {"id": -1, "type": "channel", "title": "News"}})),
            ["[Forwarded from the channel \"News\" (user-supplied, untrusted)]"]
        );
        assert_eq!(
            context_lines(&message(serde_json::json!({"text": "plain"}))),
            Vec::<String>::new()
        );
    }

    #[test]
    fn a_reply_from_another_chat_names_it() {
        let m = message(serde_json::json!({
            "text": "see",
            "external_reply": {"origin": {"type": "channel", "date": 0,
                                          "chat": {"id": -1, "type": "channel", "title": "News"}},
                               "photo": [{"file_id": "p", "width": 1, "height": 1}]}
        }));
        assert_eq!(
            context_lines(&m),
            [
                "[Replying to a message from the channel \"News\" in another chat (user-supplied, untrusted): a photo]"
            ]
        );
    }

    #[test]
    fn an_album_waits_for_its_parts() {
        let now = std::time::Instant::now();
        let part = |id: i64, group: &str, chat: i64| {
            let mut m = message(serde_json::json!({"message_id": id, "media_group_id": group}));
            m.chat.id = chat;
            m
        };
        let mut albums = Albums::default();
        assert!(albums.add("g".into(), part(1, "g", 5), now).is_none());
        assert!(albums.holds(5, "g"));
        assert!(!albums.holds(6, "g"));
        assert!(albums.add("g".into(), part(2, "g", 5), now).is_none());
        assert!(albums.add("h".into(), part(3, "h", 6), now).is_none());
        assert_eq!(albums.take_quiet(now + Duration::from_millis(500)).len(), 0);
        let other = albums.take_chat(6);
        assert_eq!(
            other.len(),
            1,
            "a later message of chat 6 sends its album first"
        );
        let done = albums.take_quiet(now + ALBUM_QUIET);
        assert_eq!(done.len(), 1);
        assert_eq!(
            done[0].iter().map(|m| m.message_id).collect::<Vec<_>>(),
            [1, 2]
        );
        assert!(albums.is_empty());
        for id in 0..9 {
            assert!(
                albums
                    .add("full".into(), part(id, "full", 5), now)
                    .is_none()
            );
        }
        let full = albums.add("full".into(), part(9, "full", 5), now);
        assert_eq!(full.map(|parts| parts.len()), Some(ALBUM_PARTS));
        assert!(albums.is_empty());
    }

    fn counted() -> (Typing, Arc<AtomicU64>) {
        let sent = Arc::new(AtomicU64::new(0));
        let count = Arc::clone(&sent);
        let typing = Typing::new(move |_| {
            let count = Arc::clone(&count);
            Box::pin(async move {
                count.fetch_add(1, Ordering::Relaxed);
            })
        });
        (typing, sent)
    }

    #[tokio::test(start_paused = true)]
    async fn typing_shows_until_the_reply() {
        let (typing, sent) = counted();
        typing.start(7);
        tokio::time::sleep(Duration::from_secs(10)).await;
        assert_eq!(sent.load(Ordering::Relaxed), 3, "at 0, 4 and 8 seconds");
        typing.replied(7);
        tokio::time::sleep(Duration::from_secs(60)).await;
        assert_eq!(sent.load(Ordering::Relaxed), 3, "none after the reply");
        assert!(typing.chats().is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn typing_waits_for_every_reply_due() {
        let (typing, sent) = counted();
        typing.start(7);
        typing.start(7);
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert_eq!(sent.load(Ordering::Relaxed), 1, "shown once for both");
        typing.replied(7);
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert_eq!(
            sent.load(Ordering::Relaxed),
            2,
            "shown again: a reply is still due"
        );
        typing.sent(7);
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert_eq!(sent.load(Ordering::Relaxed), 3, "shown again after a file");
        typing.replied(7);
        tokio::time::sleep(Duration::from_secs(30)).await;
        assert_eq!(
            sent.load(Ordering::Relaxed),
            3,
            "none once every reply came"
        );
        typing.sent(7);
        typing.replied(7);
        tokio::time::sleep(Duration::from_secs(30)).await;
        assert_eq!(
            sent.load(Ordering::Relaxed),
            3,
            "nothing due, nothing shown"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn typing_ends_on_its_own_and_forgets_the_count() {
        let (typing, sent) = counted();
        typing.start(7);
        typing.start(7);
        tokio::time::sleep(TYPING_FOR + Duration::from_secs(60)).await;
        // The second replaced the first before it ran: at 0, 4, ... 300 s.
        assert_eq!(sent.load(Ordering::Relaxed), 76);
        assert!(typing.chats().is_empty(), "forgotten when done");
        typing.start(7);
        typing.replied(7);
        tokio::time::sleep(Duration::from_secs(30)).await;
        assert_eq!(sent.load(Ordering::Relaxed), 76, "the old count is gone");
    }

    #[test]
    fn the_chat_of_a_reply_is_read() {
        assert_eq!(chat_of("tg:123"), Some(123));
        assert_eq!(chat_of("tg:-100"), Some(-100));
        assert_eq!(chat_of("slack:1"), None);
    }

    #[test]
    fn parse_message_id_round_trip() {
        assert_eq!(parse_message_id("tg:chat:123:msg:7"), Some(7));
        assert_eq!(parse_message_id("tg:chat:123:msg:9999999"), Some(9_999_999));
    }

    #[test]
    fn parse_message_id_rejects_malformed() {
        assert_eq!(parse_message_id(""), None);
        assert_eq!(parse_message_id("tg:foo:1:bar:2"), None);
        assert_eq!(parse_message_id("tg:chat:x:msg:y"), None);
        assert_eq!(parse_message_id("chat:1:msg:2"), None); // missing tg: prefix
    }
}
