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
mod havn_ws;
mod telegram;
mod types;
mod whisper;

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use anyhow::{Context as _, Result};
use clap::Parser;
use tracing::{debug, info, warn};
use tracing_subscriber::EnvFilter;

use crate::config::Config;
use crate::havn_ws::{WsClient, WsConfig};
use crate::telegram::{Telegram, Update};
use crate::types::{ChannelFrame, ChannelInbound, ChannelMessageContent, ChannelOutbound};
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
    #[arg(long, env = "HAVN_TG_CONFIG", default_value = "/etc/havn-channel-telegram/config.toml")]
    config: PathBuf,
}

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();
    let cli = Cli::parse();
    let cfg = Config::load(&cli.config).with_context(|| {
        format!("loading config from {}", cli.config.display())
    })?;
    info!(
        config = %cli.config.display(),
        gateway = %cfg.havn_gateway,
        channel = %cfg.channel_id,
        account = %cfg.account_id,
        "havn-channel-telegram starting"
    );

    let telegram = Telegram::new(&cfg.bot_token, cfg.poll_timeout_seconds)
        .context("Telegram client init")?;
    let whisper = match cfg.openai_api_key.as_ref() {
        Some(k) if !k.is_empty() => {
            Some(
                Whisper::new(
                    k.clone(),
                    cfg.whisper_base_url.clone(),
                    cfg.whisper_model.clone(),
                )
                .context("Whisper init")?,
            )
        }
        _ => {
            info!("no openai_api_key configured — voice messages will be dropped");
            None
        }
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
        outbound_tx.clone(),
        cfg.account_id.clone(),
    ));
    let outbound_task = tokio::spawn(havn_outbound_loop(telegram.clone(), inbound_rx));

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
    outbound_tx: tokio::sync::mpsc::Sender<ChannelFrame>,
    account_id: String,
) -> Result<()> {
    let seq = Arc::new(AtomicU64::new(1));
    let mut offset: i64 = 0;
    loop {
        let updates = match tg.get_updates(offset).await {
            Ok(u) => u,
            Err(e) => {
                warn!(error = %e, "telegram getUpdates failed; backing off 5s");
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                continue;
            }
        };
        for update in updates {
            offset = update.update_id + 1;
            let chat_id = update
                .message
                .as_ref()
                .filter(|m| m.chat.kind == "private")
                .map(|m| m.chat.id);
            // Fire typing indicator before translate (which may do STT).
            // Spawned fire-and-forget so it never blocks the inbound loop.
            // Only for private chats — group messages are dropped by translate_update.
            if let Some(cid) = chat_id {
                let tg_c = tg.clone();
                tokio::spawn(async move { tg_c.send_typing(cid).await });
            }
            let frame = translate_update(&update, &tg, whisper.as_ref(), &account_id, &seq).await;
            if let Some(f) = frame
                && outbound_tx.send(f).await.is_err()
            {
                info!("outbound channel closed; telegram inbound loop exiting");
                return Ok(());
            }
        }
    }
}

async fn translate_update(
    update: &Update,
    tg: &Telegram,
    whisper: Option<&Whisper>,
    account_id: &str,
    seq: &Arc<AtomicU64>,
) -> Option<ChannelFrame> {
    let msg = update.message.as_ref()?;
    if msg.chat.kind != "private" {
        debug!(
            chat_kind = %msg.chat.kind,
            chat_id = msg.chat.id,
            "dropping non-private chat message (v0.2 supports 1:1 DMs only)"
        );
        return None;
    }
    let from = msg.from.as_ref()?;

    let timestamp = chrono::DateTime::from_timestamp(msg.date, 0)
        .unwrap_or_else(chrono::Utc::now);

    // Dispatch by content shape. Telegram messages are mutually
    // exclusive between text / voice / location / contact at the
    // Bot API level (a single Update.message carries one); venue is
    // the only overlap (it includes both location and venue fields).
    let content = if let Some(voice) = &msg.voice {
        translate_voice(tg, whisper, voice).await?
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
    } else {
        debug!(message_id = msg.message_id, "dropping unsupported message variant");
        return None;
    };

    let inbound = ChannelInbound {
        seq: seq.fetch_add(1, Ordering::Relaxed),
        account_id: account_id.to_string(),
        channel_message_id: format!("tg:chat:{}:msg:{}", msg.chat.id, msg.message_id),
        sender_id: format!("tg:{}", from.id),
        content,
        timestamp,
        // Stash the original payload for audit and so the agent can
        // inspect telegram-specific bits (username, first_name,
        // forward_from, …) if a future skill teaches it to.
        raw: serde_json::to_value(msg).ok(),
    };
    Some(ChannelFrame::Inbound(inbound))
}

/// Translate a Telegram voice message into a `ChannelMessageContent::
/// Audio` payload. Steps: resolve `file_id` → download OGG → Whisper
/// STT → transcript. Returns None on any failure (operator sees a
/// warn in the daemon log; user sees no agent reply, which is the
/// signal that something's wrong).
async fn translate_voice(
    tg: &Telegram,
    whisper: Option<&Whisper>,
    voice: &telegram::Voice,
) -> Option<ChannelMessageContent> {
    let Some(whisper) = whisper else {
        warn!(
            file_id = %voice.file_id,
            "received voice message but no Whisper API configured (set openai_api_key); dropping"
        );
        return None;
    };
    let file_path = match tg.get_file(&voice.file_id).await {
        Ok(p) => p,
        Err(e) => {
            warn!(file_id = %voice.file_id, error = %e, "telegram getFile failed");
            return None;
        }
    };
    let audio = match tg.download_file(&file_path).await {
        Ok(b) => b,
        Err(e) => {
            warn!(file_path, error = %e, "downloading voice from telegram failed");
            return None;
        }
    };
    let bytes_len = audio.len();
    let transcript = match whisper.transcribe(audio, "voice.ogg").await {
        Ok(t) => t,
        Err(e) => {
            warn!(
                file_id = %voice.file_id,
                bytes = bytes_len,
                error = %e,
                "whisper transcription failed"
            );
            return None;
        }
    };
    debug!(
        duration = voice.duration,
        bytes = bytes_len,
        transcript_len = transcript.len(),
        "telegram voice → whisper transcript"
    );
    Some(ChannelMessageContent::Audio {
        transcript,
        duration_seconds: Some(voice.duration),
        url: Some(tg.public_file_url(&file_path)),
    })
}

/// havn → Telegram. Drain WS-received frames and translate each
/// `ChannelFrame::Outbound` into a `sendMessage` call. Other frame
/// kinds (Pong from gateway-initiated keepalive, ignored Inbound /
/// Resume) are logged and dropped.
async fn havn_outbound_loop(
    tg: Telegram,
    mut inbound_rx: tokio::sync::mpsc::Receiver<ChannelFrame>,
) -> Result<()> {
    while let Some(frame) = inbound_rx.recv().await {
        match frame {
            ChannelFrame::Outbound(o) => {
                if let Err(e) = deliver_outbound(&tg, &o).await {
                    warn!(error = %e, seq = o.seq, "telegram sendMessage failed; dropping");
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

async fn deliver_outbound(tg: &Telegram, o: &ChannelOutbound) -> Result<()> {
    // Parse `to` (`tg:<chat_id>`).
    let chat_id: i64 = o
        .to
        .strip_prefix("tg:")
        .ok_or_else(|| {
            anyhow::anyhow!("outbound `to` not in tg:<id> format: {:?}", o.to)
        })?
        .parse()
        .with_context(|| format!("parse chat_id from {:?}", o.to))?;

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
        ChannelMessageContent::File { url, filename } => {
            format!("[file: {filename}]\n{url}")
        }
        // Audio / Location / Contact outbound: agents would emit
        // these for symmetry, but Telegram has dedicated APIs
        // (sendVoice, sendLocation, sendContact). Until a real agent
        // emits one of these, flatten to a textual marker. Gateway-
        // side agent_socket also flattens before we get here, so
        // these arms are belt-and-braces.
        ChannelMessageContent::Audio { transcript, duration_seconds, .. } => match duration_seconds {
            Some(d) => format!("[voice, {d}s] {transcript}"),
            None => format!("[voice] {transcript}"),
        },
        ChannelMessageContent::Location { latitude, longitude, name, .. } => {
            let label = name.clone().unwrap_or_default();
            let coords = format!("{latitude:.6},{longitude:.6}");
            if label.is_empty() {
                format!("[location: {coords}]")
            } else {
                format!("[location: {coords} ({label})]")
            }
        }
        ChannelMessageContent::Contact { display_name, phone_number, .. } => match phone_number {
            Some(ph) => format!("[contact: {display_name} {ph}]"),
            None => format!("[contact: {display_name}]"),
        },
        // ChannelMessageContent is `#[non_exhaustive]`; in-crate
        // future variants compile-error here. Handle them explicitly
        // when added.
    };

    tg.send_message(chat_id, &text, reply_to_message_id).await
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
mod tests {
    use super::*;

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
