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
//! Group and supergroup messages go on with their group (havn issue
//! #263), marked as addressed to the bot when they name it, reply to it
//! or are a command; a reply to a group answers the message that last
//! addressed the bot there. Channel posts are dropped. On the outbound side every content kind
//! reaches the user: text through `sendMessage` (with the gateway's
//! buttons), a poll through `sendPoll` (its text if Telegram refuses
//! it), and a `File` fetched from the gateway and sent as a photo,
//! animation or document by what its bytes are; a file that cannot be
//! fetched or sent becomes a line saying so. `Image`, `Audio`,
//! `Location` and `Contact` go as text markers (`[image: <url>]`, ...).

#![forbid(unsafe_code)]

mod config;
mod gateway;
mod havn_ws;
mod markdown;
mod telegram;
mod types;

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
    /// Where the daemon keeps the last reply it delivered, to resume from
    /// after a restart. Defaults to `<account_id>.state.json` in systemd's
    /// `StateDirectory`, else next to the config file.
    #[arg(long, env = "HAVN_TG_STATE")]
    state: Option<PathBuf>,
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

    let telegram = Telegram::new(&cfg.bot_api, &cfg.bot_token, cfg.poll_timeout_seconds)
        .context("Telegram client init")?;
    // Who the bot is, for what in a group addresses it (havn issue #263).
    let me = loop {
        match telegram.get_me().await {
            Ok(me) => break Arc::new(me),
            Err(e) if Telegram::refused_token(&e) => {
                return Err(e).context("Telegram refused the bot token");
            }
            Err(e) => {
                warn!(error = %e, "telegram getMe failed; retrying in 5s");
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        }
    };
    info!(
        bot = me.username.as_deref().unwrap_or_default(),
        "telegram bot identified"
    );
    if !note_privacy(&me) {
        warn!("privacy mode is on: {PRIVACY_ADVICE}");
    }
    // Voice messages go to the gateway, which transcribes them (havn
    // issue #164).
    if cfg.openai_api_key.is_some() || cfg.whisper_base_url.is_some() || cfg.whisper_model.is_some()
    {
        warn!(
            "openai_api_key, whisper_base_url and whisper_model are no longer used: voice \
             messages go to the gateway, which transcribes them with [transcription] in its \
             config.toml"
        );
    }
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
    // The last reply delivered, to resume from (issue #23).
    let state = cli.state.clone().or_else(|| {
        // systemd sets it to the unit's StateDirectory (the first, when
        // there are several).
        let dir = std::env::var("STATE_DIRECTORY")
            .ok()
            .and_then(|dirs| dirs.split(':').next().map(PathBuf::from))
            .filter(|dir| !dir.as_os_str().is_empty())
            .or_else(|| cli.config.parent().map(std::path::Path::to_path_buf))?;
        Some(havn_ws::Delivered::file_in(&dir, &cfg.account_id))
    });
    keep_groups_beside(state.as_deref());
    let delivered = Arc::new(havn_ws::Delivered::load(state, &cfg.account_id));
    let ws = WsClient::spawn(WsConfig {
        gateway_url: cfg.havn_gateway.clone(),
        adapter_token: cfg.adapter_token.clone(),
        channel_id: cfg.channel_id.clone(),
        account_id: cfg.account_id.clone(),
        delivered: Arc::clone(&delivered),
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
        gateway.clone(),
        outbound_tx.clone(),
        cfg.account_id.clone(),
        typing.clone(),
        me,
    ));
    let outbound_task = tokio::spawn(havn_outbound_loop(
        telegram.clone(),
        gateway,
        inbound_rx,
        typing,
        delivered,
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
    gateway: Gateway,
    outbound_tx: tokio::sync::mpsc::Sender<ChannelFrame>,
    account_id: String,
    typing: Typing,
    me: Arc<telegram::TelegramUser>,
) -> Result<()> {
    let seq = Arc::new(AtomicU64::new(1));
    // Group messages read before the gateway says it takes them would be
    // dropped for good: the first read waits for it, a while.
    let asked = std::time::Instant::now();
    while !GATEWAY_SAID.load(Ordering::Relaxed) && asked.elapsed() < GATEWAY_SAYS_WITHIN {
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    // The next update not seen yet. Telegram forgets the updates before
    // the offset a poll gives, so while messages are held, polls give the
    // first of them: a restart then gets them again (seen ones are passed
    // over until then).
    let mut next: i64 = 0;
    let mut held = Held::default();
    let mut poll: Option<Poll> = None;
    loop {
        let polled = poll_or_due(&mut poll, &tg, &held, next).await;
        let updates = match polled {
            None => Vec::new(),
            Some(Ok(u)) => u,
            Some(Err(e)) => {
                warn!(error = %e, "telegram getUpdates failed; backing off 5s");
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                Vec::new()
            }
        };
        // Messages to pass on, in order: one each, or what was gathered.
        let mut ready: Vec<Vec<telegram::Message>> = Vec::new();
        for update in updates {
            if update.update_id < next {
                continue;
            }
            next = update.update_id + 1;
            // A button press (havn issue #203) goes on as the person who
            // pressed it sending the button's data; Telegram is told it was
            // taken, and the buttons come off so they are pressed once.
            // The bot's own status in a group changed: added, made an
            // administrator or not, removed.
            if let Some(change) = update.my_chat_member {
                if change.chat.kind == "group" || change.chat.kind == "supergroup" {
                    let member = matches!(
                        change.new_chat_member.status.as_str(),
                        "member" | "restricted"
                    );
                    let sees_all = if member {
                        privacy_now(&tg).await
                    } else {
                        false
                    };
                    note_status(change.chat.id, &change.new_chat_member.status, || sees_all);
                }
                continue;
            }
            if let Some(vote) = update.poll_answer {
                let Some(msg) = vote_message(&tg, &vote) else {
                    continue;
                };
                typing.start(msg.chat.id);
                held.pass(msg, &mut ready);
                continue;
            }
            let msg = match (update.message, update.callback_query) {
                (Some(msg), _) => msg,
                (None, Some(press)) => {
                    let pressed = press.as_message();
                    let tg = tg.clone();
                    let under = press.message.as_ref().map(|m| (m.chat.id, m.message_id));
                    tokio::spawn(async move {
                        tg.answer_press(&press.id).await;
                        if let Some((chat_id, message_id)) = under {
                            tg.clear_buttons(chat_id, message_id).await;
                        }
                    });
                    let Some(msg) = pressed else {
                        continue;
                    };
                    // A press goes on at once, not gathered with texts.
                    if msg.chat.kind == "private" {
                        typing.start(msg.chat.id);
                    }
                    held.pass(msg, &mut ready);
                    continue;
                }
                (None, None) => continue,
            };
            // Typing shows from before translate (which may do STT) until
            // the reply comes (issue #12); only for private chats: a group
            // message may get no reply. What is gathered is
            // one message, with one reply.
            if msg.chat.kind == "group" || msg.chat.kind == "supergroup" {
                learn_group(&msg, &me);
            }
            let (chat_id, private) = (msg.chat.id, msg.chat.kind == "private");
            if !held.put(update.update_id, msg, std::time::Instant::now(), &mut ready) && private {
                typing.start(chat_id);
            }
        }
        ready.extend(held.take_due(std::time::Instant::now()));
        for msgs in ready {
            let chat_id = msgs[0].chat.id;
            let frame = translate(&msgs, &tg, &gateway, &account_id, &seq, &me).await;
            // A service message gets no reply.
            if frame.is_none() {
                typing.replied(chat_id);
            }
            if let Some(f) = frame
                && outbound_tx.send(f).await.is_err()
            {
                info!("outbound channel closed; telegram inbound loop exiting");
                if let Some((task, _)) = &poll {
                    task.abort();
                }
                return Ok(());
            }
        }
    }
}

/// A getUpdates call in flight, and when it was asked.
type Poll = (
    tokio::task::JoinHandle<Result<Vec<telegram::Update>>>,
    std::time::Instant,
);

/// The updates of the poll in flight (asking one from `next`, or from the
/// first held update, when none is), or none when the first of the held
/// messages falls due first.
///
/// One poll at a time, in a task of its own so it ends in its time
/// whatever the loop does. When the held messages come first, the same
/// poll is waited on next, so none is given up or runs alongside another.
/// A poll asked when a hold was due already is given [`POLL_GRACE`]
/// first, so what came while the loop was busy (the rest of an album)
/// joins.
async fn poll_or_due(
    poll: &mut Option<Poll>,
    tg: &Telegram,
    held: &Held,
    next: i64,
) -> Option<Result<Vec<telegram::Update>>> {
    let (task, asked) = poll.get_or_insert_with(|| {
        let offset = held.first_update().map_or(next, |first| first.min(next));
        let tg = tg.clone();
        (
            tokio::spawn(async move { tg.get_updates(offset).await }),
            std::time::Instant::now(),
        )
    });
    let joined = match held.due() {
        Some(due) => {
            let until = deadline(due, *asked);
            tokio::select! {
                joined = &mut *task => joined,
                () = tokio::time::sleep_until(tokio::time::Instant::from_std(until)) => return None,
            }
        }
        None => task.await,
    };
    *poll = None;
    Some(joined.unwrap_or_else(|e| Err(e.into())))
}

/// How long a poll asked late may take to bring what came already.
const POLL_GRACE: Duration = Duration::from_millis(500);

/// When the poll asked at `asked` is given up for the holds due at `due`.
fn deadline(due: std::time::Instant, asked: std::time::Instant) -> std::time::Instant {
    if asked >= due {
        asked + POLL_GRACE
    } else {
        due
    }
}

/// How long an album waits for another part, and the most parts it has
/// (Telegram's limit).
const ALBUM_QUIET: Duration = Duration::from_millis(1500);
const ALBUM_PARTS: usize = 10;
/// How long a text waits for another (issue #24): briefly, for messages
/// sent in quick succession; as long as an album part after a text near
/// [`TEXT_SPLIT`] long, where the Telegram app splits a long paste (4096).
/// At most [`TEXT_PARTS`] go together.
const TEXT_QUIET: Duration = Duration::from_millis(300);
const TEXT_SPLIT: usize = 3500;
const TEXT_PARTS: usize = 25;

/// Messages held to go on as one: the parts of an album (issue #14), which
/// Telegram sends one right after another with the album's
/// `media_group_id`, or texts sent in quick succession, a long paste among
/// them (issue #24). A chat has one hold at a time; a message that does
/// not join it sends it on first, so the order stays.
#[derive(Default)]
struct Held {
    waiting: Vec<Hold>,
}

struct Hold {
    chat_id: i64,
    /// The album's `media_group_id`; none for texts.
    group: Option<String>,
    parts: Vec<telegram::Message>,
    /// The update its first part came in.
    first_update: i64,
    /// When it goes on unless another part comes.
    due: std::time::Instant,
}

impl Held {
    /// The update the first waiting part came in.
    fn first_update(&self) -> Option<i64> {
        self.waiting.iter().map(|h| h.first_update).min()
    }

    /// When the first hold is due.
    fn due(&self) -> Option<std::time::Instant> {
        self.waiting.iter().map(|h| h.due).min()
    }

    /// Take `msg`, from update `update`, adding to `ready` what is to go on
    /// now, in order. Whether it joined messages held already, so no
    /// other reply is due for it.
    fn put(
        &mut self,
        update: i64,
        msg: telegram::Message,
        now: std::time::Instant,
        ready: &mut Vec<Vec<telegram::Message>>,
    ) -> bool {
        // In a group only an album is gathered: texts in a row may be
        // several people's.
        if msg.chat.kind != "private" && msg.media_group_id.is_none() {
            self.pass(msg, ready);
            return false;
        }
        let chat_id = msg.chat.id;
        let group = msg.media_group_id.clone();
        // A command goes on at once, even right after a part of a long
        // paste (`/stop`); a part the app's split starts with a path is
        // not one.
        if group.is_none() && (msg.text.is_none() || command(&msg)) {
            self.pass(msg, ready);
            return false;
        }
        // A part joins its album however late it is read. A text that
        // replies, quotes or is forwarded starts its own, and so does one
        // after texts that are due already.
        let text_joins = context_lines(&msg).is_empty();
        let joins = self.waiting.iter().any(|h| {
            h.chat_id == chat_id
                && h.group == group
                && (group.is_some() || text_joins && now < h.due)
        });
        if !joins {
            ready.extend(self.take_chat(chat_id));
        }
        if let Some(parts) = self.add(group, update, msg, now) {
            ready.push(parts);
        }
        joins
    }

    /// Add `msg` to `ready`, after what its chat holds.
    fn pass(&mut self, msg: telegram::Message, ready: &mut Vec<Vec<telegram::Message>>) {
        ready.extend(self.take_chat(msg.chat.id));
        ready.push(vec![msg]);
    }

    /// Hold `msg`, of album `group` or a text, that came in update
    /// `update`; the hold, when it is full.
    fn add(
        &mut self,
        group: Option<String>,
        update: i64,
        msg: telegram::Message,
        now: std::time::Instant,
    ) -> Option<Vec<telegram::Message>> {
        let chat_id = msg.chat.id;
        let (quiet, most) = match &group {
            None if long(&msg) => (ALBUM_QUIET, TEXT_PARTS),
            None => (TEXT_QUIET, TEXT_PARTS),
            Some(_) => (ALBUM_QUIET, ALBUM_PARTS),
        };
        let found = self
            .waiting
            .iter()
            .position(|h| h.chat_id == chat_id && h.group == group);
        let at = if let Some(at) = found {
            at
        } else {
            self.waiting.push(Hold {
                chat_id,
                group,
                parts: Vec::new(),
                first_update: update,
                due: now,
            });
            self.waiting.len() - 1
        };
        let hold = &mut self.waiting[at];
        hold.parts.push(msg);
        hold.due = now + quiet;
        (hold.parts.len() >= most).then(|| self.waiting.remove(at).parts)
    }

    /// What `chat_id` holds, for a message that came after it.
    fn take_chat(&mut self, chat_id: i64) -> Vec<Vec<telegram::Message>> {
        self.take(|h| h.chat_id == chat_id)
    }

    /// The holds no part came for in time.
    fn take_due(&mut self, now: std::time::Instant) -> Vec<Vec<telegram::Message>> {
        self.take(|h| h.due <= now)
    }

    fn take(&mut self, done: impl Fn(&Hold) -> bool) -> Vec<Vec<telegram::Message>> {
        let (taken, kept): (Vec<Hold>, Vec<Hold>) = std::mem::take(&mut self.waiting)
            .into_iter()
            .partition(|h| done(h));
        self.waiting = kept;
        taken.into_iter().map(|h| h.parts).collect()
    }
}

/// Whether `msg` is a command (`/name` or `/name@bot`, then anything),
/// which goes on at once rather than with texts sent around it.
fn command(msg: &telegram::Message) -> bool {
    let Some(name) = msg.text.as_deref().and_then(|t| t.strip_prefix('/')) else {
        return false;
    };
    let name = name.split(char::is_whitespace).next().unwrap_or_default();
    let name = name.split_once('@').map_or(name, |(name, _)| name);
    !name.is_empty() && name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
}

/// Whether `msg` is a text near where the Telegram app splits a long paste.
fn long(msg: &telegram::Message) -> bool {
    msg.text
        .as_deref()
        .is_some_and(|t| t.encode_utf16().count() >= TEXT_SPLIT)
}

/// Whether the bot sees every message in groups (privacy mode off), as
/// `getMe` last said: what a group it is added to gets, as Telegram fixes
/// the mode of a group when the bot is added to it.
static SEES_ALL: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

/// Per group, whether the bot sees every message there, where known: from
/// its privacy mode when it was added, or shown by a message the bot gets
/// only when it does (as the group's administrator, say).
static GROUP_SEES_ALL: std::sync::LazyLock<std::sync::Mutex<std::collections::HashMap<i64, bool>>> =
    std::sync::LazyLock::new(Default::default);

/// Groups `GROUP_SEES_ALL` keeps at most.
const GROUPS_KNOWN: usize = 10_000;

/// Note whether `me`, as `getMe` gave it, sees every message in groups;
/// whether it does.
fn note_privacy(me: &telegram::TelegramUser) -> bool {
    let sees_all = me.can_read_all_group_messages.unwrap_or(false);
    SEES_ALL.store(sees_all, Ordering::Relaxed);
    sees_all
}

/// The advice for a bot in privacy mode, for the log.
const PRIVACY_ADVICE: &str = "in a group the bot gets only commands to it and replies to its \
     messages, not mentions; turn privacy mode off with BotFather (/setprivacy), then remove the \
     bot from the group and add it again, or make it an administrator there";

/// Whether the bot sees every message in group `chat_id`, where known.
fn group_sees_all(chat_id: i64) -> Option<bool> {
    GROUP_SEES_ALL.lock().ok()?.get(&chat_id).copied()
}

/// Where `GROUP_SEES_ALL` is kept across restarts, beside the state file:
/// a group in privacy mode never shows it again.
static GROUPS_FILE: std::sync::OnceLock<PathBuf> = std::sync::OnceLock::new();

/// Keep what is known of the groups beside the state file `state`
/// (`<state>.groups`), loading what was kept there.
fn keep_groups_beside(state: Option<&std::path::Path>) {
    let Some(state) = state else {
        return;
    };
    let mut path = state.as_os_str().to_owned();
    path.push(".groups");
    let path = PathBuf::from(path);
    let kept: Option<std::collections::HashMap<i64, bool>> = std::fs::read_to_string(&path)
        .ok()
        .and_then(|body| serde_json::from_str(&body).ok());
    if let (Some(kept), Ok(mut known)) = (kept, GROUP_SEES_ALL.lock()) {
        *known = kept;
    }
    let _ = GROUPS_FILE.set(path);
}

/// Write `known` to `GROUPS_FILE`, the file replaced whole.
fn save_groups(known: &std::collections::HashMap<i64, bool>) {
    let Some(path) = GROUPS_FILE.get() else {
        return;
    };
    let mut tmp = path.clone().into_os_string();
    tmp.push(".tmp");
    let tmp = PathBuf::from(tmp);
    let written = serde_json::to_string(known)
        .map_err(std::io::Error::other)
        .and_then(|body| {
            std::fs::write(&tmp, body)?;
            std::fs::rename(&tmp, path)
        });
    if let Err(e) = written {
        warn!(path = %path.display(), error = %e, "could not keep what is known of the groups");
    }
}

/// Note that the bot does (or does not) see every message in `chat_id`.
fn note_group(chat_id: i64, sees_all: bool) {
    let Ok(mut known) = GROUP_SEES_ALL.lock() else {
        return;
    };
    if known.get(&chat_id) == Some(&sees_all) {
        return;
    }
    if known.len() >= GROUPS_KNOWN && !known.contains_key(&chat_id) {
        let Some(&any) = known.keys().next() else {
            return;
        };
        known.remove(&any);
    }
    known.insert(chat_id, sees_all);
    save_groups(&known);
}

/// Forget `chat_id`: the bot left it.
fn forget_group(chat_id: i64) {
    let Ok(mut known) = GROUP_SEES_ALL.lock() else {
        return;
    };
    if known.remove(&chat_id).is_some() {
        save_groups(&known);
    }
}

/// How long `getMe` is waited for when the bot joins a group.
const GET_ME_WITHIN: Duration = Duration::from_secs(5);

/// Whether the bot sees every message in a group it joins now: its
/// privacy mode, as `getMe` says (or last said, when it does not answer
/// in time).
async fn privacy_now(tg: &Telegram) -> bool {
    match tokio::time::timeout(GET_ME_WITHIN, tg.get_me()).await {
        Ok(Ok(fresh)) => note_privacy(&fresh),
        Ok(Err(e)) => {
            warn!(error = %e, "telegram getMe failed; taking the privacy mode it last gave");
            SEES_ALL.load(Ordering::Relaxed)
        }
        Err(_) => {
            warn!("telegram getMe did not answer in time; taking the privacy mode it last gave");
            SEES_ALL.load(Ordering::Relaxed)
        }
    }
}

/// Note the bot's new `status` in group `chat_id`, from `my_chat_member`:
/// an administrator sees every message; a member what its privacy mode
/// gives (`member_sees_all`), as Telegram fixes it when the bot is added;
/// one that left is forgotten.
fn note_status(chat_id: i64, status: &str, member_sees_all: impl FnOnce() -> bool) {
    match status {
        "administrator" | "creator" => note_group(chat_id, true),
        "member" | "restricted" => {
            let sees_all = member_sees_all();
            if !sees_all {
                warn!(chat_id, "in a group in privacy mode: {PRIVACY_ADVICE}");
            }
            note_group(chat_id, sees_all);
        }
        "left" | "kicked" => forget_group(chat_id),
        _ => {}
    }
}

/// Whether `msg` says something: text, a caption, a file, a poll and such,
/// not a service message (a member joining, a pin, a new title).
fn says_something(msg: &telegram::Message) -> bool {
    msg.text.is_some()
        || msg.caption.is_some()
        || msg.photo.is_some()
        || msg.video.is_some()
        || msg.video_note.is_some()
        || msg.animation.is_some()
        || msg.audio.is_some()
        || msg.voice.is_some()
        || msg.document.is_some()
        || msg.sticker.is_some()
        || msg.poll.is_some()
        || msg.venue.is_some()
        || msg.location.is_some()
        || msg.contact.is_some()
        || msg.unsupported_content()
}

/// Whether group message `msg` shows that the bot `me` sees every message
/// there: one a bot in privacy mode does not get. Those it gets are
/// service messages, commands, replies, messages sent via a bot, and
/// (as `addressed` takes them) messages to it.
fn shows_all_seen(msg: &telegram::Message, me: &telegram::TelegramUser) -> bool {
    says_something(msg)
        && msg.reply_to_message.is_none()
        && msg.external_reply.is_none()
        && msg.via_bot.is_none()
        && !msg.is_automatic_forward
        && !command(msg)
        && !addressed(msg, me)
}

/// Note what group message `msg` tells of whether the bot `me` sees every
/// message there: the group moved to a supergroup, or a message only such
/// a bot gets came. Joins and leaves come with `my_chat_member`.
fn learn_group(msg: &telegram::Message, me: &telegram::TelegramUser) {
    let chat_id = msg.chat.id;
    if let Some(to) = msg.migrate_to_chat_id {
        if let Some(sees_all) = group_sees_all(chat_id) {
            note_group(to, sees_all);
        }
        forget_group(chat_id);
    } else if shows_all_seen(msg, me) {
        note_group(chat_id, true);
    }
}

/// The gateway said it takes group messages (havn issue #263).
static GATEWAY_GROUPS: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

tokio::task_local! {
    /// The group the files being sent came in, for the gateway to let in
    /// instead of their sender (havn issue #263).
    static UPLOAD_GROUP: Option<String>;
}

/// The group files uploaded now came in.
pub(crate) fn upload_group() -> Option<String> {
    UPLOAD_GROUP.try_with(Clone::clone).ok().flatten()
}

/// Whether group message `msg` is to the bot `me`: it replies to one of
/// the bot's messages, is a command (not for another bot), or names it,
/// by `@username` or a mention of the bot itself.
fn addressed(msg: &telegram::Message, me: &telegram::TelegramUser) -> bool {
    if msg
        .reply_to_message
        .as_ref()
        .and_then(|r| r.from.as_ref())
        .is_some_and(|u| u.id == me.id)
    {
        return true;
    }
    let text = msg
        .text
        .as_deref()
        .or(msg.caption.as_deref())
        .unwrap_or_default();
    let username = me.username.as_deref().unwrap_or_default();
    // A channel's post Telegram forwards into its group is no one asking.
    if msg.is_automatic_forward {
        return false;
    }
    if command(msg) {
        let word = text[1..].split_whitespace().next().unwrap_or_default();
        return match word.split_once('@') {
            Some((_, bot)) => !username.is_empty() && bot.eq_ignore_ascii_case(username),
            None => true,
        };
    }
    let named = msg
        .entities
        .iter()
        .chain(msg.caption_entities.iter())
        .flatten()
        .any(|e| e.kind == "text_mention" && e.user.as_ref().is_some_and(|u| u.id == me.id));
    named || (!username.is_empty() && mentions(text, username))
}

/// Whether `text` has `@username`, in any case, not part of a longer
/// word or name.
fn mentions(text: &str, username: &str) -> bool {
    let wanted = format!("@{}", username.to_ascii_lowercase());
    let lower = text.to_ascii_lowercase();
    let word = |c: char| c.is_ascii_alphanumeric() || c == '_';
    lower.match_indices(&wanted).any(|(at, _)| {
        !lower[..at].chars().next_back().is_some_and(word)
            && !lower[at + wanted.len()..].chars().next().is_some_and(word)
    })
}

/// The gateway's first `Commands` frame came, saying whether it takes
/// group messages.
static GATEWAY_SAID: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

/// How long the daemon waits at start for the gateway to say whether it
/// takes group messages, before it reads any.
const GATEWAY_SAYS_WITHIN: Duration = Duration::from_secs(30);

/// `msgs`, one message, an album's parts or texts gathered, as one inbound
/// frame.
async fn translate(
    msgs: &[telegram::Message],
    tg: &Telegram,
    gateway: &Gateway,
    account_id: &str,
    seq: &Arc<AtomicU64>,
    me: &telegram::TelegramUser,
) -> Option<ChannelFrame> {
    let msg = msgs.first()?;
    let from = msg.from.as_ref()?;
    // A group's message goes with its group (havn issue #263), to a
    // gateway that takes them; a channel's posts are not people writing to
    // the bot.
    let group = match msg.chat.kind.as_str() {
        "private" => None,
        "group" | "supergroup" if GATEWAY_GROUPS.load(Ordering::Relaxed) => {
            Some(crate::types::ChannelGroup {
                id: format!("tg:{}", msg.chat.id),
                title: msg.chat.title.clone(),
                // On behalf of a chat (an anonymous admin, a channel): the chat.
                sender_name: match &msg.sender_chat {
                    Some(chat) => chat.title.clone(),
                    None => Some(
                        [from.first_name.as_deref(), from.last_name.as_deref()]
                            .into_iter()
                            .flatten()
                            .collect::<Vec<_>>()
                            .join(" "),
                    )
                    .filter(|name| !name.is_empty())
                    .or_else(|| from.username.clone()),
                },
                addressed: msgs.iter().any(|m| addressed(m, me)),
                sees_all: group_sees_all(msg.chat.id),
            })
        }
        "group" | "supergroup" => {
            debug!(
                chat_id = msg.chat.id,
                "dropping a group message: the gateway has not said it takes them"
            );
            return None;
        }
        other => {
            debug!(
                chat_kind = other,
                chat_id = msg.chat.id,
                "dropping a message of a channel"
            );
            return None;
        }
    };
    // A message on behalf of a chat is that chat's: every anonymous admin
    // is the same Telegram user.
    let from_id = msg.sender_chat.as_ref().map_or(from.id, |chat| chat.id);
    // One not to the bot is kept by the gateway as context only: its files
    // are not fetched.
    if let Some(g) = &group
        && !g.addressed
    {
        if !says_something(msg) {
            return None;
        }
        let text = msg
            .text
            .clone()
            .unwrap_or_else(|| format!("«{}»", summary(msg)));
        return Some(ChannelFrame::Inbound(ChannelInbound {
            seq: seq.fetch_add(1, Ordering::Relaxed),
            account_id: account_id.to_string(),
            channel_message_id: format!("tg:chat:{}:msg:{}", msg.chat.id, msg.message_id),
            sender_id: format!("tg:{from_id}"),
            content: with_context(ChannelMessageContent::Text { text }, &context_lines(msg)),
            timestamp: chrono::DateTime::from_timestamp(msg.date, 0)
                .unwrap_or_else(chrono::Utc::now),
            raw: None,
            attachments: Vec::new(),
            group,
        }));
    }
    let uploads_in = group.as_ref().map(|g| g.id.clone());
    UPLOAD_GROUP
        .scope(
            uploads_in,
            translate_parts(msgs, tg, gateway, account_id, seq, group, from_id),
        )
        .await
}

/// [`translate`] for a message whose files are fetched and sent on.
async fn translate_parts(
    msgs: &[telegram::Message],
    tg: &Telegram,
    gateway: &Gateway,
    account_id: &str,
    seq: &Arc<AtomicU64>,
    group: Option<crate::types::ChannelGroup>,
    from_id: i64,
) -> Option<ChannelFrame> {
    let msg = msgs.first()?;

    let timestamp = chrono::DateTime::from_timestamp(msg.date, 0).unwrap_or_else(chrono::Utc::now);
    let sender_id = format!("tg:{from_id}");
    let mut attachments = Vec::new();

    // Dispatch by content shape. Telegram messages are mutually
    // exclusive between text / voice / location / contact at the
    // Bot API level (a single Update.message carries one); venue is
    // the only overlap (it includes both location and venue fields).
    let content = if let Some(voice) = &msg.voice {
        let caption = msg.caption.as_deref();
        translate_voice(tg, gateway, &sender_id, voice, caption, &mut attachments).await
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
    } else if msg.text.is_some() {
        let texts: Vec<&str> = msgs.iter().filter_map(|m| m.text.as_deref()).collect();
        ChannelMessageContent::Text {
            text: texts.join("\n"),
        }
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
        // forward_from, …) if a future skill teaches it to; every message
        // gathered.
        raw: if msgs.len() > 1 {
            serde_json::to_value(msgs).ok()
        } else {
            serde_json::to_value(msg).ok()
        },
        attachments,
        group,
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

/// A replied-to message: its text quoted; else what it is, with its
/// caption quoted.
fn summary(msg: &telegram::Message) -> String {
    if let Some(text) = msg.text.as_deref() {
        return quoted(text);
    }
    let value = serde_json::to_value(msg).unwrap_or_default();
    let keys = value.as_object().into_iter().flat_map(|o| {
        o.iter()
            .filter(|(_, v)| !v.is_null())
            .map(|(k, _)| k.as_str())
    });
    let kind = content_kind(keys).unwrap_or("a message");
    match msg.caption.as_deref() {
        Some(caption) => format!("{kind} with the caption {}", quoted(caption)),
        None => kind.to_string(),
    }
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
        // Given each time it comes: the gateway takes a file for one
        // message only (issue #28).
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

/// A sticker, for the agent: its emoji and set, quoted.
fn sticker_text(sticker: &telegram::Sticker) -> String {
    let q = |s: &str| serde_json::to_string(s).unwrap_or_default();
    let emoji = q(sticker.emoji.as_deref().unwrap_or("?"));
    match &sticker.set_name {
        Some(set) => format!("[sticker: {emoji}, from the set {}]", q(set)),
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

/// A Telegram voice message, for the agent: the audio file, which the
/// gateway transcribes when it is set up to (havn issue #164), with a line
/// saying what it is. Never dropped.
async fn translate_voice(
    tg: &Telegram,
    gateway: &Gateway,
    sender_id: &str,
    voice: &telegram::Voice,
    caption: Option<&str>,
    attachments: &mut Vec<Attachment>,
) -> ChannelMessageContent {
    let caption = caption.filter(|c| !c.trim().is_empty());
    let received = receive_file(
        tg,
        gateway,
        sender_id,
        &voice.file_id,
        "voice.ogg",
        "audio/ogg",
    )
    .await;
    // Whether it was transcribed, the agent is told with the file.
    let mut note = format!("[A voice message of {} seconds.]", voice.duration);
    if let Some(caption) = caption {
        note = format!("{caption}\n\n{note}");
    }
    file_message(received, Some(&note), "voice.ogg", attachments)
}

/// Send `text`, the agent's Markdown, to `chat_id`, formatted, in as many
/// messages as Telegram's limit takes; the first replies to `reply_to`.
/// When a part cannot be sent after the first, the person is told the
/// rest did not come.
async fn send_text(
    tg: &Telegram,
    chat_id: i64,
    text: &str,
    reply_to: Option<i64>,
    buttons: &[Vec<types::Button>],
) -> Result<()> {
    let parts = markdown::split(text, telegram::MESSAGE_UNITS);
    for (n, part) in parts.iter().enumerate() {
        // The buttons go under the last part.
        let under = if n + 1 == parts.len() { buttons } else { &[] };
        if let Err(e) = send_part(
            tg,
            chat_id,
            part,
            if n == 0 { reply_to } else { None },
            under,
        )
        .await
        {
            if n > 0 {
                let note = format!(
                    "(The rest of this reply, {} more part(s), could not be delivered.)",
                    parts.len() - n
                );
                let _ = tg.send_message(chat_id, &note, false, None, &[]).await;
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
    buttons: &[Vec<types::Button>],
) -> Result<()> {
    let html = markdown::to_html(markdown);
    if (1..=telegram::MESSAGE_UNITS).contains(&markdown::visible_units(&html)) {
        match tg
            .send_message(chat_id, &html, true, reply_to, buttons)
            .await
        {
            Err(e) if telegram::refused_markup(&e) => {
                warn!(error = %e, "telegram refused a reply's formatting; sending it as plain text");
            }
            sent => return sent,
        }
    }
    let pieces = telegram::split_text(markdown, telegram::MESSAGE_UNITS);
    for (n, piece) in pieces.iter().enumerate() {
        let under = if n + 1 == pieces.len() { buttons } else { &[] };
        tg.send_message(
            chat_id,
            piece,
            false,
            if n == 0 { reply_to } else { None },
            under,
        )
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
        send_text(tg, chat_id, &rest, None, &[]).await?;
    }
    Ok(())
}

/// The "typing…" indicator of each chat waiting for a reply (issue #12):
/// Telegram shows it for about five seconds, and hides it when the bot
/// sends something, so it is sent every [`TYPING_EVERY`] while replies
/// are due (again after each reply while more are), for at most
/// [`TYPING_FOR`] at a time. The gateway does not say which reply answers
/// which message, so a chat counts the messages it waits on: one more for
/// each that comes, one fewer for each reply.
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

    /// A part of a reply was sent to `chat_id`, with more to come:
    /// Telegram hid the indicator, which shows again while a reply is due.
    fn more(&self, chat_id: i64) {
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

/// havn → Telegram. Drain WS-received frames: each
/// `ChannelFrame::Outbound` is delivered by [`deliver_outbound`], and
/// `Commands` sets the bot's command menu. `Pong` is ignored; `Inbound`
/// and `Resume`, which only the adapter sends (the gateway replays from
/// a `Resume` it receives, as from `since` on the upgrade), and `Ping`
/// are logged and dropped.
async fn havn_outbound_loop(
    tg: Telegram,
    gateway: Gateway,
    mut inbound_rx: tokio::sync::mpsc::Receiver<ChannelFrame>,
    typing: Typing,
    delivered: Arc<havn_ws::Delivered>,
) -> Result<()> {
    // The menu last set, so a reconnect does not set it again.
    let mut menu: Option<Vec<types::ChannelCommand>> = None;
    while let Some(frame) = inbound_rx.recv().await {
        match frame {
            ChannelFrame::Commands(c) => {
                GATEWAY_GROUPS.store(c.groups, Ordering::Relaxed);
                GATEWAY_SAID.store(true, Ordering::Relaxed);
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
                delivered.mark(o.seq);
                // A reply came: a file (as some replies are) or text. A
                // part of one has more to come, so typing shows on.
                if let Some(chat_id) = chat_of(&o.to) {
                    if o.partial {
                        typing.more(chat_id);
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

/// The bot's open polls (havn issue #204), by poll id: their chat, message
/// and what a vote sends back.
static POLLS: std::sync::LazyLock<Mutex<Polls>> =
    std::sync::LazyLock::new(|| Mutex::new(Polls::default()));

/// How many open polls are remembered; past that, the oldest go.
const POLLS_KEPT: usize = 256;

#[derive(Default)]
struct Polls {
    open: HashMap<String, (i64, i64, String)>,
    order: std::collections::VecDeque<String>,
}

impl Polls {
    fn put(&mut self, poll_id: String, chat_id: i64, message_id: i64, answer: String) {
        if self
            .open
            .insert(poll_id.clone(), (chat_id, message_id, answer))
            .is_none()
        {
            self.order.push_back(poll_id);
        }
        while self.order.len() > POLLS_KEPT {
            if let Some(oldest) = self.order.pop_front() {
                self.open.remove(&oldest);
            }
        }
    }

    fn take(&mut self, poll_id: &str) -> Option<(i64, i64, String)> {
        let taken = self.open.remove(poll_id)?;
        self.order.retain(|id| id != poll_id);
        Some(taken)
    }
}

/// A vote in one of the bot's polls as a message from the person who
/// voted, its text the poll's answer and the numbers chosen; the poll is
/// closed, so it is answered once. `None` for a poll the bot does not
/// know (asked before a restart) or a vote taken back.
fn vote_message(tg: &Telegram, vote: &telegram::PollAnswer) -> Option<telegram::Message> {
    if vote.option_ids.is_empty() {
        return None;
    }
    let from = vote.user.clone()?;
    let mut polls = POLLS.lock().ok()?;
    // Only the person it was sent to answers it: a vote in a forwarded copy
    // leaves it open. In a private chat, the chat is the person.
    if polls.open.get(&vote.poll_id)?.0 != from.id {
        return None;
    }
    let (chat_id, message_id, answer) = polls.take(&vote.poll_id)?;
    drop(polls);
    let numbers: Vec<String> = vote
        .option_ids
        .iter()
        .map(|n| (n + 1).to_string())
        .collect();
    let tg = tg.clone();
    tokio::spawn(async move { tg.stop_poll(chat_id, message_id).await });
    let chat: telegram::Chat = serde_json::from_value(serde_json::json!({
        "id": chat_id, "type": "private"
    }))
    .ok()?;
    Some(telegram::Message {
        from: Some(from),
        text: Some(format!("{answer} {}", numbers.join(","))),
        date: chrono::Utc::now().timestamp(),
        ..telegram::Message::bare(message_id, chat)
    })
}

/// The chat of an outbound `to` (`tg:<chat_id>`).
fn chat_of(to: &str) -> Option<i64> {
    to.strip_prefix("tg:")?.parse().ok()
}

async fn deliver_outbound(tg: &Telegram, gateway: &Gateway, o: &ChannelOutbound) -> Result<()> {
    let chat_id = chat_of(&o.to)
        .ok_or_else(|| anyhow::anyhow!("outbound `to` not in tg:<id> format: {:?}", o.to))?;

    let reply_to_message_id = o.in_reply_to.as_deref().and_then(parse_message_id);

    // A question of several answers goes as a poll; its text, if the poll
    // is refused.
    if let Some(poll) = &o.poll {
        match tg.send_poll(chat_id, &poll.question, &poll.options).await {
            Ok((poll_id, message_id)) => {
                if let Ok(mut polls) = POLLS.lock() {
                    polls.put(poll_id, chat_id, message_id, poll.answer.clone());
                }
                return Ok(());
            }
            Err(e) => warn!(error = %format!("{e:#}"), "telegram refused a poll; sending its text"),
        }
    }

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
    // The end of a reply whose parts were all sent already says nothing
    // more (havn issue #212): it only ends the turn.
    if text.trim().is_empty() && o.buttons.is_empty() {
        return Ok(());
    }

    send_text(tg, chat_id, &text, reply_to_message_id, &o.buttons).await
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
    fn a_group_message_addresses_the_bot_by_name_reply_or_command() {
        let me: telegram::TelegramUser = serde_json::from_value(
            serde_json::json!({"id": 99, "is_bot": true, "username": "acme_bot"}),
        )
        .expect("me");
        let group = |fields: serde_json::Value| {
            let mut fields = fields;
            fields["chat"] = serde_json::json!({"id": -100, "type": "supergroup", "title": "Team"});
            message(fields)
        };
        for (fields, to_bot) in [
            (
                serde_json::json!({"text": "hi @acme_bot, a question"}),
                true,
            ),
            (serde_json::json!({"text": "hi @ACME_BOT"}), true),
            (serde_json::json!({"text": "hi @acme_bot_two"}), false),
            (serde_json::json!({"text": "lunch?"}), false),
            (serde_json::json!({"text": "/new"}), true),
            (serde_json::json!({"text": "/new@acme_bot"}), true),
            (serde_json::json!({"text": "/new@other_bot"}), false),
            (serde_json::json!({"text": "/etc/hosts is odd"}), false),
            (
                serde_json::json!({"text": "mail foo@acme_bot please"}),
                false,
            ),
            (
                serde_json::json!({"text": "news @acme_bot", "is_automatic_forward": true}),
                false,
            ),
            (
                serde_json::json!({"caption": "look @acme_bot", "photo": []}),
                true,
            ),
            (
                serde_json::json!({"text": "Acme, help", "entities": [
                    {"type": "text_mention", "offset": 0, "length": 4, "user": {"id": 99}}
                ]}),
                true,
            ),
            (
                serde_json::json!({"text": "yes", "reply_to_message": {
                    "message_id": 7, "date": 0, "chat": {"id": -100, "type": "supergroup"},
                    "from": {"id": 99, "is_bot": true}}}),
                true,
            ),
            (
                serde_json::json!({"text": "yes", "reply_to_message": {
                    "message_id": 7, "date": 0, "chat": {"id": -100, "type": "supergroup"},
                    "from": {"id": 6}}}),
                false,
            ),
        ] {
            let msg = group(fields.clone());
            assert_eq!(addressed(&msg, &me), to_bot, "{fields}");
        }
    }

    #[test]
    fn only_a_message_a_bot_in_privacy_mode_never_gets_shows_it_sees_all() {
        let me: telegram::TelegramUser = serde_json::from_value(serde_json::json!({
            "id": 99, "is_bot": true, "username": "acme_bot",
            "can_read_all_group_messages": false
        }))
        .expect("me");
        let group = |fields: serde_json::Value| {
            let mut fields = fields;
            fields["chat"] = serde_json::json!({"id": -100, "type": "group", "title": "Team"});
            message(fields)
        };
        for (fields, shows) in [
            (serde_json::json!({"text": "lunch?"}), true),
            (serde_json::json!({"photo": [], "caption": "look"}), true),
            // What a bot in privacy mode gets anyway.
            (
                serde_json::json!({"new_chat_members": [{"id": 99, "is_bot": true}]}),
                false,
            ),
            (serde_json::json!({"new_chat_title": "Team 2"}), false),
            (serde_json::json!({"text": "/help"}), false),
            (serde_json::json!({"text": "@acme_bot hi"}), false),
            (
                serde_json::json!({"text": "me too", "reply_to_message": {
                    "message_id": 7, "date": 0, "chat": {"id": -100, "type": "group"},
                    "from": {"id": 6}}}),
                false,
            ),
            (
                serde_json::json!({"text": "found it", "via_bot": {"id": 99, "is_bot": true}}),
                false,
            ),
            (
                serde_json::json!({"text": "news", "is_automatic_forward": true}),
                false,
            ),
            (
                serde_json::json!({"text": "this", "external_reply": {
                    "origin": {"type": "hidden_user", "date": 0, "sender_user_name": "X"}}}),
                false,
            ),
        ] {
            assert_eq!(
                shows_all_seen(&group(fields.clone()), &me),
                shows,
                "{fields}"
            );
        }
        assert!(!note_privacy(&me));
    }

    #[test]
    fn what_is_known_of_a_group_comes_with_the_bot_s_status_and_moves_with_it() {
        let me: telegram::TelegramUser = serde_json::from_value(
            serde_json::json!({"id": 99, "is_bot": true, "username": "acme_bot"}),
        )
        .expect("me");
        let in_group = |chat: i64, fields: serde_json::Value| {
            let mut fields = fields;
            fields["chat"] = serde_json::json!({"id": chat, "type": "group"});
            message(fields)
        };
        // Added in privacy mode, then made an administrator, then removed.
        note_status(-7001, "member", || false);
        assert_eq!(group_sees_all(-7001), Some(false));
        learn_group(
            &in_group(-7001, serde_json::json!({"text": "/help@acme_bot"})),
            &me,
        );
        assert_eq!(
            group_sees_all(-7001),
            Some(false),
            "a command proves nothing"
        );
        note_status(-7001, "administrator", || unreachable!());
        assert_eq!(group_sees_all(-7001), Some(true));
        note_status(-7001, "left", || unreachable!());
        assert_eq!(group_sees_all(-7001), None);
        // Added with privacy mode off; the group becomes a supergroup.
        note_status(-7002, "member", || true);
        learn_group(
            &in_group(-7002, serde_json::json!({"migrate_to_chat_id": -1_007_002})),
            &me,
        );
        assert_eq!(group_sees_all(-7002), None);
        assert_eq!(group_sees_all(-1_007_002), Some(true));
        // Not known (joined before the daemon), shown by a message only such a bot gets.
        assert_eq!(group_sees_all(-7003), None);
        learn_group(&in_group(-7003, serde_json::json!({"text": "lunch?"})), &me);
        assert_eq!(group_sees_all(-7003), Some(true));
        let change: telegram::ChatMemberUpdated = serde_json::from_value(serde_json::json!({
            "chat": {"id": -7004, "type": "supergroup"},
            "new_chat_member": {"status": "member", "user": {"id": 99, "is_bot": true}}
        }))
        .expect("my_chat_member");
        assert_eq!(change.new_chat_member.status, "member");
    }

    #[test]
    fn a_service_message_in_a_group_is_not_passed_on() {
        let msg = message(serde_json::json!({
            "chat": {"id": -100, "type": "group"},
            "new_chat_title": "Team 2"
        }));
        assert!(!says_something(&msg));
        assert!(says_something(&message(serde_json::json!({"text": "hi"}))));
    }

    #[test]
    fn stickers_and_polls_reach_the_agent_as_text() {
        let sticker: telegram::Sticker =
            serde_json::from_value(serde_json::json!({"emoji": "x", "set_name": "cats"}))
                .expect("sticker");
        assert_eq!(
            sticker_text(&sticker),
            "[sticker: \"x\", from the set \"cats\"]"
        );
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
        let mut held = Held::default();
        let mut ready = Vec::new();
        assert!(!held.put(1 + 100, part(1, "g", 5), now, &mut ready));
        assert!(held.put(2 + 100, part(2, "g", 5), now, &mut ready));
        assert!(!held.put(3 + 100, part(3, "h", 6), now, &mut ready));
        assert!(ready.is_empty());
        assert_eq!(held.first_update(), Some(101), "polls keep the first part");
        assert_eq!(held.due(), Some(now + ALBUM_QUIET));
        assert_eq!(held.take_due(now + Duration::from_millis(500)).len(), 0);
        let other = held.take_chat(6);
        assert_eq!(
            other.len(),
            1,
            "a later message of chat 6 sends its album first"
        );
        let done = held.take_due(now + ALBUM_QUIET);
        assert_eq!(done.len(), 1);
        assert_eq!(ids(&done[0]), [1, 2]);
        assert!(held.waiting.is_empty());
        assert_eq!(held.first_update(), None);
        for id in 0..10 {
            held.put(id + 100, part(id, "full", 5), now, &mut ready);
        }
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].len(), ALBUM_PARTS);
        ready.clear();

        // A part read late, after a slow upload, still joins its album.
        held.put(20, part(20, "late", 5), now, &mut ready);
        let late = now + ALBUM_QUIET * 4;
        assert!(held.put(21, part(21, "late", 5), late, &mut ready));
        assert_eq!(ids(&held.take_due(late + ALBUM_QUIET)[0]), [20, 21]);
        assert!(held.waiting.is_empty());
    }

    fn ids(msgs: &[telegram::Message]) -> Vec<i64> {
        msgs.iter().map(|m| m.message_id).collect()
    }

    #[test]
    fn texts_sent_in_quick_succession_go_as_one() {
        let now = std::time::Instant::now();
        let text =
            |id: i64, text: &str| message(serde_json::json!({"message_id": id, "text": text}));
        let mut held = Held::default();
        let mut ready = Vec::new();

        // A text waits briefly for another.
        assert!(!held.put(1, text(1, "hi"), now, &mut ready));
        assert_eq!(held.due(), Some(now + TEXT_QUIET));
        assert!(held.take_due(now + Duration::from_millis(200)).is_empty());
        let later = now + Duration::from_millis(200);
        assert!(held.put(2, text(2, "and this"), later, &mut ready));
        assert!(
            held.take_due(now + TEXT_QUIET).is_empty(),
            "the window starts again"
        );
        let done = held.take_due(later + TEXT_QUIET);
        assert_eq!(done.len(), 1);
        assert_eq!(ids(&done[0]), [1, 2]);

        // A long paste, split by the app near 4096: its parts wait longer.
        let part = "x".repeat(4096);
        assert!(!held.put(3, text(3, &part), now, &mut ready));
        let later = now + Duration::from_millis(1400);
        assert!(held.take_due(later).is_empty());
        assert!(held.put(4, text(4, &part), later, &mut ready));
        // A part may start with a `/`, where the app split.
        let last = format!("/var/log/{}", "y".repeat(1800));
        assert!(held.put(5, text(5, &last), later, &mut ready));
        assert!(held.take_due(later + Duration::from_millis(200)).is_empty());
        let done = held.take_due(later + TEXT_QUIET);
        assert_eq!(ids(&done[0]), [3, 4, 5]);
        assert!(ready.is_empty());

        // A command right after a long part is not a part.
        held.put(8, text(8, &part), now, &mut ready);
        assert!(!held.put(9, text(9, "/stop"), now, &mut ready));
        assert_eq!(
            ready.iter().map(|m| ids(m)).collect::<Vec<_>>(),
            [vec![8], vec![9]]
        );
        ready.clear();

        // A hold that is due takes no more, even before it has gone on.
        held.put(6, text(6, "one"), now, &mut ready);
        assert!(!held.put(7, text(7, "two"), now + TEXT_QUIET, &mut ready));
        assert_eq!(ready.iter().map(|m| ids(m)).collect::<Vec<_>>(), [vec![6]]);
    }

    #[test]
    fn a_poll_asked_late_gets_a_moment() {
        let now = std::time::Instant::now();
        let due = now + TEXT_QUIET;
        assert_eq!(deadline(due, now), due, "on time: the hold's own window");
        let late = due + Duration::from_secs(4);
        assert_eq!(deadline(due, late), late + POLL_GRACE);
    }

    #[test]
    fn commands_are_told_from_texts() {
        let is = |text: &str| command(&message(serde_json::json!({"text": text})));
        for text in ["/stop", "/model gpt", "/new@acme_bot", "/start@acme_bot x"] {
            assert!(is(text), "{text}");
        }
        for text in ["/var/log/syslog", "/", "/ stop", "hi /stop", "/über"] {
            assert!(!is(text), "{text}");
        }
        assert!(!command(&message(serde_json::json!({"caption": "/stop"}))));
    }

    #[test]
    fn what_is_held_goes_before_the_message_after_it() {
        let now = std::time::Instant::now();
        let text =
            |id: i64, text: &str| message(serde_json::json!({"message_id": id, "text": text}));
        let mut held = Held::default();
        let mut ready = Vec::new();

        held.put(1, text(1, "look"), now, &mut ready);
        let album = message(serde_json::json!({"message_id": 2, "media_group_id": "g"}));
        assert!(
            !held.put(2, album, now, &mut ready),
            "an album is not a text"
        );
        assert_eq!(ready.iter().map(|m| ids(m)).collect::<Vec<_>>(), [vec![1]]);
        held.put(3, text(3, "/stop"), now, &mut ready);
        assert_eq!(
            ready.iter().map(|m| ids(m)).collect::<Vec<_>>(),
            [vec![1], vec![2], vec![3]],
            "a command goes at once, after the album"
        );
        assert!(held.waiting.is_empty());

        // A reply starts its own message; a press is never gathered.
        ready.clear();
        held.put(4, text(4, "one"), now, &mut ready);
        let reply = message(serde_json::json!({"message_id": 5, "text": "two",
            "reply_to_message": {"message_id": 1, "date": 0, "chat": {"id": 5, "type": "private"}}}));
        assert!(!held.put(5, reply, now, &mut ready));
        held.pass(text(6, "choice"), &mut ready);
        assert_eq!(
            ready.iter().map(|m| ids(m)).collect::<Vec<_>>(),
            [vec![4], vec![5], vec![6]]
        );

        // Other chats are not held.
        ready.clear();
        let mut group = text(7, "hey");
        group.chat.kind = "group".into();
        assert!(!held.put(7, group, now, &mut ready));
        assert_eq!(ready.len(), 1);
        assert!(held.waiting.is_empty());
    }

    #[test]
    fn open_polls_are_remembered_up_to_a_limit() {
        let mut polls = Polls::default();
        for n in 0..=POLLS_KEPT {
            polls.put(
                format!("p{n}"),
                5,
                i64::try_from(n).expect("n"),
                "/choose q".into(),
            );
        }
        assert!(polls.take("p0").is_none(), "the oldest went");
        assert_eq!(
            polls.take(&format!("p{POLLS_KEPT}")),
            Some((
                5,
                i64::try_from(POLLS_KEPT).expect("n"),
                "/choose q".to_string()
            ))
        );
        assert!(polls.take(&format!("p{POLLS_KEPT}")).is_none(), "once");
    }

    #[test]
    fn a_replied_photo_names_its_kind_and_caption() {
        let m = message(serde_json::json!({
            "text": "this?",
            "reply_to_message": {"message_id": 0, "date": 0, "chat": {"id": 5, "type": "private"},
                                 "from": {"id": 5}, "caption": "the plan",
                                 "photo": [{"file_id": "p", "width": 1, "height": 1}]}
        }));
        assert_eq!(
            context_lines(&m),
            [
                "[Replying to their own earlier message (user-supplied, untrusted): a photo with the caption \"the plan\"]"
            ]
        );
        let m = message(serde_json::json!({
            "text": "and?",
            "reply_to_message": {"message_id": 0, "date": 0, "chat": {"id": 5, "type": "private"},
                                 "from": {"id": 9, "is_bot": true},
                                 "animation": {"file_id": "a"}, "document": {"file_id": "a"}}
        }));
        assert_eq!(
            context_lines(&m),
            ["[Replying to the bot's message (user-supplied, untrusted): an animation]"]
        );
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
        typing.replied(7);
        tokio::time::sleep(Duration::from_secs(30)).await;
        assert_eq!(
            sent.load(Ordering::Relaxed),
            2,
            "none once every reply came"
        );
        typing.replied(7);
        tokio::time::sleep(Duration::from_secs(30)).await;
        assert_eq!(
            sent.load(Ordering::Relaxed),
            2,
            "nothing due, nothing shown"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_part_of_a_reply_keeps_typing_on() {
        let (typing, sent) = counted();
        typing.start(7);
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert_eq!(sent.load(Ordering::Relaxed), 1);
        typing.more(7);
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert_eq!(sent.load(Ordering::Relaxed), 2, "shown again after a part");
        typing.replied(7);
        tokio::time::sleep(Duration::from_secs(30)).await;
        assert_eq!(sent.load(Ordering::Relaxed), 2, "the reply ends it");
        typing.more(7);
        tokio::time::sleep(Duration::from_secs(30)).await;
        assert_eq!(
            sent.load(Ordering::Relaxed),
            2,
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
