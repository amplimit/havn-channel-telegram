//! Thin Telegram Bot API client — only the endpoints this adapter
//! needs: `getUpdates` (long-poll inbound), `getFile` and the file
//! download (voice, photos, documents), `sendMessage`, `sendDocument`,
//! `sendPhoto` and `sendAnimation` (deliver outbound), and
//! `sendChatAction` (typing indicator).
//!
//! Reference: <https://core.telegram.org/bots/api>. Bot tokens look
//! like `<bot_id>:<random>`; the API base is `<bot_api>/bot<token>/`,
//! `bot_api` being `https://api.telegram.org` unless configured.
//!
//! We deliberately don't pull in `teloxide` or another full SDK —
//! this daemon's surface is tiny, and a small bespoke client is
//! easier to audit / lock down than a multi-thousand-LOC dep.

use anyhow::{Context as _, Result};
use serde::{Deserialize, Serialize};

#[derive(Clone)]
pub struct Telegram {
    base: String,
    /// Where files are downloaded from; also holds the bot token.
    files: String,
    http: reqwest::Client,
    poll_timeout_seconds: u64,
}

// By hand: `base` holds the bot token.
impl std::fmt::Debug for Telegram {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Telegram")
            .field("poll_timeout_seconds", &self.poll_timeout_seconds)
            .finish_non_exhaustive()
    }
}

impl Telegram {
    pub fn new(bot_api: &str, bot_token: &str, poll_timeout_seconds: u64) -> Result<Self> {
        let http = reqwest::Client::builder()
            // getUpdates long-poll timeout + 5s slack so the HTTP
            // client doesn't fire before Telegram does.
            .timeout(std::time::Duration::from_secs(poll_timeout_seconds + 5))
            // The bot token is in every URL: it is not carried to where a
            // redirect points, nor, in the clear, to a proxy.
            .redirect(reqwest::redirect::Policy::none());
        let http = if bot_api.starts_with("http://") {
            http.no_proxy()
        } else {
            http
        }
        .build()
        .context("build reqwest client")?;
        Ok(Self {
            base: format!("{}/bot{bot_token}", bot_api.trim_end_matches('/')),
            files: format!("{}/file/bot{bot_token}", bot_api.trim_end_matches('/')),
            http,
            poll_timeout_seconds,
        })
    }

    /// Long-poll for updates from Telegram. `offset` is the
    /// `update_id + 1` of the last update we processed; Telegram
    /// drops anything older than that on its end. Returns up to 100
    /// updates per call (Telegram's hard cap).
    pub async fn get_updates(&self, offset: i64) -> Result<Vec<Update>> {
        let url = format!("{}/getUpdates", self.base);
        let timeout = self.poll_timeout_seconds;
        let resp: ApiResponse<Vec<serde_json::Value>> = self
            .http
            .get(&url)
            .query(&[
                ("offset", offset.to_string()),
                ("timeout", timeout.to_string()),
                // Don't request `edited_message` / `channel_post` /
                // `inline_query` — agents respond to direct messages
                // only in v0.2. Future expansion can widen this.
                (
                    "allowed_updates",
                    "[\"message\",\"callback_query\",\"poll_answer\"]".to_string(),
                ),
            ])
            .send()
            .await
            .map_err(reqwest::Error::without_url)
            .context("getUpdates request")?
            .json()
            .await
            .context("getUpdates body parse")?;
        if !resp.ok {
            anyhow::bail!(
                "telegram getUpdates returned ok=false: {}",
                resp.description.unwrap_or_default()
            );
        }
        // Each on its own: one this bot cannot read is passed over, not
        // left to hold up every message after it.
        Ok(resp
            .result
            .unwrap_or_default()
            .into_iter()
            .filter_map(read_update)
            .collect())
    }

    /// Resolve a `file_id` to a downloadable path. Telegram returns
    /// a `file_path` valid for 1 hour. Used by `download_file` below
    /// for voice messages.
    pub async fn get_file(&self, file_id: &str) -> Result<String> {
        let url = format!("{}/getFile", self.base);
        let resp: ApiResponse<FileMeta> = self
            .http
            .get(&url)
            .query(&[("file_id", file_id)])
            .send()
            .await
            .map_err(reqwest::Error::without_url)
            .context("getFile request")?
            .json()
            .await
            .context("getFile body")?;
        if !resp.ok {
            anyhow::bail!(
                "telegram getFile returned ok=false: {}",
                resp.description.unwrap_or_default()
            );
        }
        let file = resp.result.context("getFile missing result")?;
        Ok(file.file_path)
    }

    /// Download the raw bytes of a Telegram-hosted file, from
    /// `<bot_api>/file/bot<token>/<file_path>`.
    pub async fn download_file(&self, file_path: &str) -> Result<Vec<u8>> {
        let url = format!("{}/{file_path}", self.files);
        let mut resp = self
            .http
            .get(&url)
            .timeout(std::time::Duration::from_secs(180))
            .send()
            .await
            .map_err(reqwest::Error::without_url)
            .context("download_file request")?
            .error_for_status()
            .map_err(reqwest::Error::without_url)
            .context("download_file status")?;
        // Redirects are not followed, so a 3xx is no file.
        if !resp.status().is_success() {
            anyhow::bail!("download_file: the server answered {}", resp.status());
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = resp.chunk().await.context("download_file body")? {
            bytes.extend_from_slice(&chunk);
            if bytes.len() > crate::gateway::MAX_FILE_BYTES {
                anyhow::bail!("the file is larger than the gateway takes");
            }
        }
        Ok(bytes)
    }

    /// Send a file as `kind`, with its caption (Telegram keeps 1024
    /// characters of one), in HTML when `html`.
    #[allow(clippy::too_many_arguments)]
    pub async fn send_file(
        &self,
        kind: FileKind,
        chat_id: i64,
        bytes: Vec<u8>,
        filename: &str,
        caption: Option<&str>,
        html: bool,
        reply_to_message_id: Option<i64>,
    ) -> Result<()> {
        let method = kind.method();
        let url = format!("{}/{method}", self.base);
        // Telegram may ask the bot to slow down, and says how long to wait.
        for tried in 1..=SEND_TRIES {
            let part =
                reqwest::multipart::Part::bytes(bytes.clone()).file_name(filename.to_string());
            let mut form = reqwest::multipart::Form::new()
                .text("chat_id", chat_id.to_string())
                .part(kind.field(), part);
            if let Some(caption) = caption.filter(|c| !c.trim().is_empty()) {
                form = form.text("caption", caption.to_string());
                if html {
                    form = form.text("parse_mode", "HTML");
                }
            }
            if let Some(rt) = reply_to_message_id {
                form = form
                    .text("reply_to_message_id", rt.to_string())
                    .text("allow_sending_without_reply", "true");
            }
            let resp: ApiResponse<serde_json::Value> = self
                .http
                .post(&url)
                .timeout(std::time::Duration::from_secs(180))
                .multipart(form)
                .send()
                .await
                .map_err(reqwest::Error::without_url)
                .with_context(|| format!("{method} request"))?
                .json()
                .await
                .with_context(|| format!("{method} body parse"))?;
            if resp.ok {
                return Ok(());
            }
            match resp.parameters.and_then(|p| p.retry_after) {
                Some(wait) if tried < SEND_TRIES => {
                    tracing::info!(
                        wait,
                        "telegram asked to slow down; sending the file again after the wait"
                    );
                    tokio::time::sleep(std::time::Duration::from_secs(
                        wait.min(MAX_RETRY_AFTER_SECONDS),
                    ))
                    .await;
                }
                _ => anyhow::bail!(
                    "telegram {method} returned ok=false: {}",
                    resp.description.unwrap_or_default()
                ),
            }
        }
        anyhow::bail!("telegram {method}: still refused after {SEND_TRIES} tries")
    }

    /// Send a "typing" chat action. The indicator lasts ~5 seconds on
    /// the client side or until a message is sent (whichever comes first).
    /// Best-effort — failures are logged but not propagated.
    pub async fn send_typing(&self, chat_id: i64) {
        let url = format!("{}/sendChatAction", self.base);
        let body = serde_json::json!({ "chat_id": chat_id, "action": "typing" });
        if let Err(e) = self.http.post(&url).json(&body).send().await {
            // Its URL holds the bot token.
            tracing::debug!(error = %e.without_url(), "sendChatAction failed (non-fatal)");
        }
    }

    /// Send a text message, in HTML when `html`. `reply_to_message_id`
    /// threads under the original (Telegram-side equivalent of
    /// `in_reply_to`).
    pub async fn send_message(
        &self,
        chat_id: i64,
        text: &str,
        html: bool,
        reply_to_message_id: Option<i64>,
        buttons: &[Vec<crate::types::Button>],
    ) -> Result<()> {
        let url = format!("{}/sendMessage", self.base);
        let mut body = serde_json::json!({
            "chat_id": chat_id,
            "text": text,
        });
        if html {
            body["parse_mode"] = serde_json::json!("HTML");
        }
        if !buttons.is_empty() {
            body["reply_markup"] = keyboard(buttons);
        }
        if let Some(rt) = reply_to_message_id {
            body["reply_to_message_id"] = serde_json::json!(rt);
            // If the original was deleted, don't error — just send as
            // a fresh message.
            body["allow_sending_without_reply"] = serde_json::json!(true);
        }
        // Telegram limits how fast a bot writes to one chat; a long reply
        // sent in parts can hit it, and it says how long to wait.
        for tried in 1..=SEND_TRIES {
            let resp: ApiResponse<serde_json::Value> = self
                .http
                .post(&url)
                .json(&body)
                .send()
                .await
                .map_err(reqwest::Error::without_url)
                .context("sendMessage request")?
                .json()
                .await
                .context("sendMessage body parse")?;
            if resp.ok {
                return Ok(());
            }
            match resp.parameters.and_then(|p| p.retry_after) {
                Some(wait) if tried < SEND_TRIES => {
                    tracing::info!(
                        wait,
                        "telegram asked to slow down; sending again after the wait"
                    );
                    tokio::time::sleep(std::time::Duration::from_secs(
                        wait.min(MAX_RETRY_AFTER_SECONDS),
                    ))
                    .await;
                }
                _ => anyhow::bail!(
                    "telegram sendMessage returned ok=false: {}",
                    resp.description.unwrap_or_default()
                ),
            }
        }
        anyhow::bail!("telegram sendMessage: still refused after {SEND_TRIES} tries")
    }
}

impl Telegram {
    /// Tell Telegram a button press was taken, so the person's app stops
    /// waiting on it. Best-effort.
    pub async fn answer_press(&self, press_id: &str) {
        let url = format!("{}/answerCallbackQuery", self.base);
        let body = serde_json::json!({ "callback_query_id": press_id });
        if let Err(e) = self.http.post(&url).json(&body).send().await {
            tracing::debug!(error = %e.without_url(), "answerCallbackQuery failed (non-fatal)");
        }
    }

    /// Send a poll of several answers that is not anonymous, so each vote
    /// comes as a `poll_answer`: its poll id and message id.
    pub async fn send_poll(
        &self,
        chat_id: i64,
        question: &str,
        options: &[String],
    ) -> Result<(String, i64)> {
        let url = format!("{}/sendPoll", self.base);
        let body = serde_json::json!({
            "chat_id": chat_id,
            "question": question,
            "options": options.iter().map(|o| serde_json::json!({ "text": o })).collect::<Vec<_>>(),
            "is_anonymous": false,
            "allows_multiple_answers": true,
        });
        let resp: ApiResponse<serde_json::Value> = self
            .http
            .post(&url)
            .json(&body)
            .send()
            .await
            .map_err(reqwest::Error::without_url)
            .context("sendPoll request")?
            .json()
            .await
            .context("sendPoll body parse")?;
        if !resp.ok {
            anyhow::bail!(
                "telegram sendPoll returned ok=false: {}",
                resp.description.unwrap_or_default()
            );
        }
        let sent = resp.result.context("sendPoll without a message")?;
        let poll_id = sent
            .pointer("/poll/id")
            .and_then(serde_json::Value::as_str)
            .context("sendPoll without a poll id")?
            .to_string();
        let message_id = sent
            .get("message_id")
            .and_then(serde_json::Value::as_i64)
            .context("sendPoll without a message id")?;
        Ok((poll_id, message_id))
    }

    /// Close poll message `message_id`, so it is answered once.
    /// Best-effort.
    pub async fn stop_poll(&self, chat_id: i64, message_id: i64) {
        let url = format!("{}/stopPoll", self.base);
        let body = serde_json::json!({ "chat_id": chat_id, "message_id": message_id });
        if let Err(e) = self.http.post(&url).json(&body).send().await {
            tracing::debug!(error = %e.without_url(), "stopPoll failed (non-fatal)");
        }
    }

    /// Take the buttons off message `message_id`, so they are pressed once.
    /// Best-effort.
    pub async fn clear_buttons(&self, chat_id: i64, message_id: i64) {
        let url = format!("{}/editMessageReplyMarkup", self.base);
        let body = serde_json::json!({
            "chat_id": chat_id,
            "message_id": message_id,
            "reply_markup": { "inline_keyboard": [] },
        });
        if let Err(e) = self.http.post(&url).json(&body).send().await {
            tracing::debug!(error = %e.without_url(), "editMessageReplyMarkup failed (non-fatal)");
        }
    }

    /// The bot itself (`getMe`): its id and username, by which a group
    /// message is known to address it (havn issue #263).
    pub async fn get_me(&self) -> Result<TelegramUser> {
        let url = format!("{}/getMe", self.base);
        let resp: ApiResponse<TelegramUser> = self
            .http
            .get(&url)
            .send()
            .await
            .map_err(reqwest::Error::without_url)
            .context("getMe request")?
            .json()
            .await
            .context("getMe body parse")?;
        match (resp.ok, resp.result) {
            (true, Some(me)) => Ok(me),
            _ => anyhow::bail!(
                "telegram getMe returned ok=false: {}",
                resp.description.unwrap_or_default()
            ),
        }
    }

    /// Whether `e`, from [`Self::get_me`], is Telegram refusing the token,
    /// which trying again does not mend.
    pub fn refused_token(e: &anyhow::Error) -> bool {
        let said = format!("{e:#}");
        said.contains("Unauthorized") || said.contains("Not Found")
    }

    /// Set the bot's command menu (`setMyCommands`) to `commands`: those
    /// Telegram takes (a name of 1-32 lowercase letters, digits and `_`,
    /// a description of 1-256 characters), at most 100. How many were set.
    pub async fn set_my_commands(
        &self,
        commands: &[crate::types::ChannelCommand],
    ) -> Result<usize> {
        let menu = menu_entries(commands);
        let url = format!("{}/setMyCommands", self.base);
        let body = serde_json::json!({ "commands": menu });
        let resp: ApiResponse<serde_json::Value> = self
            .http
            .post(&url)
            .json(&body)
            .send()
            .await
            .map_err(reqwest::Error::without_url)
            .context("setMyCommands request")?
            .json()
            .await
            .context("setMyCommands body parse")?;
        if !resp.ok {
            anyhow::bail!(
                "telegram setMyCommands returned ok=false: {}",
                resp.description.unwrap_or_default()
            );
        }
        Ok(menu.len())
    }
}

/// `buttons` as Telegram's inline keyboard: a press sends back the
/// button's `data` (at most 64 bytes, which the gateway keeps to).
fn keyboard(buttons: &[Vec<crate::types::Button>]) -> serde_json::Value {
    let rows: Vec<Vec<serde_json::Value>> = buttons
        .iter()
        .map(|row| {
            row.iter()
                .filter(|b| !b.data.is_empty() && b.data.len() <= 64)
                .map(|b| serde_json::json!({ "text": b.label, "callback_data": b.data }))
                .collect()
        })
        .filter(|row: &Vec<serde_json::Value>| !row.is_empty())
        .collect();
    serde_json::json!({ "inline_keyboard": rows })
}

/// The entries of `commands` Telegram's menu takes, as it takes them.
fn menu_entries(commands: &[crate::types::ChannelCommand]) -> Vec<serde_json::Value> {
    commands
        .iter()
        .filter(|c| {
            (1..=32).contains(&c.name.len())
                && c.name
                    .chars()
                    .all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || ch == '_')
        })
        .filter_map(|c| {
            let description: String = c.description.trim().chars().take(256).collect();
            (!description.is_empty())
                .then(|| serde_json::json!({ "command": c.name, "description": description }))
        })
        .take(100)
        .collect()
}

/// UTF-16 units Telegram takes in one message, and in one caption.
pub const MESSAGE_UNITS: usize = 4096;
pub const CAPTION_UNITS: usize = 1024;

/// `text` in parts of at most `max` UTF-16 units (Telegram's measure),
/// in order: each part ends at the last paragraph break in its second
/// half, else the last line break, else the last space, else at `max`;
/// a character is never split. The break between two parts is dropped
/// (only the line breaks of a break at a line, so the next line keeps its
/// indentation); a part that would be blank is left out.
pub fn split_text(text: &str, max: usize) -> Vec<String> {
    let mut parts = Vec::new();
    let mut rest = text;
    loop {
        // The byte where `max` units end, if the rest goes past it.
        let mut units = 0;
        let mut end = None;
        for (at, c) in rest.char_indices() {
            units += c.len_utf16();
            if units > max {
                end = Some(at);
                break;
            }
        }
        let Some(end) = end else {
            if !rest.trim().is_empty() {
                parts.push(rest.trim_end().to_string());
            }
            return parts;
        };
        let window = &rest[..end];
        let half = window.len() / 2;
        let (cut, next, at_space) = ["\n\n", "\n", " "]
            .iter()
            .find_map(|sep| {
                window
                    .rfind(sep)
                    .filter(|&at| at >= half && at > 0)
                    .map(|at| (at, at + sep.len(), *sep == " "))
            })
            .unwrap_or((end, end, false));
        let part = rest[..cut].trim_end();
        if !part.is_empty() {
            parts.push(part.to_string());
        }
        rest = if at_space {
            rest[next..].trim_start_matches(' ')
        } else {
            rest[next..].trim_start_matches(['\n', '\r'])
        };
    }
}

/// `caption` as Telegram keeps it with a file: what fits, and the rest
/// to send as messages after it.
pub fn caption_parts(caption: &str) -> (String, Option<String>) {
    let mut parts = crate::markdown::split(caption, CAPTION_UNITS).into_iter();
    let first = parts.next().unwrap_or_default();
    let rest: Vec<String> = parts.collect();
    (first, (!rest.is_empty()).then(|| rest.join("\n")))
}

/// Whether Telegram refused a message for its formatting ("can't parse
/// entities"), so it can go again as plain text.
pub fn refused_markup(e: &anyhow::Error) -> bool {
    format!("{e:#}").contains("can't parse entities")
}

/// Whether Telegram answered and refused, as opposed to a request that
/// failed on the way, which it may have taken.
pub fn refused(e: &anyhow::Error) -> bool {
    format!("{e:#}").contains("returned ok=false")
}

/// How a file goes to Telegram (issue #15): an image it can show as a
/// photo, a GIF as an animation, anything else as a document.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileKind {
    Document,
    Photo,
    Animation,
}

impl FileKind {
    fn method(self) -> &'static str {
        match self {
            Self::Document => "sendDocument",
            Self::Photo => "sendPhoto",
            Self::Animation => "sendAnimation",
        }
    }

    fn field(self) -> &'static str {
        match self {
            Self::Document => "document",
            Self::Photo => "photo",
            Self::Animation => "animation",
        }
    }
}

/// What `sendPhoto` takes: at most 10 MB, width and height at most
/// 10000 together, and at most 20 times as wide as high or high as wide.
const MAX_PHOTO_BYTES: usize = 10 * 1024 * 1024;
const MAX_PHOTO_SIDES: u32 = 10_000;
const MAX_PHOTO_RATIO: u32 = 20;

/// How file `bytes` go: by what they are, not by their name.
pub fn file_kind(bytes: &[u8]) -> FileKind {
    if bytes.starts_with(b"GIF87a") || bytes.starts_with(b"GIF89a") {
        return FileKind::Animation;
    }
    let image = bytes.starts_with(b"\xFF\xD8\xFF")
        || bytes.starts_with(b"\x89PNG\r\n\x1a\n")
        || (bytes.len() >= 12 && &bytes[..4] == b"RIFF" && &bytes[8..12] == b"WEBP");
    if !image || bytes.len() > MAX_PHOTO_BYTES {
        return FileKind::Document;
    }
    match image_size(bytes) {
        Some((w, h))
            if w > 0
                && h > 0
                && u64::from(w) + u64::from(h) <= u64::from(MAX_PHOTO_SIDES)
                && w.max(h) <= w.min(h).saturating_mul(MAX_PHOTO_RATIO) =>
        {
            FileKind::Photo
        }
        _ => FileKind::Document,
    }
}

/// The width and height of a PNG, JPEG or WebP image, read from its
/// header.
fn image_size(bytes: &[u8]) -> Option<(u32, u32)> {
    let be16 = |at: usize| -> Option<u32> {
        Some(u32::from(u16::from_be_bytes(
            bytes.get(at..at + 2)?.try_into().ok()?,
        )))
    };
    let le = |at: usize, n: usize| -> Option<u32> {
        let slice = bytes.get(at..at + n)?;
        Some(
            slice
                .iter()
                .rev()
                .fold(0_u32, |acc, b| (acc << 8) | u32::from(*b)),
        )
    };
    if bytes.starts_with(b"\x89PNG\r\n\x1a\n") {
        if bytes.get(12..16)? != b"IHDR" {
            return None;
        }
        let be32 = |at: usize| -> Option<u32> {
            Some(u32::from_be_bytes(bytes.get(at..at + 4)?.try_into().ok()?))
        };
        return Some((be32(16)?, be32(20)?));
    }
    if bytes.starts_with(b"\xFF\xD8") {
        let mut at = 2;
        while at + 9 < bytes.len() {
            if bytes[at] != 0xFF {
                return None;
            }
            let marker = bytes[at + 1];
            if marker == 0xFF {
                at += 1;
                continue;
            }
            if (0xD0..=0xD9).contains(&marker) || marker == 0x01 {
                at += 2;
                continue;
            }
            if (0xC0..=0xCF).contains(&marker) && !matches!(marker, 0xC4 | 0xC8 | 0xCC) {
                return Some((be16(at + 7)?, be16(at + 5)?));
            }
            at += 2 + usize::try_from(be16(at + 2)?).ok()?;
        }
        return None;
    }
    if bytes.len() >= 30 && &bytes[..4] == b"RIFF" && &bytes[8..12] == b"WEBP" {
        return match &bytes[12..16] {
            b"VP8 " => Some((le(26, 2)? & 0x3FFF, le(28, 2)? & 0x3FFF)),
            b"VP8L" => {
                let bits = le(21, 4)?;
                Some(((bits & 0x3FFF) + 1, ((bits >> 14) & 0x3FFF) + 1))
            }
            b"VP8X" => Some((le(24, 3)? + 1, le(27, 3)? + 1)),
            _ => None,
        };
    }
    None
}

#[derive(Debug, Deserialize)]
struct ApiResponse<T> {
    ok: bool,
    #[serde(default)]
    description: Option<String>,
    #[serde(default)]
    result: Option<T>,
    /// For a 429: how long to wait before trying again.
    #[serde(default)]
    parameters: Option<ResponseParameters>,
}

#[derive(Debug, Deserialize, Default)]
struct ResponseParameters {
    #[serde(default)]
    retry_after: Option<u64>,
}

/// Times a message is tried when Telegram asks the bot to slow down, and
/// the longest wait it is given.
const SEND_TRIES: usize = 3;
const MAX_RETRY_AFTER_SECONDS: u64 = 60;

/// An update as this bot reads it; one whose message it cannot read keeps
/// its id, so the poll moves past it, and no message.
fn read_update(value: serde_json::Value) -> Option<Update> {
    let update_id = value.get("update_id")?.as_i64()?;
    match serde_json::from_value::<Update>(value) {
        Ok(update) => Some(update),
        Err(e) => {
            tracing::warn!(update_id, error = %e, "passing over a Telegram update this bot cannot read");
            Some(Update {
                update_id,
                message: None,
                callback_query: None,
                poll_answer: None,
            })
        }
    }
}

#[derive(Debug, Deserialize)]
#[allow(clippy::struct_field_names)]
pub struct Update {
    pub update_id: i64,
    #[serde(default)]
    pub message: Option<Message>,
    /// A press of a button under one of the bot's messages.
    #[serde(default)]
    pub callback_query: Option<CallbackQuery>,
    /// A vote in one of the bot's polls.
    #[serde(default)]
    pub poll_answer: Option<PollAnswer>,
}

#[derive(Debug, Deserialize)]
pub struct PollAnswer {
    pub poll_id: String,
    #[serde(default)]
    pub user: Option<TelegramUser>,
    /// The options chosen, from 0; none when the vote was taken back.
    #[serde(default)]
    pub option_ids: Vec<usize>,
}

#[derive(Debug, Deserialize)]
pub struct CallbackQuery {
    pub id: String,
    pub from: TelegramUser,
    /// The message the button is under; only its chat and id when it is
    /// too old for the bot to read.
    #[serde(default)]
    pub message: Option<Message>,
    #[serde(default)]
    pub data: Option<String>,
}

impl CallbackQuery {
    /// The press as a message from the person who pressed it, its text the
    /// button's data, in the chat of the message the button is under.
    pub fn as_message(&self) -> Option<Message> {
        let under = self.message.as_ref()?;
        let data = self.data.as_ref().filter(|d| !d.is_empty())?;
        Some(Message {
            from: Some(self.from.clone()),
            text: Some(data.clone()),
            date: chrono::Utc::now().timestamp(),
            ..Message::bare(under.message_id, under.chat.clone())
        })
    }
}

#[derive(Debug, Deserialize, Default)]
struct FileMeta {
    /// Path returned by Telegram, valid for ~1 hour.
    file_path: String,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Message {
    #[allow(clippy::struct_field_names)]
    pub message_id: i64,
    pub from: Option<TelegramUser>,
    pub chat: Chat,
    /// UNIX seconds. Convert with `chrono::DateTime::from_timestamp`.
    pub date: i64,
    #[serde(default)]
    pub text: Option<String>,
    #[serde(default)]
    pub voice: Option<Voice>,
    #[serde(default)]
    pub location: Option<Location>,
    #[serde(default)]
    pub venue: Option<Venue>,
    #[serde(default)]
    pub contact: Option<Contact>,
    /// The sizes of a photo, smallest first.
    #[serde(default)]
    pub photo: Option<Vec<PhotoSize>>,
    #[serde(default)]
    pub document: Option<Document>,
    /// A photo's or document's caption.
    #[serde(default)]
    pub caption: Option<String>,
    #[serde(default)]
    pub video: Option<MediaFile>,
    #[serde(default)]
    pub video_note: Option<MediaFile>,
    #[serde(default)]
    pub audio: Option<MediaFile>,
    #[serde(default)]
    pub sticker: Option<Sticker>,
    /// A GIF; it comes as a `document` too, which is what is passed on.
    #[serde(default)]
    pub animation: Option<serde_json::Value>,
    #[serde(default)]
    pub poll: Option<Poll>,
    /// Kinds of message a person sends on purpose that this bot cannot
    /// pass on; their contents are not read.
    #[serde(default)]
    pub dice: Option<serde_json::Value>,
    #[serde(default)]
    pub game: Option<serde_json::Value>,
    #[serde(default)]
    pub story: Option<serde_json::Value>,
    #[serde(default)]
    pub paid_media: Option<serde_json::Value>,
    #[serde(default)]
    pub checklist: Option<serde_json::Value>,
    /// The album the message is a part of.
    #[serde(default)]
    pub media_group_id: Option<String>,
    /// The message of this chat it replies to, and the part of it quoted.
    #[serde(default)]
    #[allow(clippy::struct_field_names)]
    pub reply_to_message: Option<Box<Message>>,
    #[serde(default)]
    pub quote: Option<TextQuote>,
    /// The message of another chat it replies to.
    #[serde(default)]
    pub external_reply: Option<ExternalReply>,
    /// Where a forwarded message came from.
    #[serde(default)]
    pub forward_origin: Option<MessageOrigin>,
    /// The chat a message is sent on behalf of: a group's anonymous
    /// admin, or a channel.
    #[serde(default)]
    pub sender_chat: Option<Chat>,
    /// A channel's post forwarded into its discussion group by Telegram.
    #[serde(default)]
    pub is_automatic_forward: bool,
    /// Who was added to the group, in that service message.
    #[serde(default)]
    pub new_chat_members: Vec<TelegramUser>,
    /// Mentions, commands and such in `text`, and in `caption`.
    #[serde(default)]
    pub entities: Option<Vec<MessageEntity>>,
    #[serde(default)]
    pub caption_entities: Option<Vec<MessageEntity>>,
}

/// Something marked in a message's text; only a mention of a user
/// without a username (`text_mention`) is read, by whom it names.
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct MessageEntity {
    #[serde(rename = "type")]
    pub kind: String,
    #[serde(default)]
    pub user: Option<TelegramUser>,
}

impl Message {
    /// A message with nothing in it, of chat `chat`.
    pub fn bare(message_id: i64, chat: Chat) -> Self {
        Self {
            message_id,
            from: None,
            chat,
            date: 0,
            text: None,
            voice: None,
            location: None,
            venue: None,
            contact: None,
            photo: None,
            document: None,
            caption: None,
            video: None,
            video_note: None,
            audio: None,
            sticker: None,
            animation: None,
            poll: None,
            dice: None,
            game: None,
            story: None,
            paid_media: None,
            checklist: None,
            media_group_id: None,
            reply_to_message: None,
            quote: None,
            external_reply: None,
            forward_origin: None,
            sender_chat: None,
            is_automatic_forward: false,
            new_chat_members: Vec::new(),
            entities: None,
            caption_entities: None,
        }
    }

    /// Whether it is a kind of message a person sent on purpose that this
    /// bot cannot pass on (not a service message such as a pin).
    #[must_use]
    pub fn unsupported_content(&self) -> bool {
        self.dice.is_some()
            || self.game.is_some()
            || self.story.is_some()
            || self.paid_media.is_some()
            || self.checklist.is_some()
    }
}

/// A video, video note or audio file: what is needed to fetch it.
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct MediaFile {
    pub file_id: String,
    #[serde(default)]
    pub file_name: Option<String>,
    #[serde(default)]
    pub mime_type: Option<String>,
    #[serde(default)]
    pub file_size: Option<u64>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[allow(clippy::struct_excessive_bools)]
pub struct Sticker {
    #[serde(default)]
    pub file_id: String,
    /// The same for every copy of the sticker, unlike `file_id`.
    #[serde(default)]
    pub file_unique_id: String,
    #[serde(default)]
    pub emoji: Option<String>,
    #[serde(default)]
    pub set_name: Option<String>,
    /// A Lottie (`.tgs`) or a video (`.webm`) sticker, which cannot be
    /// shown as an image; its thumbnail can.
    #[serde(default)]
    pub is_animated: bool,
    #[serde(default)]
    pub is_video: bool,
    #[serde(default)]
    pub thumbnail: Option<PhotoSize>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Poll {
    pub question: String,
    #[serde(default)]
    pub options: Vec<PollOption>,
    #[serde(default)]
    pub total_voter_count: u32,
    #[serde(default)]
    pub is_closed: bool,
    /// "regular" or "quiz".
    #[serde(default, rename = "type")]
    pub kind: String,
    #[serde(default)]
    pub allows_multiple_answers: bool,
    /// For a quiz, its answer, when the bot may know it.
    #[serde(default)]
    pub correct_option_id: Option<usize>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct PollOption {
    pub text: String,
    #[serde(default)]
    pub voter_count: u32,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct TextQuote {
    pub text: String,
}

/// A message of another chat a message replies to: where it came from and
/// what it carries.
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ExternalReply {
    pub origin: MessageOrigin,
    #[serde(flatten)]
    pub content: serde_json::Map<String, serde_json::Value>,
}

/// Who wrote a forwarded message, or one replied to from another chat.
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct MessageOrigin {
    /// `user`, `hidden_user`, `chat` or `channel`.
    #[serde(rename = "type")]
    pub kind: String,
    #[serde(default)]
    pub sender_user: Option<TelegramUser>,
    #[serde(default)]
    pub sender_user_name: Option<String>,
    #[serde(default)]
    pub sender_chat: Option<Chat>,
    #[serde(default)]
    pub chat: Option<Chat>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct PhotoSize {
    pub file_id: String,
    pub width: u32,
    pub height: u32,
    #[serde(default)]
    pub file_size: Option<u64>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Document {
    pub file_id: String,
    #[serde(default)]
    pub file_name: Option<String>,
    #[serde(default)]
    pub mime_type: Option<String>,
    #[serde(default)]
    pub file_size: Option<u64>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Voice {
    pub file_id: String,
    /// Duration in seconds. Telegram always provides this for voice.
    pub duration: u32,
    #[serde(default)]
    pub mime_type: Option<String>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Location {
    pub latitude: f64,
    pub longitude: f64,
    #[serde(default)]
    pub horizontal_accuracy: Option<f32>,
}

/// Telegram "venue" — a named place. Carries a `Location` plus
/// human-readable name / address. Inbound messages with venue are
/// merged: we lift name + address into our `Location` content type.
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Venue {
    pub location: Location,
    pub title: String,
    pub address: String,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Contact {
    pub phone_number: String,
    pub first_name: String,
    #[serde(default)]
    pub last_name: Option<String>,
    #[serde(default)]
    pub vcard: Option<String>,
}

#[derive(Debug, Deserialize, Serialize, Clone, Default)]
pub struct TelegramUser {
    pub id: i64,
    #[serde(default)]
    pub is_bot: bool,
    #[serde(default)]
    pub username: Option<String>,
    #[serde(default)]
    pub first_name: Option<String>,
    #[serde(default)]
    pub last_name: Option<String>,
    /// The bot's own (`getMe`): whether it sees every message in groups,
    /// with privacy mode off.
    #[serde(default)]
    pub can_read_all_group_messages: Option<bool>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Chat {
    pub id: i64,
    /// "private" | "group" | "supergroup" | "channel". v0.2 only
    /// handles `private` (1:1 DM); others are dropped with a log.
    #[serde(rename = "type")]
    pub kind: String,
    /// A group's or channel's name.
    #[serde(default)]
    pub title: Option<String>,
}

/// The file name extension and media type of an image's `bytes`.
pub fn image_type(bytes: &[u8]) -> Option<(&'static str, &'static str)> {
    if bytes.len() >= 12 && &bytes[..4] == b"RIFF" && &bytes[8..12] == b"WEBP" {
        Some(("webp", "image/webp"))
    } else if bytes.starts_with(b"\x89PNG\r\n\x1a\n") {
        Some(("png", "image/png"))
    } else if bytes.starts_with(b"\xFF\xD8\xFF") {
        Some(("jpg", "image/jpeg"))
    } else if bytes.starts_with(b"GIF87a") || bytes.starts_with(b"GIF89a") {
        Some(("gif", "image/gif"))
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn the_menu_keeps_what_telegram_takes() {
        let c = |name: &str, description: &str| crate::types::ChannelCommand {
            name: name.into(),
            description: description.into(),
        };
        let long = "x".repeat(300);
        let entries = super::menu_entries(&[
            c("new", "Start a new chat"),
            c("agent_model", &long),
            c("Bad", "uppercase"),
            c("agent-model", "hyphen"),
            c("empty", "  "),
            c(&"a".repeat(33), "too long"),
        ]);
        let names: Vec<_> = entries
            .iter()
            .map(|e| e["command"].as_str().unwrap())
            .collect();
        assert_eq!(names, ["new", "agent_model"]);
        assert_eq!(entries[1]["description"].as_str().unwrap().len(), 256);
        let many: Vec<_> = (0..150).map(|i| c(&format!("c{i}"), "d")).collect();
        assert_eq!(super::menu_entries(&many).len(), 100);
    }

    #[test]
    fn a_caption_is_cut_by_what_telegram_counts() {
        let wide = "\u{1F600}".repeat(600);
        let (first, rest) = super::caption_parts(&wide);
        assert_eq!(first.encode_utf16().count(), 1024);
        assert_eq!(first.chars().count(), 512);
        assert_eq!(rest.expect("the rest").chars().count(), 88);
    }
}

#[cfg(test)]
mod split_tests {
    use super::*;

    fn units(s: &str) -> usize {
        s.chars().map(char::len_utf16).sum()
    }

    #[test]
    fn a_long_reply_is_split_whole_and_in_order() {
        let para = "word ".repeat(199) + "end.";
        let text = [para.as_str(); 10].join("\n\n");
        let parts = split_text(&text, MESSAGE_UNITS);
        assert_eq!(
            parts.len(),
            3,
            "{:?}",
            parts.iter().map(|p| units(p)).collect::<Vec<_>>()
        );
        assert!(parts.iter().all(|p| units(p) <= MESSAGE_UNITS));
        assert!(
            parts.iter().all(|p| p.ends_with("end.")),
            "split at paragraph breaks"
        );
        assert_eq!(parts.join("\n\n"), text);
    }

    #[test]
    fn chinese_and_emoji_stay_whole() {
        let text = "季度收入增长了很多。".repeat(1000);
        let parts = split_text(&text, MESSAGE_UNITS);
        assert_eq!(parts.len(), 3);
        assert_eq!(parts.concat(), text);
        // An emoji is two UTF-16 units; a cut never splits one.
        let emoji = "\u{1F600}".repeat(5000);
        let parts = split_text(&emoji, MESSAGE_UNITS);
        assert!(parts.iter().all(|p| units(p) <= MESSAGE_UNITS));
        assert_eq!(parts.concat(), emoji);
    }

    #[test]
    fn short_text_is_one_part_and_blank_text_none() {
        assert_eq!(split_text("hi", MESSAGE_UNITS), ["hi"]);
        assert_eq!(split_text("", MESSAGE_UNITS), Vec::<String>::new());
        assert_eq!(
            split_text(&"\r\n".repeat(5000), MESSAGE_UNITS),
            Vec::<String>::new()
        );
    }

    #[test]
    fn exactly_at_the_limit_is_one_part() {
        assert_eq!(split_text(&"a".repeat(10), 10).len(), 1);
        assert_eq!(
            split_text(&"a".repeat(11), 10),
            ["a".repeat(10), "a".to_string()]
        );
    }

    #[test]
    fn a_line_after_a_cut_keeps_its_indentation() {
        let code = format!("{}\n    indented()\n", "x".repeat(15));
        let parts = split_text(&code, 20);
        assert_eq!(parts, ["x".repeat(15), "    indented()".to_string()]);
    }

    #[test]
    fn a_break_only_early_in_the_window_is_not_used() {
        let text = format!("ab {}", "c".repeat(30));
        let parts = split_text(&text, 20);
        assert_eq!(parts[0].chars().count(), 20, "cut at the limit: {parts:?}");
        assert_eq!(parts.concat(), text);
    }

    #[test]
    fn leading_breaks_make_no_empty_part() {
        let text = format!("\n\n\n{}", "word ".repeat(10));
        let parts = split_text(&text, 20);
        assert!(parts.iter().all(|p| !p.trim().is_empty()), "{parts:?}");
    }

    #[test]
    fn a_wait_asked_for_is_read() {
        let r: ApiResponse<serde_json::Value> = serde_json::from_value(serde_json::json!({
            "ok": false, "error_code": 429, "description": "Too Many Requests: retry after 3",
            "parameters": {"retry_after": 3}
        }))
        .expect("response");
        assert_eq!(r.parameters.and_then(|p| p.retry_after), Some(3));
    }

    #[test]
    fn a_long_caption_goes_on_after_the_file() {
        let caption = "line\n".repeat(400);
        let (first, rest) = caption_parts(&caption);
        assert!(units(&first) <= CAPTION_UNITS);
        let rest = rest.expect("the rest");
        assert_eq!(format!("{first}\n{rest}").trim_end(), caption.trim_end());
        assert_eq!(caption_parts("short"), ("short".to_string(), None));
    }

    #[test]
    fn videos_audio_stickers_and_polls_are_read() {
        let m: Message = serde_json::from_value(serde_json::json!({
            "message_id": 1, "date": 0, "chat": {"id": 5, "type": "private"},
            "video": {"file_id": "v", "mime_type": "video/mp4"},
            "audio": {"file_id": "a", "file_name": "song.mp3"},
            "sticker": {"file_id": "s", "emoji": "x", "set_name": "cats", "is_animated": true,
                        "thumbnail": {"file_id": "t", "width": 128, "height": 128}},
            "poll": {"question": "Lunch?", "options": [{"text": "yes", "voter_count": 2}, {"text": "no"}],
                     "total_voter_count": 2, "is_closed": true, "type": "quiz", "correct_option_id": 1},
            "media_group_id": "g1",
            "quote": {"text": "part", "position": 0},
            "reply_to_message": {"message_id": 0, "date": 0, "chat": {"id": 5, "type": "private"},
                                 "from": {"id": 9, "is_bot": true}, "text": "earlier"},
            "forward_origin": {"type": "channel", "date": 0,
                               "chat": {"id": -1, "type": "channel", "title": "News"}},
            "external_reply": {"origin": {"type": "hidden_user", "date": 0, "sender_user_name": "Ann"},
                               "photo": [{"file_id": "p", "width": 1, "height": 1}]}
        }))
        .expect("message");
        assert_eq!(m.video.expect("video").file_id, "v");
        assert_eq!(
            m.audio.expect("audio").file_name.as_deref(),
            Some("song.mp3")
        );
        let sticker = m.sticker.expect("sticker");
        assert_eq!(sticker.set_name.as_deref(), Some("cats"));
        assert!(sticker.is_animated);
        assert_eq!(sticker.thumbnail.expect("thumbnail").file_id, "t");
        let poll = m.poll.expect("poll");
        assert_eq!(poll.options.len(), 2);
        assert_eq!(poll.options[0].voter_count, 2);
        assert_eq!(
            (poll.kind.as_str(), poll.correct_option_id),
            ("quiz", Some(1))
        );
        assert_eq!(m.media_group_id.as_deref(), Some("g1"));
        assert_eq!(m.quote.expect("quote").text, "part");
        let replied = m.reply_to_message.expect("reply");
        assert!(replied.from.expect("from").is_bot);
        let origin = m.forward_origin.expect("forward");
        assert_eq!(origin.chat.and_then(|c| c.title).as_deref(), Some("News"));
        let external = m.external_reply.expect("external");
        assert_eq!(external.origin.sender_user_name.as_deref(), Some("Ann"));
        assert!(external.content.contains_key("photo"));
    }

    fn png(width: u32, height: u32) -> Vec<u8> {
        let mut bytes = b"\x89PNG\r\n\x1a\n\0\0\0\rIHDR".to_vec();
        bytes.extend_from_slice(&width.to_be_bytes());
        bytes.extend_from_slice(&height.to_be_bytes());
        bytes.extend_from_slice(&[8, 2, 0, 0, 0]);
        bytes
    }

    #[test]
    fn images_telegram_can_show_go_as_photos() {
        assert_eq!(file_kind(&png(1280, 960)), FileKind::Photo);
        assert_eq!(
            file_kind(&png(9000, 2000)),
            FileKind::Document,
            "over 10000 together"
        );
        assert_eq!(
            file_kind(&png(4200, 200)),
            FileKind::Document,
            "over 20 to 1"
        );
        assert_eq!(file_kind(&png(0, 10)), FileKind::Document);
        assert_eq!(
            file_kind(&png(0x8000_0000, 0x8000_0000)),
            FileKind::Document,
            "no overflow"
        );
        let mut big = png(100, 100);
        big.resize(MAX_PHOTO_BYTES + 1, 0);
        assert_eq!(file_kind(&big), FileKind::Document, "over 10 MB");
        assert_eq!(file_kind(b"GIF89a\x01\0\x01\0"), FileKind::Animation);
        assert_eq!(file_kind(b"%PDF-1.7"), FileKind::Document);
        assert_eq!(
            file_kind(b"\x89PNG\r\n\x1a\n"),
            FileKind::Document,
            "no header"
        );
    }

    #[test]
    fn jpeg_and_webp_sizes_are_read() {
        // SOI, an APP0 segment, then SOF0 with height 960 and width 1280.
        let jpeg = [
            0xFF, 0xD8, 0xFF, 0xE0, 0x00, 0x04, 0x00, 0x00, 0xFF, 0xC0, 0x00, 0x11, 0x08, 0x03,
            0xC0, 0x05, 0x00, 0x03, 0x01, 0x22, 0x00,
        ];
        assert_eq!(image_size(&jpeg), Some((1280, 960)));
        assert_eq!(file_kind(&jpeg), FileKind::Photo);
        let mut webp = b"RIFF\0\0\0\0WEBPVP8X\x0a\0\0\0\0\0\0\0".to_vec();
        webp.extend_from_slice(&[0xFF, 0x04, 0x00, 0xBF, 0x03, 0x00]);
        assert_eq!(image_size(&webp), Some((1280, 960)));
        let mut lossy = b"RIFF\0\0\0\0WEBPVP8 \0\0\0\0\0\0\0\x9d\x01\x2a".to_vec();
        lossy.extend_from_slice(&[0x00, 0x05, 0xC0, 0x03]);
        assert_eq!(image_size(&lossy), Some((1280, 960)));
    }

    #[test]
    fn a_formatting_refusal_is_told_apart() {
        assert!(refused_markup(&anyhow::anyhow!(
            "telegram sendMessage returned ok=false: Bad Request: can't parse entities: Unsupported start tag"
        )));
        assert!(!refused_markup(&anyhow::anyhow!(
            "telegram sendMessage returned ok=false: Bad Request: message is too long"
        )));
        assert!(refused(&anyhow::anyhow!(
            "telegram sendPhoto returned ok=false: Bad Request: PHOTO_INVALID_DIMENSIONS"
        )));
        assert!(!refused(
            &anyhow::anyhow!("operation timed out").context("sendPhoto request")
        ));
    }

    #[test]
    fn an_update_that_cannot_be_read_is_passed_over() {
        let good = serde_json::json!({"update_id": 7, "message": {
            "message_id": 1, "date": 0, "chat": {"id": 5, "type": "private"}, "text": "hi"}});
        let bad = serde_json::json!({"update_id": 8, "message": {"message_id": "x"}});
        let read = read_update(good).expect("good");
        assert_eq!(read.update_id, 7);
        assert!(read.message.is_some());
        let read = read_update(bad).expect("bad");
        assert_eq!(read.update_id, 8);
        assert!(read.message.is_none());
        assert!(read_update(serde_json::json!({"nothing": 1})).is_none());
    }

    #[test]
    fn a_press_reads_as_its_sender_writing_its_data() {
        let update: Update = serde_json::from_value(serde_json::json!({
            "update_id": 9,
            "callback_query": {
                "id": "q1", "from": {"id": 42, "first_name": "Ann"},
                "message": {"message_id": 77, "date": 0, "chat": {"id": 42, "type": "private"},
                            "text": "A step waits for approval"},
                "data": "/approve r1"
            }
        }))
        .expect("update");
        let press = update.callback_query.expect("press");
        let m = press.as_message().expect("message");
        assert_eq!(m.text.as_deref(), Some("/approve r1"));
        assert_eq!(m.from.expect("from").id, 42);
        assert_eq!((m.chat.id, m.message_id), (42, 77));
        assert!(m.reply_to_message.is_none());
        // Too old to read: only the chat and id come, which is enough.
        let old: CallbackQuery = serde_json::from_value(serde_json::json!({
            "id": "q2", "from": {"id": 42},
            "message": {"message_id": 5, "date": 0, "chat": {"id": 42, "type": "private"}},
            "data": "/deny r1"
        }))
        .expect("press");
        assert_eq!(
            old.as_message().expect("message").text.as_deref(),
            Some("/deny r1")
        );
        let empty: CallbackQuery =
            serde_json::from_value(serde_json::json!({"id": "q3", "from": {"id": 1}}))
                .expect("press");
        assert!(empty.as_message().is_none());
    }

    #[test]
    fn a_vote_is_read() {
        let update: Update = serde_json::from_value(serde_json::json!({
            "update_id": 3,
            "poll_answer": {"poll_id": "p9", "user": {"id": 42}, "option_ids": [0, 2]}
        }))
        .expect("update");
        let vote = update.poll_answer.expect("vote");
        assert_eq!(vote.poll_id, "p9");
        assert_eq!(vote.option_ids, [0, 2]);
        assert_eq!(vote.user.expect("user").id, 42);
    }

    #[test]
    fn buttons_become_an_inline_keyboard() {
        let b = |label: &str, data: &str| crate::types::Button {
            label: label.into(),
            data: data.into(),
        };
        let markup = keyboard(&[
            vec![b("Approve", "/approve r1"), b("Too long", &"x".repeat(65))],
            vec![b("Empty", "")],
            vec![b("Deny", "/deny r1")],
        ]);
        assert_eq!(
            markup,
            serde_json::json!({"inline_keyboard": [
                [{"text": "Approve", "callback_data": "/approve r1"}],
                [{"text": "Deny", "callback_data": "/deny r1"}]
            ]})
        );
    }

    #[test]
    fn a_dice_roll_is_content_and_a_pin_is_not() {
        let msg = |extra: serde_json::Value| -> Message {
            let mut m = serde_json::json!({"message_id": 1, "date": 0, "chat": {"id": 5, "type": "private"}});
            m.as_object_mut()
                .expect("object")
                .extend(extra.as_object().expect("object").clone());
            serde_json::from_value(m).expect("message")
        };
        assert!(msg(serde_json::json!({"dice": {"emoji": "x", "value": 3}})).unsupported_content());
        assert!(msg(serde_json::json!({"story": {"id": 1}})).unsupported_content());
        assert!(
            !msg(serde_json::json!({"pinned_message": {"message_id": 2}})).unsupported_content()
        );
    }
}
