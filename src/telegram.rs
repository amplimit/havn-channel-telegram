//! Thin Telegram Bot API client — only the endpoints this adapter
//! needs: `getUpdates` (long-poll inbound), `getFile` and the file
//! download (voice, photos, documents), `sendMessage` and
//! `sendDocument` (deliver outbound), and `sendChatAction` (typing
//! indicator).
//!
//! Reference: <https://core.telegram.org/bots/api>. Bot tokens look
//! like `<bot_id>:<random>`; the API base is
//! `https://api.telegram.org/bot<token>/`.
//!
//! We deliberately don't pull in `teloxide` or another full SDK —
//! this daemon's surface is tiny, and a small bespoke client is
//! easier to audit / lock down than a multi-thousand-LOC dep.

use anyhow::{Context as _, Result};
use serde::{Deserialize, Serialize};

#[derive(Clone)]
pub struct Telegram {
    base: String,
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
    pub fn new(bot_token: &str, poll_timeout_seconds: u64) -> Result<Self> {
        let http = reqwest::Client::builder()
            // getUpdates long-poll timeout + 5s slack so the HTTP
            // client doesn't fire before Telegram does.
            .timeout(std::time::Duration::from_secs(poll_timeout_seconds + 5))
            .build()
            .context("build reqwest client")?;
        Ok(Self {
            base: format!("https://api.telegram.org/bot{bot_token}"),
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
        let resp: ApiResponse<Vec<Update>> = self
            .http
            .get(&url)
            .query(&[
                ("offset", offset.to_string()),
                ("timeout", self.poll_timeout_seconds.to_string()),
                // Don't request `edited_message` / `channel_post` /
                // `inline_query` — agents respond to direct messages
                // only in v0.2. Future expansion can widen this.
                ("allowed_updates", "[\"message\"]".to_string()),
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
        Ok(resp.result.unwrap_or_default())
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

    /// Download the raw bytes of a Telegram-hosted file. URL format
    /// is `https://api.telegram.org/file/bot<token>/<file_path>`.
    /// Used for voice messages on the way to Whisper.
    pub async fn download_file(&self, file_path: &str) -> Result<Vec<u8>> {
        let url = format!(
            "https://api.telegram.org/file/{}/{file_path}",
            // self.base is `https://api.telegram.org/bot<token>`; the
            // file URL replaces `bot<token>` with `file/bot<token>`.
            self.base.trim_start_matches("https://api.telegram.org/")
        );
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
        let mut bytes = Vec::new();
        while let Some(chunk) = resp.chunk().await.context("download_file body")? {
            bytes.extend_from_slice(&chunk);
            if bytes.len() > crate::gateway::MAX_FILE_BYTES {
                anyhow::bail!("the file is larger than the gateway takes");
            }
        }
        Ok(bytes)
    }

    /// Send a file as a document, with its caption (Telegram keeps 1024
    /// characters of one).
    pub async fn send_document(
        &self,
        chat_id: i64,
        bytes: Vec<u8>,
        filename: &str,
        caption: Option<&str>,
        reply_to_message_id: Option<i64>,
    ) -> Result<()> {
        let url = format!("{}/sendDocument", self.base);
        let part = reqwest::multipart::Part::bytes(bytes).file_name(filename.to_string());
        let mut form = reqwest::multipart::Form::new()
            .text("chat_id", chat_id.to_string())
            .part("document", part);
        if let Some(caption) = caption.filter(|c| !c.trim().is_empty()) {
            form = form.text("caption", caption.to_string());
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
            .context("sendDocument request")?
            .json()
            .await
            .context("sendDocument body parse")?;
        if !resp.ok {
            anyhow::bail!(
                "telegram sendDocument returned ok=false: {}",
                resp.description.unwrap_or_default()
            );
        }
        Ok(())
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

    /// Send a plain text message. `reply_to_message_id` threads under
    /// the original (Telegram-side equivalent of `in_reply_to`).
    pub async fn send_message(
        &self,
        chat_id: i64,
        text: &str,
        reply_to_message_id: Option<i64>,
    ) -> Result<()> {
        let url = format!("{}/sendMessage", self.base);
        let mut body = serde_json::json!({
            "chat_id": chat_id,
            "text": text,
        });
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
    let mut parts = split_text(caption, CAPTION_UNITS).into_iter();
    let first = parts.next().unwrap_or_default();
    let rest: Vec<String> = parts.collect();
    (first, (!rest.is_empty()).then(|| rest.join("\n")))
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

#[derive(Debug, Deserialize)]
pub struct Update {
    pub update_id: i64,
    #[serde(default)]
    pub message: Option<Message>,
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
}

impl Message {
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
pub struct Sticker {
    #[serde(default)]
    pub emoji: Option<String>,
    #[serde(default)]
    pub set_name: Option<String>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Poll {
    pub question: String,
    #[serde(default)]
    pub options: Vec<PollOption>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct PollOption {
    pub text: String,
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

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct TelegramUser {
    pub id: i64,
    #[serde(default)]
    pub username: Option<String>,
    #[serde(default)]
    pub first_name: Option<String>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Chat {
    pub id: i64,
    /// "private" | "group" | "supergroup" | "channel". v0.2 only
    /// handles `private` (1:1 DM); others are dropped with a log.
    #[serde(rename = "type")]
    pub kind: String,
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
            "sticker": {"emoji": "x", "set_name": "cats"},
            "poll": {"question": "Lunch?", "options": [{"text": "yes"}, {"text": "no"}]}
        }))
        .expect("message");
        assert_eq!(m.video.expect("video").file_id, "v");
        assert_eq!(
            m.audio.expect("audio").file_name.as_deref(),
            Some("song.mp3")
        );
        assert_eq!(
            m.sticker.expect("sticker").set_name.as_deref(),
            Some("cats")
        );
        assert_eq!(m.poll.expect("poll").options.len(), 2);
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
