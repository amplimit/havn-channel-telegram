//! Thin Telegram Bot API client — only the two endpoints this
//! adapter needs: `getUpdates` (long-poll inbound) and `sendMessage`
//! (deliver outbound).
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

#[derive(Debug, Clone)]
pub struct Telegram {
    base: String,
    http: reqwest::Client,
    poll_timeout_seconds: u64,
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
        let bytes = self
            .http
            .get(&url)
            .send()
            .await
            .context("download_file request")?
            .bytes()
            .await
            .context("download_file body")?;
        Ok(bytes.to_vec())
    }

    /// Publicly-accessible URL for a Telegram-hosted file. Same shape
    /// as `download_file`'s URL — emitted on the wire as `Audio.url`
    /// so a future multimodal agent could re-fetch the original audio.
    /// Telegram-hosted URLs expire after about 1 hour; callers should
    /// not depend on long-term availability.
    #[must_use]
    pub fn public_file_url(&self, file_path: &str) -> String {
        format!(
            "https://api.telegram.org/file/{}/{file_path}",
            self.base.trim_start_matches("https://api.telegram.org/")
        )
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
        let resp: ApiResponse<serde_json::Value> = self
            .http
            .post(&url)
            .json(&body)
            .send()
            .await
            .context("sendMessage request")?
            .json()
            .await
            .context("sendMessage body parse")?;
        if !resp.ok {
            anyhow::bail!(
                "telegram sendMessage returned ok=false: {}",
                resp.description.unwrap_or_default()
            );
        }
        Ok(())
    }
}

#[derive(Debug, Deserialize)]
struct ApiResponse<T> {
    ok: bool,
    #[serde(default)]
    description: Option<String>,
    #[serde(default)]
    result: Option<T>,
}

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
