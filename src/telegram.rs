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
            .context("download_file request")?;
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
            form = form.text("caption", caption_cut(caption));
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
        if !resp.ok {
            anyhow::bail!(
                "telegram sendMessage returned ok=false: {}",
                resp.description.unwrap_or_default()
            );
        }
        Ok(())
    }
}

/// `caption` cut to the 1024 UTF-16 units Telegram keeps of one.
fn caption_cut(caption: &str) -> String {
    let mut units = 0;
    caption
        .chars()
        .take_while(|c| {
            units += c.len_utf16();
            units <= 1024
        })
        .collect()
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
    /// The sizes of a photo, smallest first.
    #[serde(default)]
    pub photo: Option<Vec<PhotoSize>>,
    #[serde(default)]
    pub document: Option<Document>,
    /// A photo's or document's caption.
    #[serde(default)]
    pub caption: Option<String>,
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
    use super::*;

    #[test]
    fn a_caption_is_cut_by_what_telegram_counts() {
        assert_eq!(caption_cut("short"), "short");
        let wide = "\u{1F600}".repeat(600);
        let cut = caption_cut(&wide);
        assert_eq!(cut.encode_utf16().count(), 1024);
        assert_eq!(cut.chars().count(), 512);
    }
}
