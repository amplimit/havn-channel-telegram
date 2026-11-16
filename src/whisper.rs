//! Whisper STT client -- calls the OpenAI-compatible
//! `/v1/audio/transcriptions` endpoint to turn voice messages into
//! text. Held by the daemon, used by the inbound translation path
//! when a Telegram Update carries a `voice` payload.
//!
//! Why a hosted Whisper endpoint and not a local model: in 2026
//! production self-hosters increasingly run Whisper locally via
//! faster-whisper or whisper.cpp, but for the v0.2 first cut we
//! pick the lowest-friction option -- operator pastes an API key in
//! the daemon's TOML, daemon makes one HTTPS call per voice message.
//! Local-model support follows by swapping the implementation
//! behind a trait when a real operator asks.

use anyhow::{Context as _, Result};
use reqwest::multipart;
use serde::Deserialize;

/// Whisper API endpoint. OpenAI-compatible servers (Groq, Together,
/// self-hosted whisper.cpp + openai-shim) accept the same body shape
/// at the same path — operators with a non-OpenAI provider just
/// override `WHISPER_BASE_URL`.
const DEFAULT_BASE: &str = "https://api.openai.com/v1";
/// Default model when the operator doesn't override. `OpenAI` ships
/// `whisper-1` and `gpt-4o-transcribe`; Groq exposes
/// `whisper-large-v3` / `whisper-large-v3-turbo`. Default to the
/// long-standing model so a bare config "just works".
const DEFAULT_MODEL: &str = "whisper-1";

#[derive(Debug, Clone)]
pub struct Whisper {
    base: String,
    model: String,
    api_key: String,
    http: reqwest::Client,
}

impl Whisper {
    pub fn new(api_key: String, base_url: Option<String>, model: Option<String>) -> Result<Self> {
        let http = reqwest::Client::builder()
            // Whisper is fast for short voice messages but slow for
            // long ones. 60s ceiling — typical chat voice is < 30s.
            .timeout(std::time::Duration::from_secs(60))
            .build()
            .context("build reqwest client for Whisper")?;
        Ok(Self {
            base: base_url.unwrap_or_else(|| DEFAULT_BASE.into()),
            model: model.unwrap_or_else(|| DEFAULT_MODEL.into()),
            api_key,
            http,
        })
    }

    /// Transcribe raw audio bytes. `filename` is hinted to the API
    /// so the server can sniff the encoding (Telegram voice is
    /// `audio.ogg`; in theory the API auto-detects but the multipart
    /// filename helps).
    pub async fn transcribe(&self, audio: Vec<u8>, filename: &str) -> Result<String> {
        let url = format!("{}/audio/transcriptions", self.base);
        let part = multipart::Part::bytes(audio).file_name(filename.to_string());
        let form = multipart::Form::new()
            .text("model", self.model.clone())
            .part("file", part);
        let resp = self
            .http
            .post(&url)
            .bearer_auth(&self.api_key)
            .multipart(form)
            .send()
            .await
            .context("whisper request")?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("whisper returned {status}: {body}");
        }
        let parsed: TranscriptionResponse = resp.json().await.context("parse whisper response")?;
        Ok(parsed.text)
    }
}

#[derive(Debug, Deserialize)]
struct TranscriptionResponse {
    text: String,
}
