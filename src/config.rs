//! Operator-supplied configuration — loaded from a TOML file at
//! startup. Mirrors the values the operator put in havn's
//! `[[channels.telegram.accounts]]` block: same `account_id` on both
//! sides, same `adapter_token` (havn stores it age-encrypted, this
//! daemon stores it in plain TOML — keep the file readable only by
//! the daemon's user).

use std::path::Path;

use anyhow::{Context as _, Result};
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    /// Telegram bot token (from `BotFather`). Held only in this daemon
    /// process — havn never sees it.
    pub bot_token: String,

    /// havn gateway WebSocket URL. e.g. `ws://127.0.0.1:8100` or
    /// `wss://havn.example.com`. The daemon appends
    /// `/api/v1/channel?channel=<channel_id>&account=<account_id>`.
    pub havn_gateway: String,

    /// Operator-generated random string. MUST match the value the
    /// operator stored in havn's credentials table under
    /// `(provider = "channel:<channel_id>", name = "<account_id>")`.
    /// havn compares constant-time at WS upgrade.
    pub adapter_token: String,

    /// Channel id matching havn's `[[channels.<channel_id>.accounts]]`
    /// table key. Almost always `"telegram"` — kept configurable so
    /// operators with multi-tenant labelling (e.g. `telegram-prod` /
    /// `telegram-dev`) can disambiguate.
    #[serde(default = "default_channel_id")]
    pub channel_id: String,

    /// Operator-supplied stable account handle, matching havn's
    /// `[[channels.<channel_id>.accounts]] id` value. e.g.
    /// `"alice-tg-bot"`.
    pub account_id: String,

    /// Telegram getUpdates long-poll timeout, seconds. Telegram
    /// permits up to 50; default 25 leaves room for our HTTP client
    /// timeout to fire first when the bot is silent.
    #[serde(default = "default_poll_timeout")]
    pub poll_timeout_seconds: u64,

    /// OpenAI-compatible API key for Whisper STT. Optional —
    /// daemons that don't need to transcribe voice (text-only bots)
    /// leave it unset and any inbound voice message gets dropped
    /// with a warn. The key never leaves this daemon's process;
    /// havn never sees it.
    #[serde(default)]
    pub openai_api_key: Option<String>,

    /// Override the Whisper endpoint (default
    /// `https://api.openai.com/v1`). Operators with a self-hosted
    /// whisper.cpp + openai-shim, Groq, or other OpenAI-compatible
    /// STT provider set this. Empty/unset uses `OpenAI`.
    #[serde(default)]
    pub whisper_base_url: Option<String>,

    /// Override the Whisper model name (default `whisper-1`). `OpenAI`
    /// also offers `gpt-4o-transcribe`; Groq uses
    /// `whisper-large-v3` / `whisper-large-v3-turbo`; etc.
    /// Matches whatever the configured provider accepts at
    /// `POST /audio/transcriptions`'s `model` field.
    #[serde(default)]
    pub whisper_model: Option<String>,
}

fn default_channel_id() -> String {
    "telegram".into()
}

fn default_poll_timeout() -> u64 {
    25
}

impl Config {
    pub fn load(path: &Path) -> Result<Self> {
        let raw =
            std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
        let cfg: Self =
            toml::from_str(&raw).with_context(|| format!("parsing {} as TOML", path.display()))?;
        cfg.validate()?;
        Ok(cfg)
    }

    fn validate(&self) -> Result<()> {
        if self.bot_token.is_empty() {
            anyhow::bail!("bot_token must not be empty");
        }
        if self.havn_gateway.is_empty() {
            anyhow::bail!("havn_gateway must not be empty");
        }
        if self.adapter_token.is_empty() {
            anyhow::bail!("adapter_token must not be empty");
        }
        if self.channel_id.is_empty() {
            anyhow::bail!("channel_id must not be empty");
        }
        if self.account_id.is_empty() {
            anyhow::bail!("account_id must not be empty");
        }
        // Defence in depth: havn's channel_id_codec rejects the `|`
        // separator in any field that round-trips through channel_id.
        // Reject pre-emptively so a misconfigured daemon fails fast
        // at startup, not silently at first inbound.
        for (label, value) in [
            ("channel_id", &self.channel_id),
            ("account_id", &self.account_id),
        ] {
            if value.contains('|') {
                anyhow::bail!(
                    "{label} must not contain `|` (channel_id codec separator); got {value:?}"
                );
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn write_temp(contents: &str) -> std::path::PathBuf {
        let mut p = std::env::temp_dir();
        p.push(format!(
            "havn-channel-telegram-test-{}.toml",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let mut f = std::fs::File::create(&p).unwrap();
        f.write_all(contents.as_bytes()).unwrap();
        p
    }

    #[test]
    fn loads_minimal_config() {
        let p = write_temp(
            r#"
            bot_token = "1234:abc"
            havn_gateway = "ws://127.0.0.1:8100"
            adapter_token = "tok"
            account_id = "alice-tg-bot"
            "#,
        );
        let c = Config::load(&p).unwrap();
        assert_eq!(c.account_id, "alice-tg-bot");
        assert_eq!(c.channel_id, "telegram"); // default
        assert_eq!(c.poll_timeout_seconds, 25);
    }

    #[test]
    fn rejects_pipe_in_account_id() {
        let p = write_temp(
            r#"
            bot_token = "x"
            havn_gateway = "ws://h"
            adapter_token = "x"
            account_id = "alice|bot"
            "#,
        );
        assert!(Config::load(&p).is_err());
    }

    #[test]
    fn rejects_empty_required_field() {
        let p = write_temp(
            r#"
            bot_token = ""
            havn_gateway = "ws://h"
            adapter_token = "x"
            account_id = "alice"
            "#,
        );
        assert!(Config::load(&p).is_err());
    }
}
