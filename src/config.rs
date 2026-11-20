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

    /// The Bot API's address: Telegram's, or a self-hosted
    /// `telegram-bot-api` server's. `https://`, or `http://` to a
    /// loopback address only, as every URL carries the bot token.
    #[serde(default = "default_bot_api")]
    pub bot_api: String,

    /// No longer used: voice messages go to the gateway, which
    /// transcribes them (havn issue #164). Read only to warn about them.
    #[serde(default)]
    pub openai_api_key: Option<String>,
    #[serde(default)]
    pub whisper_base_url: Option<String>,
    #[serde(default)]
    pub whisper_model: Option<String>,
}

fn default_channel_id() -> String {
    "telegram".into()
}

fn default_poll_timeout() -> u64 {
    25
}

fn default_bot_api() -> String {
    "https://api.telegram.org".into()
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
        let api = url::Url::parse(&self.bot_api)
            .with_context(|| format!("bot_api {:?} is not a URL", self.bot_api))?;
        let loopback = match api.host() {
            Some(url::Host::Domain(name)) => name.eq_ignore_ascii_case("localhost"),
            Some(url::Host::Ipv4(ip)) => ip.is_loopback(),
            Some(url::Host::Ipv6(ip)) => ip.is_loopback(),
            None => false,
        };
        if !(api.scheme() == "https" || api.scheme() == "http" && loopback) {
            anyhow::bail!(
                "bot_api must be https://, or http:// to a loopback address; got {:?}",
                self.bot_api
            );
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
        assert_eq!(c.bot_api, "https://api.telegram.org");
    }

    #[test]
    fn bot_api_is_https_or_loopback() {
        let load = |api: &str| {
            Config::load(&write_temp(&format!(
                r#"
                bot_token = "x"
                havn_gateway = "ws://h"
                adapter_token = "x"
                account_id = "alice"
                bot_api = "{api}"
                "#
            )))
        };
        for api in [
            "https://tg.example.com",
            "http://127.0.0.1:8081",
            "http://localhost:8081/",
            "http://[::1]:8081",
        ] {
            assert!(load(api).is_ok(), "{api}");
        }
        for api in [
            "http://tg.example.com",
            "http://10.0.0.1:8081",
            "ftp://x",
            "api.telegram.org",
        ] {
            assert!(load(api).is_err(), "{api}");
        }
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
