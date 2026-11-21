//! The havn gateway's HTTP side for this adapter (havn issue #106): a file
//! a Telegram user sends goes to the gateway before the message that
//! carries it, and a file an agent sends is fetched from it. Both carry
//! the adapter token, which goes nowhere but the gateway's file paths.

use std::time::Duration;

use anyhow::{Context as _, Result, bail};
use reqwest::header::CONTENT_TYPE;

use crate::types::Attachment;

/// The largest file passed either way: the gateway's limit.
pub const MAX_FILE_BYTES: usize = 25 * 1024 * 1024;
/// Where the gateway takes and serves channel files.
const FILES_PATH: &str = "/api/v1/channel/files";
const ADAPTER_TOKEN_HEADER: &str = "X-Adapter-Token";

#[derive(Clone)]
pub struct Gateway {
    http: reqwest::Client,
    base: url::Url,
    token: String,
    channel: String,
    account: String,
}

impl Gateway {
    /// For the gateway whose WebSocket is at `ws_url`, as `havn_gateway`
    /// names it.
    pub fn new(ws_url: &str, token: &str, channel: &str, account: &str) -> Result<Self> {
        let mut base = url::Url::parse(ws_url)
            .with_context(|| format!("parse havn_gateway URL {ws_url:?}"))?;
        let scheme = match base.scheme() {
            "ws" | "http" => "http",
            "wss" | "https" => "https",
            other => bail!("havn_gateway has the scheme {other:?}; use ws:// or wss://"),
        };
        base.set_scheme(scheme)
            .map_err(|()| anyhow::anyhow!("havn_gateway cannot use {scheme}"))?;
        base.set_path("/");
        base.set_query(None);
        base.set_fragment(None);
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(180))
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .context("build reqwest client")?;
        Ok(Self {
            http,
            base,
            token: token.to_string(),
            channel: channel.to_string(),
            account: account.to_string(),
        })
    }

    fn files_url(&self, rest: &str) -> Result<url::Url> {
        let mut url = self
            .base
            .join(&format!("{FILES_PATH}{rest}"))
            .context("build the gateway's file URL")?;
        url.query_pairs_mut()
            .append_pair("channel", &self.channel)
            .append_pair("account", &self.account);
        Ok(url)
    }

    /// Give the gateway file `name` that `sender` sent; the attachment
    /// their message then names.
    pub async fn upload(
        &self,
        sender: &str,
        name: &str,
        media_type: &str,
        bytes: Vec<u8>,
    ) -> Result<Attachment> {
        let mut url = self.files_url("")?;
        url.query_pairs_mut()
            .append_pair("sender", sender)
            .append_pair("name", name);
        // In a group the gateway lets the group in (havn issue #263).
        if let Some(group) = crate::upload_group() {
            url.query_pairs_mut().append_pair("group", &group);
        }
        let bytes = bytes::Bytes::from(bytes);
        // The account's uploads at once are few: when they are all
        // running, the file waits a moment. No other refusal passes soon.
        let mut attempt = 0;
        let (status, body) = loop {
            attempt += 1;
            let resp = self
                .http
                .post(url.clone())
                .header(ADAPTER_TOKEN_HEADER, &self.token)
                .header(CONTENT_TYPE, media_type)
                .timeout(Duration::from_secs(110))
                .body(bytes.clone())
                .send()
                .await
                .context("sending the file to havn")?;
            let status = resp.status();
            if status.is_success() {
                return resp.json().await.context("havn's answer to the file");
            }
            let body = resp.text().await.unwrap_or_default();
            if status != reqwest::StatusCode::CONFLICT
                || !body.contains("still running")
                || attempt == 3
            {
                break (status, body);
            }
            tokio::time::sleep(Duration::from_secs(2 * attempt)).await;
        };
        bail!(
            "havn refused the file ({status}): {}",
            body.chars().take(200).collect::<String>()
        );
    }

    /// The bytes of a file an agent sent, at `path` as its outbound `File`
    /// names it. Only the gateway's own file paths are fetched.
    pub async fn fetch(&self, path: &str) -> Result<Vec<u8>> {
        let file = files_path(path).with_context(|| format!("not a havn file path: {path:?}"))?;
        let mut resp = self
            .http
            .get(self.files_url(&format!("/{file}"))?)
            .header(ADAPTER_TOKEN_HEADER, &self.token)
            .send()
            .await
            .context("fetching the file from havn")?;
        let status = resp.status();
        if !status.is_success() {
            bail!("havn did not give the file ({status})");
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = resp.chunk().await.context("reading the file from havn")? {
            bytes.extend_from_slice(&chunk);
            if bytes.len() > MAX_FILE_BYTES {
                bail!(
                    "the file is larger than {} MB",
                    MAX_FILE_BYTES / (1024 * 1024)
                );
            }
        }
        Ok(bytes)
    }
}

/// `<agent>/<file>` of a path `/api/v1/channel/files/<agent>/<file>`,
/// both plain ids, or `None` for any other path.
fn files_path(path: &str) -> Option<&str> {
    let rest = path.strip_prefix(FILES_PATH)?.strip_prefix('/')?;
    let plain = |s: &str| !s.is_empty() && s.chars().all(|c| c.is_ascii_alphanumeric() || c == '-');
    let mut parts = rest.split('/');
    match (parts.next(), parts.next(), parts.next()) {
        (Some(agent), Some(file), None) if plain(agent) && plain(file) => Some(rest),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_the_gateways_file_paths_are_fetched() {
        let ok = "/api/v1/channel/files/01a1e1a2-f604-713b-bab9-e49a84ffdc6d/01a1e1a3-1b10-762d-bc09-083ebaec9f74";
        assert!(files_path(ok).is_some());
        for bad in [
            "https://attacker.example/x",
            "/api/v1/channel/files/a/b/c",
            "/api/v1/channel/files/a/../b",
            "/api/v1/channel/files/a",
            "/api/v1/channel/filesx/a/b",
            "//attacker.example/api/v1/channel/files/a/b",
            "/api/v1/channel/files/a/b?x=1",
        ] {
            assert!(files_path(bad).is_none(), "{bad}");
        }
    }

    #[test]
    fn the_http_base_follows_the_websocket_url() {
        let g = Gateway::new(
            "wss://havn.example.com/some/path?q=1",
            "t",
            "telegram",
            "bot",
        )
        .expect("gateway");
        let url = g.files_url("/a/b").expect("url");
        assert_eq!(
            url.as_str(),
            "https://havn.example.com/api/v1/channel/files/a/b?channel=telegram&account=bot"
        );
        assert!(Gateway::new("ftp://h", "t", "c", "a").is_err());
    }
}
