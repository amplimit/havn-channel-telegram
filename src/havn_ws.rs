//! WebSocket client to a havn gateway's `/api/v1/channel` endpoint
//! (spec §3). Owns one bidirectional WS, exposes a tx/rx pair to the
//! daemon's main loop, and reconnects with backoff on disconnect.
//!
//! The connection-level protocol per spec §3.1:
//! - upgrade with `X-Adapter-Token` header
//! - send `ChannelFrame::Inbound` on user message arrival
//! - receive `ChannelFrame::Outbound` to forward to the platform
//! - resume on every connect from the last outbound seq got (`since` in the
//!   upgrade; across a restart, the last delivered, kept on disk, see
//!   [`Delivered`]): the gateway replays the replies it kept (5 minutes)
//!   that came after it, before any new one
//! - heartbeat via `ChannelFrame::Ping` / `Pong`

use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context as _, Result};
use futures_util::{SinkExt as _, StreamExt as _};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::client::IntoClientRequest as _;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::protocol::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};
use tracing::{info, warn};

use crate::types::ChannelFrame;

/// Minimum / maximum reconnect backoff. The daemon never gives up —
/// transient gateway restarts are expected. 1s start, doubles on each
/// failure to a 60s ceiling.
const RECONNECT_BACKOFF_MIN: Duration = Duration::from_secs(1);
const RECONNECT_BACKOFF_MAX: Duration = Duration::from_secs(60);

/// Outbound queue capacity from the daemon → WS. Generous because
/// individual frames are small and the bottleneck is the WS write.
const OUTBOUND_CAPACITY: usize = 128;
/// Inbound queue capacity from WS → daemon. The Telegram-side
/// outbound dispatcher is the consumer; if it falls behind the WS
/// task applies backpressure naturally.
const INBOUND_CAPACITY: usize = 128;

#[derive(Debug, Clone)]
pub struct WsConfig {
    pub gateway_url: String,
    pub adapter_token: String,
    pub channel_id: String,
    pub account_id: String,
    pub delivered: Arc<Delivered>,
}

/// Where the daemon is in the gateway's numbering of its replies (issue
/// #23): the last one it got, to resume from when it reconnects, and the
/// last one it delivered, kept in a file so a restart resumes from that.
#[derive(Debug)]
pub struct Delivered {
    path: Option<PathBuf>,
    account_id: String,
    got: Mutex<Option<u64>>,
    /// A write that failed is said once.
    warned: std::sync::atomic::AtomicBool,
}

/// What the file holds.
#[derive(serde::Serialize, serde::Deserialize)]
struct Kept {
    account_id: String,
    seq: u64,
}

impl Delivered {
    /// The last seq kept at `path` for `account_id`, if any: none for a
    /// file of another account, or none at all (the first run).
    pub fn load(path: Option<PathBuf>, account_id: &str) -> Self {
        let delivered = path
            .as_ref()
            .and_then(|p| std::fs::read_to_string(p).ok())
            .and_then(|s| serde_json::from_str::<Kept>(&s).ok())
            .filter(|k| k.account_id == account_id)
            .map(|k| k.seq);
        Self {
            path,
            account_id: account_id.to_string(),
            got: Mutex::new(delivered),
            warned: std::sync::atomic::AtomicBool::new(false),
        }
    }

    /// The file of `account_id` in `dir`.
    pub fn file_in(dir: &std::path::Path, account_id: &str) -> PathBuf {
        let name: String = account_id
            .chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                    c
                } else {
                    '_'
                }
            })
            .collect();
        dir.join(format!("{name}.state.json"))
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Option<u64>> {
        self.got
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Where to resume from on connecting: the last reply got, none on the
    /// first run, which has nothing to resume (resuming from 0 would send
    /// again every reply of the last 5 minutes).
    pub fn since(&self) -> Option<u64> {
        *self.lock()
    }

    /// The reply numbered `seq` came from the gateway. The gateway sends a
    /// connection's replies in order and replays only those after the one
    /// it resumes from, so one at or before the last got means it numbers
    /// from somewhere else now (a gateway that counts from 1 restarted, or
    /// its clock went back): taken all the same, from there on.
    pub fn got(&self, seq: u64) {
        let mut got = self.lock();
        if let Some(last) = *got
            && seq <= last
        {
            warn!(
                seq,
                last,
                "the gateway numbers its replies from an earlier point now (it restarted, or \
                 its clock went back); delivering them"
            );
        }
        *got = Some(seq);
    }

    /// The reply numbered `seq` was delivered: kept, the file replaced
    /// whole so a crash leaves the old one or the new one.
    pub fn mark(&self, seq: u64) {
        let Some(path) = &self.path else {
            return;
        };
        let kept = Kept {
            account_id: self.account_id.clone(),
            seq,
        };
        let mut tmp = path.clone().into_os_string();
        tmp.push(".tmp");
        let tmp = PathBuf::from(tmp);
        let written = serde_json::to_string(&kept)
            .map_err(std::io::Error::other)
            .and_then(|body| {
                std::fs::write(&tmp, body)?;
                std::fs::rename(&tmp, path)
            });
        if let Err(e) = written
            && !self.warned.swap(true, std::sync::atomic::Ordering::Relaxed)
        {
            warn!(
                path = %path.display(),
                error = %e,
                "could not keep the last reply delivered; a restart will not resume from it \
                 (set StateDirectory= in the unit, or --state)"
            );
        }
    }
}

/// Handle held by the daemon to push frames into the WS and read
/// incoming frames out. A single `WsClient` survives reconnects —
/// internally it loops over `connect → run → backoff → reconnect`.
pub struct WsClient {
    /// Daemon → WS. Send `ChannelFrame::Inbound` (and the rare
    /// `Ping`) here.
    pub outbound_tx: mpsc::Sender<ChannelFrame>,
    /// WS → daemon. Receive `ChannelFrame::Outbound` (and rare
    /// `Pong`) here.
    pub inbound_rx: mpsc::Receiver<ChannelFrame>,
}

impl WsClient {
    /// Spawn the connection-management task and return a handle. The
    /// task runs forever, reconnecting with backoff. Caller drives
    /// graceful shutdown by dropping its end of `outbound_tx` — that
    /// closes the channel; the WS task observes EOF and exits.
    pub fn spawn(cfg: WsConfig) -> Self {
        let (outbound_tx, outbound_rx) = mpsc::channel::<ChannelFrame>(OUTBOUND_CAPACITY);
        let (inbound_tx, inbound_rx) = mpsc::channel::<ChannelFrame>(INBOUND_CAPACITY);
        tokio::spawn(connection_loop(cfg, outbound_rx, inbound_tx));
        Self {
            outbound_tx,
            inbound_rx,
        }
    }
}

/// Forever-loop: connect, run one session, reconnect on error with
/// exponential backoff. Exits cleanly only when both mpsc ends are
/// closed (caller dropped `outbound_tx`).
async fn connection_loop(
    cfg: WsConfig,
    mut outbound_rx: mpsc::Receiver<ChannelFrame>,
    inbound_tx: mpsc::Sender<ChannelFrame>,
) {
    let mut backoff = RECONNECT_BACKOFF_MIN;
    loop {
        match connect(&cfg).await {
            Ok(ws) => {
                info!(
                    gateway = %cfg.gateway_url,
                    channel = %cfg.channel_id,
                    account = %cfg.account_id,
                    since = ?cfg.delivered.since(),
                    "havn WS connected"
                );
                backoff = RECONNECT_BACKOFF_MIN;

                let outcome = run_session(ws, &mut outbound_rx, &inbound_tx, &cfg.delivered).await;
                match outcome {
                    SessionEnd::OutboundClosed => {
                        info!("havn WS: outbound channel closed; daemon shutting down");
                        return;
                    }
                    SessionEnd::WsClosed => {
                        warn!("havn WS: connection lost; will reconnect");
                    }
                    SessionEnd::WsError(e) => {
                        warn!(error = %e, "havn WS: error on connection; will reconnect");
                    }
                }
            }
            Err(e) => {
                warn!(error = %e, backoff_s = backoff.as_secs(), "havn WS: connect failed; backing off");
            }
        }
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(RECONNECT_BACKOFF_MAX);
    }
}

async fn connect(cfg: &WsConfig) -> Result<WebSocketStream<MaybeTlsStream<TcpStream>>> {
    // Build the WS URL with query params per spec §3.1.
    let mut url = url::Url::parse(&cfg.gateway_url)
        .with_context(|| format!("parse havn_gateway URL {:?}", cfg.gateway_url))?;
    // Replace any existing path; spec mandates `/api/v1/channel`.
    url.set_path("/api/v1/channel");
    url.query_pairs_mut()
        .clear()
        .append_pair("channel", &cfg.channel_id)
        .append_pair("account", &cfg.account_id);
    // The replies kept for it while it was away come first, before any new
    // one: the gateway starts the replay as it registers the connection.
    if let Some(since) = cfg.delivered.since() {
        url.query_pairs_mut()
            .append_pair("since", &since.to_string());
    }

    let mut req = url
        .as_str()
        .into_client_request()
        .context("build WS upgrade request")?;
    req.headers_mut().insert(
        "X-Adapter-Token",
        HeaderValue::from_str(&cfg.adapter_token)
            .context("adapter_token contains non-ASCII or control chars")?,
    );
    let (ws, _resp) = tokio_tungstenite::connect_async(req)
        .await
        .context("WS upgrade")?;
    Ok(ws)
}

enum SessionEnd {
    /// Caller dropped the outbound mpsc — daemon is shutting down.
    OutboundClosed,
    /// WS closed cleanly (server-initiated or peer-initiated).
    WsClosed,
    /// WS errored mid-flight.
    WsError(tokio_tungstenite::tungstenite::Error),
}

async fn run_session(
    ws: WebSocketStream<MaybeTlsStream<TcpStream>>,
    outbound_rx: &mut mpsc::Receiver<ChannelFrame>,
    inbound_tx: &mpsc::Sender<ChannelFrame>,
    delivered: &Delivered,
) -> SessionEnd {
    let (mut sink, mut stream) = ws.split();
    loop {
        tokio::select! {
            // Outbound: daemon → WS
            maybe_frame = outbound_rx.recv() => {
                let Some(frame) = maybe_frame else {
                    // Caller dropped the sender; clean shutdown.
                    let _ = sink.send(Message::Close(None)).await;
                    return SessionEnd::OutboundClosed;
                };
                let s = match serde_json::to_string(&frame) {
                    Ok(s) => s,
                    Err(e) => {
                        warn!(error = %e, "havn WS: serialize ChannelFrame failed; skipping");
                        continue;
                    }
                };
                if let Err(e) = sink.send(Message::Text(s.into())).await {
                    return SessionEnd::WsError(e);
                }
            }
            // Inbound: WS → daemon
            maybe_msg = stream.next() => {
                let msg = match maybe_msg {
                    Some(Ok(m)) => m,
                    Some(Err(e)) => return SessionEnd::WsError(e),
                    None => return SessionEnd::WsClosed,
                };
                match msg {
                    Message::Text(t) => {
                        let parsed: ChannelFrame = match serde_json::from_str(&t) {
                            Ok(f) => f,
                            Err(e) => {
                                warn!(error = %e, "havn WS: malformed frame; skipping");
                                continue;
                            }
                        };
                        if let ChannelFrame::Outbound(o) = &parsed {
                            delivered.got(o.seq);
                        }
                        // If the daemon's consumer is gone, EOF the
                        // session — shutdown is in progress.
                        if inbound_tx.send(parsed).await.is_err() {
                            return SessionEnd::OutboundClosed;
                        }
                    }
                    Message::Close(_) => return SessionEnd::WsClosed,
                    // Control frames — tungstenite auto-responds to
                    // protocol-level Pings; we don't need to.
                    Message::Ping(_) | Message::Pong(_) | Message::Binary(_) | Message::Frame(_) => {}
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    fn dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("havn-tg-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn the_first_run_resumes_from_nothing() {
        let d = dir("first");
        let delivered = Delivered::load(Some(Delivered::file_in(&d, "bot")), "bot");
        assert_eq!(delivered.since(), None);
        let _ = std::fs::remove_dir_all(&d);
    }

    #[test]
    fn a_restart_resumes_from_the_last_delivered() {
        let d = dir("restart");
        let path = Delivered::file_in(&d, "bot");
        let delivered = Delivered::load(Some(path.clone()), "bot");
        for seq in [41, 42, 43] {
            delivered.got(seq);
        }
        delivered.mark(41);
        delivered.mark(42);
        // A reconnect resumes from the last got: 43 is waiting to go out.
        assert_eq!(delivered.since(), Some(43));
        // A restart, from the last delivered: 43 was lost with the process.
        let again = Delivered::load(Some(path.clone()), "bot");
        assert_eq!(again.since(), Some(42));
        // A file of another account is not this one's.
        assert_eq!(Delivered::load(Some(path), "other").since(), None);
        let _ = std::fs::remove_dir_all(&d);
    }

    #[test]
    fn a_gateway_numbering_from_earlier_is_followed() {
        let d = dir("earlier");
        let path = Delivered::file_in(&d, "bot");
        let delivered = Delivered::load(Some(path.clone()), "bot");
        delivered.got(500);
        delivered.mark(500);
        // A gateway counting from 1 restarted.
        delivered.got(1);
        delivered.mark(1);
        assert_eq!(delivered.since(), Some(1));
        assert_eq!(Delivered::load(Some(path), "bot").since(), Some(1));
        let _ = std::fs::remove_dir_all(&d);
    }

    #[test]
    fn accounts_keep_files_of_their_own() {
        let d = dir("accounts");
        assert_ne!(Delivered::file_in(&d, "a/b"), Delivered::file_in(&d, "c"));
        assert_eq!(
            Delivered::file_in(&d, "../x").file_name().unwrap(),
            "___x.state.json"
        );
        let _ = std::fs::remove_dir_all(&d);
    }

    #[test]
    fn a_file_that_cannot_be_written_keeps_the_seqs_in_memory() {
        let delivered = Delivered::load(
            Some(PathBuf::from("/nonexistent/dir/bot.state.json")),
            "bot",
        );
        delivered.got(5);
        delivered.mark(5);
        delivered.mark(6);
        assert_eq!(delivered.since(), Some(5));
        let d = dir("garbled");
        let garbled = d.join("bot.state.json");
        std::fs::write(&garbled, "not json").unwrap();
        assert_eq!(Delivered::load(Some(garbled), "bot").since(), None);
        let _ = std::fs::remove_dir_all(&d);
    }
}
