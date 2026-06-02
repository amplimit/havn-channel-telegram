//! WebSocket client to a havn gateway's `/api/v1/channel` endpoint
//! (spec §3). Owns one bidirectional WS, exposes a tx/rx pair to the
//! daemon's main loop, and reconnects with backoff on disconnect.
//!
//! The connection-level protocol per spec §3.1:
//! - upgrade with `X-Adapter-Token` header
//! - send `ChannelFrame::Inbound` on user message arrival
//! - receive `ChannelFrame::Outbound` to forward to the platform
//! - send `ChannelFrame::Resume{since_seq}` on reconnect (until
//!   havn's Layer D2 ships, gateway warn-drops these — the adapter
//!   should accept that data is lost across reconnects)
//! - heartbeat via `ChannelFrame::Ping` / `Pong`

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
}

/// Handle held by the daemon to push frames into the WS and read
/// incoming frames out. A single `WsClient` survives reconnects —
/// internally it loops over `connect → run → backoff → reconnect`.
pub struct WsClient {
    /// Daemon → WS. Send `ChannelFrame::Inbound` (and the rare
    /// `Resume` / `Ping`) here.
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
                    "havn WS connected"
                );
                backoff = RECONNECT_BACKOFF_MIN;
                let outcome = run_session(ws, &mut outbound_rx, &inbound_tx).await;
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
