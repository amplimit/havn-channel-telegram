//! Wire types matching `havn-proto::channel` (spec §3.1) — forked
//! locally so this pack stays independent of the havn workspace.
//! `cargo build` here works without havn checked out anywhere.
//!
//! Stay in sync with `crates/havn-proto/src/channel.rs` in the
//! havn repo. If havn extends `ChannelFrame`, this file gains the
//! variant; if havn changes a field name, this file matches. Pack
//! version pins to the havn spec version it was built against
//! (currently v0.8 — channel API stabilises here).

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
#[non_exhaustive]
pub enum ChannelFrame {
    Inbound(ChannelInbound),
    Outbound(ChannelOutbound),
    Resume(ChannelResume),
    Ping,
    Pong,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChannelInbound {
    pub seq: u64,
    pub account_id: String,
    pub channel_message_id: String,
    pub sender_id: String,
    pub content: ChannelMessageContent,
    pub timestamp: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub raw: Option<serde_json::Value>,
    /// Files the message carries, each first sent to the gateway's
    /// `POST /api/v1/channel/files` (havn issue #106).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub attachments: Vec<Attachment>,
}

/// A file saved in the agent's inbox, as the gateway answered its upload.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Attachment {
    pub path: String,
    pub name: String,
    pub media_type: String,
    pub size: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub text_path: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChannelOutbound {
    pub seq: u64,
    pub account_id: String,
    pub to: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub in_reply_to: Option<String>,
    pub content: ChannelMessageContent,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChannelResume {
    pub account_id: String,
    pub since_seq: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
#[non_exhaustive]
pub enum ChannelMessageContent {
    Text {
        text: String,
    },
    Image {
        url: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        caption: Option<String>,
    },
    /// Outbound, a file an agent sent: `url` is the gateway's
    /// `/api/v1/channel/files/<agent>/<file>`, fetched with the adapter
    /// token.
    File {
        url: String,
        filename: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        caption: Option<String>,
    },
    Audio {
        transcript: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        duration_seconds: Option<u32>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        url: Option<String>,
    },
    Location {
        latitude: f64,
        longitude: f64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        accuracy_meters: Option<f32>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        name: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        address: Option<String>,
    },
    Contact {
        display_name: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        phone_number: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        vcard: Option<String>,
    },
}
