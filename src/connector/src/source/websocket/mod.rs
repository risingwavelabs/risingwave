// Copyright 2026 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

pub mod enumerator;
pub mod source;
pub use enumerator::WebsocketSplitEnumerator;
pub mod split;

use std::collections::HashMap;

use anyhow::Context;
use phf::{Set, phf_set};
use serde::Deserialize;
use serde_with::{DisplayFromStr, serde_as};
use tokio_tungstenite::tungstenite::client::ClientRequestBuilder;
use tokio_tungstenite::tungstenite::http::Uri;
use with_options::WithOptions;

use crate::enforce_secret::EnforceSecret;
use crate::error::ConnectorResult;
use crate::source::SourceProperties;
use crate::source::websocket::source::{WebsocketSplit, WebsocketSplitReader};

pub const WEBSOCKET_CONNECTOR: &str = "websocket";

fn default_ping_interval_secs() -> u64 {
    30
}

#[serde_as]
#[derive(Clone, Debug, Deserialize, WithOptions)]
pub struct WebsocketProperties {
    /// The `WebSocket` server URL to connect to,
    /// e.g. `ws://localhost:8080/stream` or `wss://example.com/feed`.
    /// Must be prefixed with either `ws://` or `wss://`.
    pub url: String,

    /// Optional text message sent to the server right after the connection is
    /// established, e.g. a subscribe request. It is sent again after every reconnection.
    #[serde(rename = "init.message")]
    pub init_message: Option<String>,

    /// Optional additional HTTP headers for the connection handshake request,
    /// encoded as a JSON object of string pairs,
    /// e.g. `{"Authorization": "Bearer secret-token"}`.
    pub headers: Option<String>,

    /// Interval in seconds between keep-alive pings sent to the server.
    /// Defaults to 30. Set to 0 to disable pings.
    #[serde(rename = "ping.interval.secs", default = "default_ping_interval_secs")]
    #[serde_as(as = "DisplayFromStr")]
    pub ping_interval_secs: u64,

    /// If no data message is received within this many seconds, the connection is
    /// considered stale and is re-established (the init message is sent again).
    /// This guards against servers that silently stop delivering subscription data
    /// while keeping the underlying connection (and ping/pong) alive.
    /// Disabled by default. Control frames do not reset the timer.
    #[serde(rename = "idle.timeout.secs")]
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub idle_timeout_secs: Option<u64>,

    #[serde(flatten)]
    pub unknown_fields: HashMap<String, String>,
}

impl WebsocketProperties {
    /// Validate the properties and build the client handshake request.
    pub(crate) fn build_client_request(&self) -> ConnectorResult<ClientRequestBuilder> {
        let uri: Uri = self
            .url
            .parse()
            .with_context(|| format!("failed to parse WebSocket url: {}", self.url))?;
        if !matches!(uri.scheme_str(), Some("ws") | Some("wss")) {
            return Err(anyhow::anyhow!(
                "invalid WebSocket url: {}, must be prefixed with `ws://` or `wss://`",
                self.url
            )
            .into());
        }

        let mut request = ClientRequestBuilder::new(uri);
        if let Some(headers) = &self.headers {
            let headers: HashMap<String, String> = serde_json::from_str(headers)
                .context("failed to parse `headers`, expect a JSON object of string pairs")?;
            for (name, value) in headers {
                request = request.with_header(name, value);
            }
        }
        Ok(request)
    }
}

impl EnforceSecret for WebsocketProperties {
    const ENFORCE_SECRET_PROPERTIES: Set<&'static str> = phf_set! {
        "headers",
    };
}

impl SourceProperties for WebsocketProperties {
    type Split = WebsocketSplit;
    type SplitEnumerator = WebsocketSplitEnumerator;
    type SplitReader = WebsocketSplitReader;

    const SOURCE_NAME: &'static str = WEBSOCKET_CONNECTOR;
}

impl crate::source::UnknownFields for WebsocketProperties {
    fn unknown_fields(&self) -> HashMap<String, String> {
        self.unknown_fields.clone()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::source::TryFromBTreeMap;

    #[test]
    fn test_parse_properties() {
        let props: BTreeMap<String, String> = [
            ("url".to_owned(), "ws://localhost:8080/stream".to_owned()),
            (
                "init.message".to_owned(),
                r#"{"op":"subscribe"}"#.to_owned(),
            ),
            (
                "headers".to_owned(),
                r#"{"Authorization": "Bearer foo"}"#.to_owned(),
            ),
            ("ping.interval.secs".to_owned(), "15".to_owned()),
            ("idle.timeout.secs".to_owned(), "120".to_owned()),
        ]
        .into_iter()
        .collect();

        let props = WebsocketProperties::try_from_btreemap(props, true).unwrap();
        assert_eq!(props.url, "ws://localhost:8080/stream");
        assert_eq!(props.init_message.as_deref(), Some(r#"{"op":"subscribe"}"#));
        assert_eq!(props.ping_interval_secs, 15);
        assert_eq!(props.idle_timeout_secs, Some(120));
        props.build_client_request().unwrap();
    }

    #[test]
    fn test_parse_properties_default() {
        let props: BTreeMap<String, String> =
            [("url".to_owned(), "wss://example.com/feed".to_owned())]
                .into_iter()
                .collect();

        let props = WebsocketProperties::try_from_btreemap(props, true).unwrap();
        assert_eq!(props.ping_interval_secs, 30);
        assert!(props.idle_timeout_secs.is_none());
        assert!(props.init_message.is_none());
        assert!(props.headers.is_none());
        props.build_client_request().unwrap();
    }

    #[test]
    fn test_invalid_url_scheme() {
        let props: BTreeMap<String, String> =
            [("url".to_owned(), "http://example.com/feed".to_owned())]
                .into_iter()
                .collect();

        let props = WebsocketProperties::try_from_btreemap(props, true).unwrap();
        let err = props.build_client_request().unwrap_err();
        assert!(err.to_string().contains("ws://"), "{}", err);
    }

    #[test]
    fn test_invalid_headers() {
        let props: BTreeMap<String, String> = [
            ("url".to_owned(), "ws://example.com/feed".to_owned()),
            ("headers".to_owned(), "not-a-json-object".to_owned()),
        ]
        .into_iter()
        .collect();

        let props = WebsocketProperties::try_from_btreemap(props, true).unwrap();
        props.build_client_request().unwrap_err();
    }
}
