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

use std::time::Duration;

use anyhow::Context;
use async_trait::async_trait;

use super::WebsocketProperties;
use super::split::WebsocketSplit;
use crate::error::ConnectorResult;
use crate::source::{SourceEnumeratorContextRef, SplitEnumerator};

const CONNECT_CHECK_TIMEOUT: Duration = Duration::from_secs(10);

pub struct WebsocketSplitEnumerator {
    properties: WebsocketProperties,
    /// Whether the connectivity to the server has been verified. The check is only
    /// performed once per enumerator (i.e. essentially on source creation), since
    /// `list_splits` is called periodically and dialing a rate-limited endpoint on
    /// every tick could get the client throttled or banned.
    connectivity_checked: bool,
}

#[async_trait]
impl SplitEnumerator for WebsocketSplitEnumerator {
    type Properties = WebsocketProperties;
    type Split = WebsocketSplit;

    async fn new(
        properties: Self::Properties,
        _context: SourceEnumeratorContextRef,
    ) -> ConnectorResult<WebsocketSplitEnumerator> {
        // Validate the url and headers eagerly to fail fast on malformed properties.
        properties.build_client_request()?;
        Ok(Self {
            properties,
            connectivity_checked: false,
        })
    }

    async fn list_splits(&mut self) -> ConnectorResult<Vec<WebsocketSplit>> {
        if !self.connectivity_checked {
            let request = self.properties.build_client_request()?;
            let (mut stream, _resp) = tokio::time::timeout(
                CONNECT_CHECK_TIMEOUT,
                tokio_tungstenite::connect_async(request),
            )
            .await
            .with_context(|| {
                format!(
                    "timeout connecting to WebSocket server {} after {} seconds",
                    self.properties.url,
                    CONNECT_CHECK_TIMEOUT.as_secs()
                )
            })?
            .with_context(|| {
                format!(
                    "failed to connect to WebSocket server {}",
                    self.properties.url
                )
            })?;
            // Close the probe connection gracefully; ignore errors since the
            // connectivity has already been verified.
            let _ = stream.close(None).await;
            self.connectivity_checked = true;
        }

        Ok(vec![WebsocketSplit::new()])
    }
}

#[cfg(test)]
mod tests {
    #[cfg(not(madsim))]
    use futures::StreamExt;
    #[cfg(not(madsim))]
    use tokio::net::TcpListener;

    use super::*;
    use crate::source::SourceEnumeratorContext;
    #[cfg(not(madsim))]
    use crate::source::SplitMetaData;

    fn test_properties(url: String) -> WebsocketProperties {
        WebsocketProperties {
            url,
            init_message: None,
            headers: None,
            ping_interval_secs: 30,
            idle_timeout_secs: None,
            unknown_fields: Default::default(),
        }
    }

    // These tests dial a local TCP server via `tokio_tungstenite`, which drives the
    // connection with the real `tokio` runtime. They are therefore incompatible with the
    // madsim simulation runtime, where no real reactor is running.
    #[cfg(not(madsim))]
    #[tokio::test]
    async fn test_list_splits_checks_connectivity_once() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            // Serve exactly one connection: subsequent `list_splits` calls must not
            // dial again.
            let (stream, _) = listener.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
            while ws.next().await.is_some() {}
            drop(listener);
        });

        let mut enumerator = WebsocketSplitEnumerator::new(
            test_properties(format!("ws://{}", addr)),
            SourceEnumeratorContext::dummy().into(),
        )
        .await
        .unwrap();

        let splits = enumerator.list_splits().await.unwrap();
        assert_eq!(splits.len(), 1);
        assert_eq!(splits[0].id().as_ref(), "websocket");

        // The probe connection has been closed and the listener no longer accepts
        // connections, so this only passes if no new dial happens.
        let splits = enumerator.list_splits().await.unwrap();
        assert_eq!(splits.len(), 1);
    }

    #[cfg(not(madsim))]
    #[tokio::test]
    async fn test_list_splits_unreachable_server() {
        // Bind and drop a listener to obtain an address that refuses connections.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);

        let mut enumerator = WebsocketSplitEnumerator::new(
            test_properties(format!("ws://{}", addr)),
            SourceEnumeratorContext::dummy().into(),
        )
        .await
        .unwrap();

        enumerator.list_splits().await.unwrap_err();
    }

    #[tokio::test]
    async fn test_new_rejects_malformed_properties() {
        let res = WebsocketSplitEnumerator::new(
            test_properties("http://example.com".to_owned()),
            SourceEnumeratorContext::dummy().into(),
        )
        .await;
        assert!(res.is_err());
    }
}
