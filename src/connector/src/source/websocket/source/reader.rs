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

use async_trait::async_trait;
use futures::{SinkExt, Stream, StreamExt};
use futures_async_stream::try_stream;
use thiserror_ext::AsReport;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::{Bytes, Error as WsError, Message};

use super::WebsocketSplit;
use super::message::WebsocketMessage;
use crate::error::ConnectorResult as Result;
use crate::parser::ParserConfig;
use crate::source::common::into_chunk_stream;
use crate::source::websocket::WebsocketProperties;
use crate::source::{
    BoxSourceChunkStream, Column, SourceContextRef, SourceMessage, SplitId, SplitMetaData,
    SplitReader,
};

const RECONNECT_BACKOFF_MIN: Duration = Duration::from_secs(1);
const RECONNECT_BACKOFF_MAX: Duration = Duration::from_secs(60);

pub struct WebsocketSplitReader {
    properties: WebsocketProperties,
    split_id: SplitId,
    parser_config: ParserConfig,
    source_ctx: SourceContextRef,
}

#[async_trait]
impl SplitReader for WebsocketSplitReader {
    type Properties = WebsocketProperties;
    type Split = WebsocketSplit;

    async fn new(
        properties: WebsocketProperties,
        splits: Vec<WebsocketSplit>,
        parser_config: ParserConfig,
        source_ctx: SourceContextRef,
        _columns: Option<Vec<Column>>,
    ) -> Result<Self> {
        // Validate the url and headers eagerly to fail fast on malformed properties.
        // The actual connection is established lazily in the data stream, which handles
        // reconnection with backoff.
        properties.build_client_request()?;

        let split_id = splits
            .first()
            .map(|split| split.id())
            .unwrap_or_else(|| WebsocketSplit::new().id());

        Ok(Self {
            properties,
            split_id,
            parser_config,
            source_ctx,
        })
    }

    fn into_stream(self) -> BoxSourceChunkStream {
        let parser_config = self.parser_config.clone();
        let source_context = self.source_ctx.clone();
        into_chunk_stream(self.into_data_stream(), parser_config, source_context)
    }
}

/// An event selected from the `WebSocket` stream or the ping ticker.
enum Event {
    /// An item from the `WebSocket` stream, `None` means the stream has ended.
    Message(Option<std::result::Result<Message, WsError>>),
    /// Time to send a keep-alive ping.
    PingTick,
    /// No data message arrived before the idle deadline.
    IdleTimeout,
}

/// Wait for the next event on the connection.
///
/// This is a separate function because `tokio::select!` expands to `.await`s that
/// `#[try_stream]` cannot rewrite inside the stream body.
async fn next_event<S>(
    ws_stream: &mut S,
    ping_ticker: &mut tokio::time::Interval,
    ping_enabled: bool,
    idle_deadline: Option<tokio::time::Instant>,
) -> Event
where
    S: Stream<Item = std::result::Result<Message, WsError>> + Unpin,
{
    tokio::select! {
        biased;
        message = ws_stream.next() => Event::Message(message),
        _ = ping_ticker.tick(), if ping_enabled => Event::PingTick,
        _ = async { tokio::time::sleep_until(idle_deadline.unwrap()).await },
            if idle_deadline.is_some() => Event::IdleTimeout,
    }
}

impl WebsocketSplitReader {
    /// Connect to the `WebSocket` server and stream messages, reconnecting with a capped
    /// exponential backoff whenever the connection is lost. `WebSocket` servers routinely
    /// drop long-lived connections (e.g. periodic forced disconnects or idle timeouts),
    /// so transient failures must not bubble up as stream errors.
    ///
    /// A `WebSocket` stream cannot be replayed, so messages sent by the server while the
    /// connection is down are lost. This is inherent to the protocol.
    #[try_stream(ok = Vec<SourceMessage>, error = crate::error::ConnectorError)]
    async fn into_data_stream(self) {
        let url = self.properties.url.clone();
        // A per-reader counter used as the message offset, increasing monotonically
        // across reconnections.
        let mut sequence_number: u64 = 0;
        let mut backoff = RECONNECT_BACKOFF_MIN;
        let mut first_attempt = true;

        loop {
            if !first_attempt {
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(RECONNECT_BACKOFF_MAX);
            }
            first_attempt = false;

            // Property validation errors are fatal; they have already been checked in
            // `SplitReader::new`, so an error here is unreachable in practice.
            let request = self.properties.build_client_request()?;
            let mut ws_stream = match connect_async(request).await {
                Ok((ws_stream, _resp)) => {
                    tracing::info!(url, "connected to WebSocket server");
                    ws_stream
                }
                Err(e) => {
                    tracing::warn!(
                        url,
                        error = %e.as_report(),
                        "failed to connect to WebSocket server, will retry in {:?}",
                        backoff,
                    );
                    continue;
                }
            };

            if let Some(init_message) = &self.properties.init_message
                && let Err(e) = ws_stream.send(Message::text(init_message.clone())).await
            {
                tracing::warn!(
                    url,
                    error = %e.as_report(),
                    "failed to send init message, will reconnect in {:?}",
                    backoff,
                );
                continue;
            }

            let ping_interval_secs = self.properties.ping_interval_secs;
            let ping_enabled = ping_interval_secs > 0;
            // The interval is unused when pings are disabled, but `tokio::time::interval`
            // panics on a zero duration, hence the `max(1)`.
            let mut ping_ticker =
                tokio::time::interval(Duration::from_secs(ping_interval_secs.max(1)));
            ping_ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

            // Deadline for receiving the next data message. Some servers keep the
            // connection (and ping/pong) alive but silently stop delivering
            // subscription data; reconnecting re-sends the init message and resumes
            // the subscription.
            let idle_timeout = self
                .properties
                .idle_timeout_secs
                .filter(|secs| *secs > 0)
                .map(Duration::from_secs);
            let mut idle_deadline =
                idle_timeout.map(|timeout| tokio::time::Instant::now() + timeout);

            // Read messages until the connection is lost, then reconnect.
            loop {
                let event = next_event(
                    &mut ws_stream,
                    &mut ping_ticker,
                    ping_enabled,
                    idle_deadline,
                )
                .await;

                match event {
                    Event::Message(Some(Ok(message))) => match message {
                        Message::Text(_) | Message::Binary(_) => {
                            let payload = match message {
                                Message::Text(text) => text.as_bytes().to_vec(),
                                Message::Binary(binary) => binary.to_vec(),
                                _ => unreachable!(),
                            };
                            // Skip empty frames, which some servers send as keep-alives.
                            if payload.is_empty() {
                                continue;
                            }
                            let message = WebsocketMessage {
                                split_id: self.split_id.clone(),
                                sequence_number,
                                payload,
                            };
                            sequence_number += 1;
                            // The connection proved to be healthy, reset the backoff
                            // and push out the idle deadline.
                            backoff = RECONNECT_BACKOFF_MIN;
                            idle_deadline = idle_timeout.map(|t| tokio::time::Instant::now() + t);
                            yield vec![SourceMessage::from(message)];
                        }
                        Message::Close(close_frame) => {
                            tracing::info!(
                                url,
                                ?close_frame,
                                "WebSocket connection closed by server, will reconnect in {:?}",
                                backoff,
                            );
                            break;
                        }
                        // Ping frames are answered automatically by tungstenite.
                        Message::Ping(_) | Message::Pong(_) | Message::Frame(_) => {}
                    },
                    Event::Message(Some(Err(e))) => {
                        tracing::warn!(
                            url,
                            error = %e.as_report(),
                            "WebSocket connection error, will reconnect in {:?}",
                            backoff,
                        );
                        break;
                    }
                    Event::Message(None) => {
                        tracing::info!(
                            url,
                            "WebSocket stream ended, will reconnect in {:?}",
                            backoff
                        );
                        break;
                    }
                    Event::PingTick => {
                        if let Err(e) = ws_stream.send(Message::Ping(Bytes::new())).await {
                            tracing::warn!(
                                url,
                                error = %e.as_report(),
                                "failed to send ping, will reconnect in {:?}",
                                backoff,
                            );
                            break;
                        }
                    }
                    Event::IdleTimeout => {
                        tracing::warn!(
                            url,
                            timeout_secs = self.properties.idle_timeout_secs,
                            "no data received within the idle timeout, reconnecting to \
                             refresh the subscription",
                        );
                        // Reconnect immediately: the transport is typically still
                        // healthy here, only the subscription went quiet, so backoff
                        // (reset on the next received message) would just add latency.
                        break;
                    }
                }
            }
        }
    }
}

// These tests run a local TCP server and drive the reader via `tokio_tungstenite`, which
// uses the real `tokio` runtime. They are therefore incompatible with the madsim
// simulation runtime, where no real reactor is running.
#[cfg(all(test, not(madsim)))]
mod tests {
    use futures::StreamExt;
    use tokio::net::TcpListener;
    use tokio_tungstenite::accept_async;
    use tokio_tungstenite::tungstenite::handshake::server::{
        Request as HandshakeRequest, Response as HandshakeResponse,
    };

    use super::*;
    use crate::source::SourceContext;
    use crate::source::websocket::split::WEBSOCKET_SPLIT_ID;

    async fn create_reader(
        url: String,
        init_message: Option<String>,
        headers: Option<String>,
        ping_interval_secs: u64,
    ) -> WebsocketSplitReader {
        create_reader_with_idle_timeout(url, init_message, headers, ping_interval_secs, None).await
    }

    async fn create_reader_with_idle_timeout(
        url: String,
        init_message: Option<String>,
        headers: Option<String>,
        ping_interval_secs: u64,
        idle_timeout_secs: Option<u64>,
    ) -> WebsocketSplitReader {
        let properties = WebsocketProperties {
            url,
            init_message,
            headers,
            ping_interval_secs,
            idle_timeout_secs,
            unknown_fields: Default::default(),
        };
        WebsocketSplitReader::new(
            properties,
            vec![WebsocketSplit::new()],
            ParserConfig::default(),
            SourceContext::dummy().into(),
            None,
        )
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn test_receive_text_and_binary_messages() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut ws = accept_async(stream).await.unwrap();
            ws.send(Message::text(r#"{"v1":1}"#)).await.unwrap();
            ws.send(Message::Binary(vec![1u8, 2, 3].into()))
                .await
                .unwrap();
            // Empty frames should be skipped by the reader.
            ws.send(Message::text("")).await.unwrap();
            ws.send(Message::text("last")).await.unwrap();
            // Keep the connection open until the client is dropped.
            while ws.next().await.is_some() {}
        });

        let reader = create_reader(format!("ws://{}", addr), None, None, 0).await;
        let mut stream = Box::pin(reader.into_data_stream());

        let batch = stream.next().await.unwrap().unwrap();
        assert_eq!(batch.len(), 1);
        assert_eq!(batch[0].payload.as_deref(), Some(r#"{"v1":1}"#.as_bytes()));
        assert_eq!(batch[0].offset, "0");
        assert_eq!(batch[0].split_id.as_ref(), WEBSOCKET_SPLIT_ID);

        let batch = stream.next().await.unwrap().unwrap();
        assert_eq!(batch[0].payload.as_deref(), Some([1u8, 2, 3].as_slice()));
        assert_eq!(batch[0].offset, "1");

        // The empty frame is skipped, so the next message is "last".
        let batch = stream.next().await.unwrap().unwrap();
        assert_eq!(batch[0].payload.as_deref(), Some(b"last".as_slice()));
        assert_eq!(batch[0].offset, "2");
    }

    #[tokio::test]
    async fn test_init_message_and_reconnect() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            // First connection: echo the init message back, then close, forcing the
            // client to reconnect.
            let (stream, _) = listener.accept().await.unwrap();
            let mut ws = accept_async(stream).await.unwrap();
            let init = ws.next().await.unwrap().unwrap();
            ws.send(Message::text(format!(
                "conn-1:{}",
                init.into_text().unwrap().as_str()
            )))
            .await
            .unwrap();
            ws.close(None).await.unwrap();

            // Second connection: the init message must be sent again.
            let (stream, _) = listener.accept().await.unwrap();
            let mut ws = accept_async(stream).await.unwrap();
            let init = ws.next().await.unwrap().unwrap();
            ws.send(Message::text(format!(
                "conn-2:{}",
                init.into_text().unwrap().as_str()
            )))
            .await
            .unwrap();
            while ws.next().await.is_some() {}
        });

        let reader = create_reader(
            format!("ws://{}", addr),
            Some("SUBSCRIBE".to_owned()),
            None,
            0,
        )
        .await;
        let mut stream = Box::pin(reader.into_data_stream());

        let batch = stream.next().await.unwrap().unwrap();
        assert_eq!(
            batch[0].payload.as_deref(),
            Some(b"conn-1:SUBSCRIBE".as_slice())
        );
        assert_eq!(batch[0].offset, "0");

        // The reader reconnects transparently (after the minimal backoff) and re-sends
        // the init message; the sequence number continues to increase.
        let batch = stream.next().await.unwrap().unwrap();
        assert_eq!(
            batch[0].payload.as_deref(),
            Some(b"conn-2:SUBSCRIBE".as_slice())
        );
        assert_eq!(batch[0].offset, "1");
    }

    #[tokio::test]
    async fn test_keep_alive_ping() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut ws = accept_async(stream).await.unwrap();
            // Notify the client when its keep-alive ping is received.
            while let Some(Ok(message)) = ws.next().await {
                if let Message::Ping(_) = message {
                    ws.send(Message::text("got-ping")).await.unwrap();
                }
            }
        });

        // The first ping is sent immediately after connecting.
        let reader = create_reader(format!("ws://{}", addr), None, None, 1).await;
        let mut stream = Box::pin(reader.into_data_stream());

        let batch = stream.next().await.unwrap().unwrap();
        assert_eq!(batch[0].payload.as_deref(), Some(b"got-ping".as_slice()));
    }

    #[tokio::test]
    async fn test_idle_timeout_reconnects_and_resubscribes() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            // First connection: acknowledge the subscription with one message, then go
            // silent WITHOUT closing — the transport stays open (answering pings is
            // handled by tungstenite on the client automatically). This models servers
            // that silently expire subscriptions.
            let (stream, _) = listener.accept().await.unwrap();
            let mut ws = accept_async(stream).await.unwrap();
            let init = ws.next().await.unwrap().unwrap();
            assert_eq!(init.into_text().unwrap().as_str(), "SUBSCRIBE");
            ws.send(Message::text("data-1")).await.unwrap();
            // Keep the connection open, ignore everything, send nothing.
            let silent = async move { while ws.next().await.is_some() {} };

            // Second connection (after the client's idle timeout): the init message
            // must be sent again, then data flows again.
            let resubscribed = async {
                let (stream, _) = listener.accept().await.unwrap();
                let mut ws = accept_async(stream).await.unwrap();
                let init = ws.next().await.unwrap().unwrap();
                assert_eq!(init.into_text().unwrap().as_str(), "SUBSCRIBE");
                ws.send(Message::text("data-2")).await.unwrap();
                while ws.next().await.is_some() {}
            };
            tokio::join!(silent, resubscribed);
        });

        let reader = create_reader_with_idle_timeout(
            format!("ws://{}", addr),
            Some("SUBSCRIBE".to_owned()),
            None,
            0,
            Some(1),
        )
        .await;
        let mut stream = Box::pin(reader.into_data_stream());

        let batch = stream.next().await.unwrap().unwrap();
        assert_eq!(batch[0].payload.as_deref(), Some(b"data-1".as_slice()));

        // After ~1s of silence the reader must reconnect on its own and re-subscribe.
        let batch = tokio::time::timeout(Duration::from_secs(10), stream.next())
            .await
            .expect("reader should recover from a silent subscription within the idle timeout")
            .unwrap()
            .unwrap();
        assert_eq!(batch[0].payload.as_deref(), Some(b"data-2".as_slice()));
        assert_eq!(
            batch[0].offset, "1",
            "sequence number continues across reconnects"
        );
    }

    #[tokio::test]
    async fn test_custom_handshake_headers() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (header_tx, header_rx) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_hdr_async(
                stream,
                |req: &HandshakeRequest, resp: HandshakeResponse| {
                    let auth = req
                        .headers()
                        .get("authorization")
                        .and_then(|v| v.to_str().ok())
                        .map(|v| v.to_owned());
                    header_tx.send(auth).ok();
                    Ok(resp)
                },
            )
            .await
            .unwrap();
            ws.send(Message::text("hello")).await.unwrap();
            while ws.next().await.is_some() {}
        });

        let reader = create_reader(
            format!("ws://{}", addr),
            None,
            Some(r#"{"Authorization": "Bearer foo"}"#.to_owned()),
            0,
        )
        .await;
        let mut stream = Box::pin(reader.into_data_stream());

        let batch = stream.next().await.unwrap().unwrap();
        assert_eq!(batch[0].payload.as_deref(), Some(b"hello".as_slice()));
        assert_eq!(
            header_rx.await.unwrap(),
            Some("Bearer foo".to_owned()),
            "the custom header should be sent in the handshake request"
        );
    }
}
