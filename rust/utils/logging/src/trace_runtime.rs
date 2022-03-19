// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
//! A custom Tokio runtime implementation that has unlimited buffer in channel.
//!
//! Should only be used in debug mode.

use std::net::ToSocketAddrs;
use std::time::Duration;

use futures::future::BoxFuture;
use opentelemetry::runtime::Runtime;
use opentelemetry::sdk::trace::{BatchMessage, TraceRuntime};

#[derive(Clone, Debug)]
pub struct RwTokio;

/// Here we manually implement [`TraceRuntime`] for [`RwTokio`].
///
/// As the trait requires `TrySend` on `Sender`, we cannot use `unbounded`.
impl TraceRuntime for RwTokio {
    type Receiver = tokio_stream::wrappers::ReceiverStream<BatchMessage>;
    type Sender = tokio::sync::mpsc::Sender<BatchMessage>;

    fn batch_message_channel(&self, _capacity: usize) -> (Self::Sender, Self::Receiver) {
        // Create an extra large channel and ignore capacity from upstream
        let (sender, receiver) = tokio::sync::mpsc::channel(1 << 20);
        (
            sender,
            tokio_stream::wrappers::ReceiverStream::new(receiver),
        )
    }
}

impl Runtime for RwTokio {
    type Interval = tokio_stream::wrappers::IntervalStream;
    type Delay = tokio::time::Sleep;

    fn interval(&self, duration: Duration) -> Self::Interval {
        opentelemetry::util::tokio_interval_stream(duration)
    }

    fn spawn(&self, future: BoxFuture<'static, ()>) {
        let _ = tokio::spawn(future);
    }

    fn delay(&self, duration: Duration) -> Self::Delay {
        tokio::time::sleep(duration)
    }
}

#[async_trait::async_trait]
impl opentelemetry_jaeger::JaegerTraceRuntime for RwTokio {
    type Socket = tokio::net::UdpSocket;

    fn create_socket<T: ToSocketAddrs>(&self, host_port: T) -> thrift::Result<Self::Socket> {
        let conn = std::net::UdpSocket::bind("0.0.0.0:0")?;
        conn.connect(host_port)?;
        Ok(tokio::net::UdpSocket::from_std(conn)?)
    }

    async fn write_to_socket(&self, socket: &Self::Socket, payload: Vec<u8>) -> thrift::Result<()> {
        socket.send(&payload).await?;

        Ok(())
    }
}
