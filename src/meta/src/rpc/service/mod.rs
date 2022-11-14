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

pub mod cluster_service;
pub mod ddl_service;
pub mod health_service;
pub mod heartbeat_service;
pub mod hummock_service;
pub mod notification_service;
pub mod scale_service;
pub mod stream_service;
pub mod user_service;

use std::pin::Pin;
use std::task::{Context, Poll};

use futures::Stream;
use tokio::sync::mpsc::Receiver;

use crate::MetaError;

/// `RwReceiverStream` is a wrapper around `tokio::sync::mpsc::Receiver` that implements
/// Stream. `RwReceiverStream` is similar to `tokio_stream::wrappers::ReceiverStream`, but it
/// maps Result<S, `MetaError`> to Result<S, `tonic::Status`>.
pub struct RwReceiverStream<S> {
    inner: Receiver<Result<S, MetaError>>,
}

impl<S> RwReceiverStream<S> {
    pub fn new(inner: Receiver<Result<S, MetaError>>) -> Self {
        Self { inner }
    }
}

impl<S> Stream for RwReceiverStream<S> {
    type Item = Result<S, tonic::Status>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner
            .poll_recv(cx)
            .map(|opt| opt.map(|res| res.map_err(Into::into)))
    }
}
