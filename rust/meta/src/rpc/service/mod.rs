pub mod catalog_service;
pub mod cluster_service;
pub mod epoch_service;
pub mod heartbeat_service;
pub mod hummock_service;
pub mod notification_service;
pub mod stream_service;

use std::pin::Pin;
use std::task::{Context, Poll};

use futures::Stream;
use risingwave_common::error::{tonic_err, RwError};
use tokio::sync::mpsc::Receiver;

/// `RwReceiverStream` is a wrapper around `tokio::sync::mpsc::Receiver` that implements Stream.
/// `RwReceiverStream` is similar to `tokio_stream::wrappers::ReceiverStream`, but it maps Result<S,
/// `RwError`> to Result<S, `tonic::Status`>.
pub struct RwReceiverStream<S> {
    inner: Receiver<Result<S, RwError>>,
}

impl<S> RwReceiverStream<S> {
    pub fn new(inner: Receiver<Result<S, RwError>>) -> Self {
        Self { inner }
    }
}

impl<S> Stream for RwReceiverStream<S> {
    type Item = Result<S, tonic::Status>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner
            .poll_recv(cx)
            .map(|opt| opt.map(|res| res.map_err(tonic_err)))
    }
}
