// Copyright 2025 RisingWave Labs
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

use std::net::SocketAddr;
use std::sync::Arc;

use either::Either;
use futures::{Stream, StreamExt, TryFutureExt, TryStreamExt, pin_mut};
use futures_async_stream::try_stream;
use risingwave_pb::id::FragmentId;
use risingwave_pb::task_service::stream_exchange_service_server::StreamExchangeService;
use risingwave_pb::task_service::{GetStreamRequest, GetStreamResponse, PbPermits, permits};
use risingwave_stream::executor::DispatcherMessageBatch;
use risingwave_stream::executor::exchange::permit::{MessageWithPermits, Receiver};
use risingwave_stream::task::LocalStreamManager;
use tonic::{Request, Response, Status, Streaming};

pub mod metrics;
pub use metrics::{GLOBAL_STREAM_EXCHANGE_SERVICE_METRICS, StreamExchangeServiceMetrics};

pub type StreamDataStream = impl Stream<Item = std::result::Result<GetStreamResponse, Status>>;

#[derive(Clone)]
pub struct StreamExchangeServiceImpl {
    stream_mgr: LocalStreamManager,
    metrics: Arc<StreamExchangeServiceMetrics>,
}

#[async_trait::async_trait]
impl StreamExchangeService for StreamExchangeServiceImpl {
    type GetStreamStream = StreamDataStream;

    #[define_opaque(StreamDataStream)]
    async fn get_stream(
        &self,
        request: Request<Streaming<GetStreamRequest>>,
    ) -> std::result::Result<Response<Self::GetStreamStream>, Status> {
        use risingwave_pb::task_service::get_stream_request::*;

        let peer_addr = request
            .remote_addr()
            .ok_or_else(|| Status::unavailable("get_stream connection unestablished"))?;

        let mut request_stream: Streaming<GetStreamRequest> = request.into_inner();

        // Extract the first `Get` request from the stream.
        let Get {
            up_actor_id,
            down_actor_id,
            up_fragment_id,
            down_fragment_id,
            up_partial_graph_id,
            term_id,
        } = {
            let req = request_stream
                .next()
                .await
                .ok_or_else(|| Status::invalid_argument("get_stream request is empty"))??;
            match req.value.unwrap() {
                Value::Get(get) => get,
                Value::AddPermits(_) => unreachable!("the first message must be `Get`"),
            }
        };

        // Map the remaining stream to add-permits.
        let add_permits_stream = request_stream.map_ok(|req| match req.value.unwrap() {
            Value::Get(_) => unreachable!("the following messages must be `AddPermits`"),
            Value::AddPermits(add_permits) => add_permits.value.unwrap(),
        });

        let stream_mgr = self.stream_mgr.clone();
        let metrics = self.metrics.clone();
        // Return the RPC response before waiting for the upstream dispatcher. Otherwise, a
        // downstream adding this input can stop draining an existing input and backpressure the
        // upstream actor whose progress is needed to create the new channel.
        let responses = async move {
            let (receiver, record_ack_batch_size) = stream_mgr
                .take_receiver(up_partial_graph_id, term_id, (up_actor_id, down_actor_id))
                .await?;
            Ok::<_, Status>(Self::get_stream_impl(
                metrics,
                peer_addr,
                receiver,
                record_ack_batch_size,
                add_permits_stream,
                (up_fragment_id, down_fragment_id),
            ))
        }
        .try_flatten_stream();

        Ok(Response::new(responses))
    }
}

impl StreamExchangeServiceImpl {
    pub fn new(stream_mgr: LocalStreamManager, metrics: Arc<StreamExchangeServiceMetrics>) -> Self {
        Self {
            stream_mgr,
            metrics,
        }
    }

    #[try_stream(ok = GetStreamResponse, error = Status)]
    async fn get_stream_impl(
        metrics: Arc<StreamExchangeServiceMetrics>,
        peer_addr: SocketAddr,
        mut receiver: Receiver,
        record_ack_batch_size: usize,
        add_permits_stream: impl Stream<Item = std::result::Result<permits::Value, tonic::Status>>,
        up_down_fragment_ids: (FragmentId, FragmentId),
    ) {
        tracing::debug!(target: "events::compute::exchange", peer_addr = %peer_addr, "serve stream exchange RPC");
        let up_fragment_id = up_down_fragment_ids.0.to_string();
        let down_fragment_id = up_down_fragment_ids.1.to_string();

        let channel_permits = receiver.permits();
        let mut batched_record_permits = 0usize;

        // Select from the permits back from the downstream and the upstream receiver.
        let select_stream = futures::stream::select(
            add_permits_stream.map_ok(Either::Left),
            #[try_stream]
            async move {
                while let Some(m) = receiver.recv_raw().await {
                    yield Either::Right(m);
                }
            },
        );
        pin_mut!(select_stream);

        let exchange_frag_send_size_metrics = metrics
            .stream_fragment_exchange_bytes
            .with_label_values(&[&up_fragment_id, &down_fragment_id]);

        while let Some(r) = select_stream.try_next().await? {
            match r {
                Either::Left(permits_to_add) => {
                    channel_permits.add_permits(permits_to_add);
                }
                Either::Right(MessageWithPermits { message, permits }) => {
                    let message = match message {
                        DispatcherMessageBatch::Chunk(chunk) => {
                            DispatcherMessageBatch::Chunk(chunk.compact_vis())
                        }
                        msg @ (DispatcherMessageBatch::Watermark(_)
                        | DispatcherMessageBatch::BarrierBatch(_)) => msg,
                    };
                    let proto = message.to_protobuf();
                    // Batch credits upstream so that the downstream only needs to echo them.
                    let permits = match permits {
                        Some(permits::Value::Record(p)) => {
                            batched_record_permits += p as usize;
                            if batched_record_permits >= record_ack_batch_size {
                                let credits = std::mem::take(&mut batched_record_permits)
                                    .try_into()
                                    .map_err(|_| {
                                        Status::internal(
                                            "batched record permits exceed the protocol limit",
                                        )
                                    })?;
                                Some(permits::Value::Record(credits))
                            } else {
                                None
                            }
                        }
                        permits => permits,
                    };
                    let response = GetStreamResponse {
                        message: Some(proto),
                        permits: Some(PbPermits { value: permits }),
                    };
                    let bytes = DispatcherMessageBatch::get_encoded_len(&response);

                    yield response;

                    exchange_frag_send_size_metrics.inc_by(bytes as u64);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use futures::FutureExt;
    use risingwave_common::array::{Op, StreamChunk};
    use risingwave_common::config::StreamingConfig;
    use risingwave_common::types::DataType;
    use risingwave_stream::executor::exchange::permit::{
        ChannelMetrics, channel_from_config_with_metrics, channel_with_metrics,
    };
    use risingwave_stream::executor::{DispatcherBarrier, Watermark};
    use tokio::sync::mpsc;
    use tokio_stream::wrappers::UnboundedReceiverStream;

    use super::*;

    fn chunk(rows: usize) -> DispatcherMessageBatch {
        DispatcherMessageBatch::Chunk(StreamChunk::new(vec![Op::Insert; rows], vec![]))
    }

    #[cfg(not(madsim))]
    #[tokio::test]
    async fn test_remote_handshake_does_not_wait_for_channel() {
        use std::sync::atomic::AtomicU64;
        use std::time::Duration;

        use risingwave_pb::stream_service::streaming_control_stream_request::{
            InitRequest, Request as ControlRequest, ResetPartialGraphsRequest,
        };
        use risingwave_pb::stream_service::{
            StreamingControlStreamRequest, streaming_control_stream_response,
        };
        use risingwave_pb::task_service::get_stream_request::{Get, Value};
        use risingwave_pb::task_service::stream_exchange_service_client::StreamExchangeServiceClient;
        use risingwave_pb::task_service::stream_exchange_service_server::StreamExchangeServiceServer;
        use risingwave_stream::executor::monitor::StreamingMetrics;
        use risingwave_stream::task::StreamEnvironment;
        use tokio::net::TcpListener;
        use tokio::sync::oneshot;
        use tokio::time::timeout;
        use tokio_stream::wrappers::TcpListenerStream;

        let stream_mgr = LocalStreamManager::new(
            StreamEnvironment::for_test(),
            Arc::new(StreamingMetrics::unused()),
            None,
            Arc::new(AtomicU64::new(0)),
        );
        let (control_tx, control_rx) = mpsc::unbounded_channel();
        let (response_tx, mut response_rx) = mpsc::unbounded_channel();
        stream_mgr.handle_new_control_stream(
            response_tx,
            UnboundedReceiverStream::new(control_rx).boxed(),
            InitRequest {},
        );
        assert!(matches!(
            response_rx.recv().await.unwrap().unwrap().response,
            Some(streaming_control_stream_response::Response::Init(_))
        ));

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let service = StreamExchangeServiceImpl::new(
            stream_mgr.clone(),
            Arc::new(GLOBAL_STREAM_EXCHANGE_SERVICE_METRICS.clone()),
        );
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let server = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(StreamExchangeServiceServer::new(service))
                .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                    let _ = shutdown_rx.await;
                })
                .await
                .unwrap();
        });

        let mut client = StreamExchangeServiceClient::connect(format!("http://{addr}"))
            .await
            .unwrap();
        let (request_tx, request_rx) = mpsc::unbounded_channel();
        let partial_graph_id = 123.into();
        request_tx
            .send(GetStreamRequest {
                value: Some(Value::Get(Get {
                    up_actor_id: 1.into(),
                    down_actor_id: 2.into(),
                    up_fragment_id: 10.into(),
                    down_fragment_id: 20.into(),
                    up_partial_graph_id: partial_graph_id,
                    term_id: "test".into(),
                })),
            })
            .unwrap();

        // No upstream actor exists to create this channel. Connection setup must still finish,
        // allowing the downstream to resume draining its existing inputs.
        let mut responses = timeout(
            Duration::from_secs(5),
            client.get_stream(UnboundedReceiverStream::new(request_rx)),
        )
        .await
        .expect("RPC establishment must not wait for the upstream channel")
        .unwrap()
        .into_inner();
        assert!(responses.message().now_or_never().is_none());

        // Wait until the subscription is registered before resetting its partial graph.
        timeout(Duration::from_secs(5), async {
            while !stream_mgr
                .inspect_barrier_state()
                .await
                .unwrap()
                .contains("partial graph 123 status: ReceivedExchangeRequest")
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        control_tx
            .send(Ok(StreamingControlStreamRequest {
                request: Some(ControlRequest::ResetPartialGraphs(
                    ResetPartialGraphsRequest {
                        partial_graph_ids: vec![partial_graph_id],
                    },
                )),
            }))
            .unwrap();

        // A subscription failure after the headers must propagate through the response body.
        let error = timeout(Duration::from_secs(5), responses.message())
            .await
            .unwrap()
            .unwrap_err();
        assert!(error.message().contains("partial graph reset"));

        drop(responses);
        drop(client);
        drop(request_tx);
        drop(response_rx);
        drop(control_tx);
        stream_mgr.shutdown().await.unwrap();
        shutdown_tx.send(()).unwrap();
        timeout(Duration::from_secs(5), server)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn test_remote_permit_batching() {
        let (tx, rx) = channel_with_metrics(100, 30, 1, ChannelMetrics::for_test());
        let (_ack_tx, ack_rx) = mpsc::unbounded_channel();
        let responses = StreamExchangeServiceImpl::get_stream_impl(
            Arc::new(GLOBAL_STREAM_EXCHANGE_SERVICE_METRICS.clone()),
            "127.0.0.1:1234".parse().unwrap(),
            rx,
            30,
            UnboundedReceiverStream::new(ack_rx).map(Ok),
            (1.into(), 2.into()),
        );
        pin_mut!(responses);

        tx.send(chunk(20)).await.unwrap();
        let response = responses.next().await.unwrap().unwrap();
        assert_eq!(response.permits.unwrap().value, None);

        // Barriers are ACKed immediately, without flushing pending record credits.
        tx.send(DispatcherMessageBatch::BarrierBatch(vec![
            DispatcherBarrier::with_prev_epoch_for_test(2, 1),
        ]))
        .await
        .unwrap();
        let response = responses.next().await.unwrap().unwrap();
        assert_eq!(
            response.permits.unwrap().value,
            Some(permits::Value::Barrier(1))
        );

        // Watermarks do not change the pending credits either.
        tx.send(DispatcherMessageBatch::Watermark(Watermark::new(
            0,
            DataType::Int64,
            1i64.into(),
        )))
        .await
        .unwrap();
        let response = responses.next().await.unwrap().unwrap();
        assert_eq!(response.permits.unwrap().value, None);

        for _ in 0..2 {
            tx.send(chunk(20)).await.unwrap();
            let response = responses.next().await.unwrap().unwrap();
            assert_eq!(
                response.permits.unwrap().value,
                Some(permits::Value::Record(40)),
            );

            tx.send(chunk(20)).await.unwrap();
            let response = responses.next().await.unwrap().unwrap();
            assert_eq!(response.permits.unwrap().value, None);
        }
    }

    #[tokio::test]
    async fn test_remote_permits_only_released_after_ack() {
        let mut config = StreamingConfig::default();
        config.developer.exchange_initial_permits = 100;
        config.developer.exchange_batched_permits = 30;
        config.developer.exchange_concurrent_barriers = 1;
        let (tx, rx) = channel_from_config_with_metrics(&config, ChannelMetrics::for_test());
        let (ack_tx, ack_rx) = mpsc::unbounded_channel();
        let responses = StreamExchangeServiceImpl::get_stream_impl(
            Arc::new(GLOBAL_STREAM_EXCHANGE_SERVICE_METRICS.clone()),
            "127.0.0.1:1234".parse().unwrap(),
            rx,
            config.developer.exchange_batched_permits,
            UnboundedReceiverStream::new(ack_rx).map(Ok),
            (1.into(), 2.into()),
        );
        pin_mut!(responses);

        // The server can wait for data without releasing any record permits.
        assert!(responses.next().now_or_never().is_none());

        for _ in 0..5 {
            tx.send(chunk(20)).await.unwrap();
        }
        let sixth_send = tx.send(chunk(20));
        pin_mut!(sixth_send);
        assert!(sixth_send.as_mut().now_or_never().is_none());

        let mut acks = vec![];
        for expected in [None, Some(40), None, Some(40), None] {
            let response = responses.next().await.unwrap().unwrap();
            let credits = response.permits.unwrap().value;
            assert_eq!(credits, expected.map(permits::Value::Record));
            if let Some(credits) = credits {
                acks.push(credits);
            }
        }
        // Merely sending the responses must not release any permits.
        assert!(sixth_send.as_mut().now_or_never().is_none());

        ack_tx.send(acks.remove(0)).unwrap();
        // Drive the server to apply the ACK, with no data left to send yet.
        assert!(responses.next().now_or_never().is_none());
        assert!(sixth_send.as_mut().now_or_never().unwrap().is_ok());
        let response = responses.next().await.unwrap().unwrap();
        assert_eq!(
            response.permits.as_ref().unwrap().value,
            Some(permits::Value::Record(40))
        );
        acks.push(response.permits.unwrap().value.unwrap());

        for ack in acks {
            ack_tx.send(ack).unwrap();
        }
        assert!(responses.next().now_or_never().is_none());

        // All 120 consumed permits were returned exactly once: the budget is 100 again.
        for _ in 0..5 {
            assert!(tx.send(chunk(20)).now_or_never().unwrap().is_ok());
        }
        assert!(tx.send(chunk(20)).now_or_never().is_none());
    }
}
