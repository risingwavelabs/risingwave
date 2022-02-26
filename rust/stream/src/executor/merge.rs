// TODO: remove this after remote channel is actually used.
#![allow(dead_code)]

use std::net::SocketAddr;
use std::time::Duration;

use async_trait::async_trait;
use futures::channel::mpsc::{Receiver, Sender};
use futures::future::select_all;
use futures::{SinkExt, StreamExt};
use risingwave_common::catalog::Schema;
use risingwave_common::error::ErrorCode::InternalError;
use risingwave_common::error::ToRwResult;
use risingwave_common::try_match_expand;
use risingwave_pb::stream_plan;
use risingwave_pb::stream_plan::stream_node::Node;
use risingwave_pb::task_service::exchange_service_client::ExchangeServiceClient;
use risingwave_pb::task_service::{GetStreamRequest, GetStreamResponse};
use risingwave_storage::StateStore;
use tonic::transport::{Channel, Endpoint};
use tonic::{Request, Streaming};
use tracing_futures::Instrument;

use super::{Barrier, Executor, Message, PkIndicesRef, Result};
use crate::executor::{ExecutorBuilder, Mutation, PkIndices};
use crate::task::{ExecutorParams, StreamManagerCore, UpDownActorIds};

/// Receive data from `gRPC` and forwards to `MergerExecutor`/`ReceiverExecutor`
pub struct RemoteInput {
    client: ExchangeServiceClient<Channel>,
    stream: Streaming<GetStreamResponse>,
    sender: Sender<Message>,

    /// Address of the remote endpoint.
    addr: SocketAddr,
    up_down_ids: UpDownActorIds,
}

impl RemoteInput {
    pub async fn create(
        addr: SocketAddr,
        up_down_ids: UpDownActorIds,
        sender: Sender<Message>,
    ) -> Result<Self> {
        let mut client = ExchangeServiceClient::new(
            Endpoint::from_shared(format!("http://{}", addr))
                .map_err(|e| InternalError(format!("{}", e)))?
                .connect_timeout(Duration::from_secs(5))
                .connect()
                .await
                .to_rw_result_with(format!("RemoteInput failed to connect to {}", addr))?,
        );
        let req = GetStreamRequest {
            up_fragment_id: up_down_ids.0,
            down_fragment_id: up_down_ids.1,
        };
        let stream = client
            .get_stream(Request::new(req))
            .await
            .to_rw_result_with(format!(
                "failed to create stream from remote_input {} from fragment {} to fragment {}",
                addr, up_down_ids.0, up_down_ids.1
            ))?
            .into_inner();
        Ok(Self {
            client,
            stream,
            sender,
            addr,
            up_down_ids,
        })
    }

    pub async fn run(&mut self) {
        loop {
            let data = self.stream.next().await;
            match data {
                // the connection from rpc server is closed, then break the loop
                None => break,
                Some(data_res) => match data_res {
                    Ok(stream_msg) => {
                        let msg_res = Message::from_protobuf(
                            stream_msg
                                .get_message()
                                .expect("no message in stream response!"),
                        );
                        match msg_res {
                            Ok(msg) => {
                                let _ = self.sender.send(msg).await;
                            }
                            Err(e) => {
                                info!("RemoteInput forward message error:{}", e);
                                break;
                            }
                        }
                    }
                    Err(e) => {
                        info!("RemoteInput tonic error status:{}", e);
                        break;
                    }
                },
            }
        }
    }
}

/// `ReceiverExecutor` is used along with a channel. After creating a mpsc channel,
/// there should be a `ReceiverExecutor` running in the background, so as to push
/// messages down to the executors.
pub struct ReceiverExecutor {
    schema: Schema,
    pk_indices: PkIndices,
    receiver: Receiver<Message>,
    /// Logical Operator Info
    op_info: String,
}

impl std::fmt::Debug for ReceiverExecutor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReceiverExecutor")
            .field("schema", &self.schema)
            .field("pk_indices", &self.pk_indices)
            .finish()
    }
}

pub struct MergeExecutorBuilder {}

impl ExecutorBuilder for MergeExecutorBuilder {
    fn build(
        params: ExecutorParams,
        node: &stream_plan::StreamNode,
        _store: impl StateStore,
        stream: &mut StreamManagerCore,
    ) -> Result<Box<dyn Executor>> {
        let node = try_match_expand!(node.get_node().unwrap(), Node::MergeNode)?;
        stream.create_merge_node(params, node)
    }
}

impl ReceiverExecutor {
    pub fn new(
        schema: Schema,
        pk_indices: PkIndices,
        receiver: Receiver<Message>,
        op_info: String,
    ) -> Self {
        Self {
            schema,
            pk_indices,
            receiver,
            op_info,
        }
    }
}

#[async_trait]
impl Executor for ReceiverExecutor {
    async fn next(&mut self) -> Result<Message> {
        let msg = self
            .receiver
            .next()
            .instrument(tracing::trace_span!("idle"))
            .await
            .unwrap(); // TODO: remove unwrap
        Ok(msg)
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn pk_indices(&self) -> PkIndicesRef {
        &self.pk_indices
    }

    fn identity(&self) -> &str {
        "ReceiverExecutor"
    }

    fn logical_operator_info(&self) -> &str {
        &self.op_info
    }
}

/// `MergeExecutor` merges data from multiple channels. Dataflow from one channel
/// will be stopped on barrier.
pub struct MergeExecutor {
    /// Schema of inputs
    schema: Schema,

    /// Primary key indices of inputs
    pk_indices: PkIndices,

    /// Number of inputs
    num_inputs: usize,

    /// Active channels
    active: Vec<Receiver<Message>>,

    /// Count of terminated channels
    terminated: usize,

    /// Channels that blocked by barriers are parked here. Would be put back
    /// until all barriers reached
    blocked: Vec<Receiver<Message>>,

    /// Current barrier.
    next_barrier: Option<Barrier>,

    /// Belonged actor id.
    actor_id: u32,

    /// Logical Operator Info
    op_info: String,
}

impl std::fmt::Debug for MergeExecutor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MergeExecutor")
            .field("schema", &self.schema)
            .field("pk_indices", &self.pk_indices)
            .field("num_inputs", &self.num_inputs)
            .finish()
    }
}

impl MergeExecutor {
    pub fn new(
        schema: Schema,
        pk_indices: PkIndices,
        actor_id: u32,
        inputs: Vec<Receiver<Message>>,
        op_info: String,
    ) -> Self {
        Self {
            schema,
            pk_indices,
            num_inputs: inputs.len(),
            active: inputs,
            blocked: vec![],
            terminated: 0,
            next_barrier: None,
            actor_id,
            op_info,
        }
    }
}

#[async_trait]
impl Executor for MergeExecutor {
    async fn next(&mut self) -> Result<Message> {
        loop {
            // Convert channel receivers to futures here to do `select_all`
            let mut futures = vec![];
            for ch in self.active.drain(..) {
                futures.push(ch.into_future());
            }
            let ((message, from), _id, remains) = select_all(futures)
                .instrument(tracing::trace_span!("idle"))
                .await;
            for fut in remains {
                self.active.push(fut.into_inner().unwrap());
            }

            match message.unwrap() {
                Message::Chunk(chunk) => {
                    self.active.push(from);
                    return Ok(Message::Chunk(chunk));
                }
                Message::Barrier(barrier) => {
                    if let Some(Mutation::Stop(actors)) = barrier.mutation.as_deref() {
                        if actors.contains(&self.actor_id) {
                            self.terminated += 1;
                        }
                    }
                    // Move this channel into the `blocked` list
                    if self.blocked.is_empty() {
                        assert_eq!(self.next_barrier, None);
                        self.next_barrier = Some(barrier.clone());
                    } else {
                        assert_eq!(self.next_barrier, Some(barrier.clone()));
                    }

                    self.blocked.push(from);
                }
            }

            if self.terminated == self.num_inputs {
                return Ok(Message::Barrier(self.next_barrier.take().unwrap()));
            }
            if self.blocked.len() == self.num_inputs {
                // Emit the barrier to downstream once all barriers collected from upstream
                assert!(self.active.is_empty());
                self.active = std::mem::take(&mut self.blocked);
                let barrier = self.next_barrier.take().unwrap();
                return Ok(Message::Barrier(barrier));
            }
            assert!(!self.active.is_empty())
        }
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn pk_indices(&self) -> PkIndicesRef {
        &self.pk_indices
    }

    fn identity(&self) -> &'static str {
        "MergeExecutor"
    }

    fn logical_operator_info(&self) -> &str {
        &self.op_info
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::thread::sleep;
    use std::time::Duration;

    use assert_matches::assert_matches;
    use futures::channel::mpsc::channel;
    use futures::SinkExt;
    use itertools::Itertools;
    use risingwave_common::array::{Op, StreamChunk};
    use risingwave_pb::data::StreamMessage;
    use risingwave_pb::task_service::exchange_service_server::{
        ExchangeService, ExchangeServiceServer,
    };
    use risingwave_pb::task_service::{GetDataRequest, GetDataResponse};
    use tokio_stream::wrappers::ReceiverStream;
    use tonic::{Response, Status};

    use super::*;

    fn build_test_chunk(epoch: u64) -> StreamChunk {
        // The number of items in `ops` is the epoch count.
        let ops = vec![Op::Insert; epoch as usize];
        StreamChunk::new(ops, vec![], None)
    }

    #[tokio::test]
    async fn test_merger() {
        const CHANNEL_NUMBER: usize = 10;
        let mut txs = Vec::with_capacity(CHANNEL_NUMBER);
        let mut rxs = Vec::with_capacity(CHANNEL_NUMBER);
        for _i in 0..CHANNEL_NUMBER {
            let (tx, rx) = futures::channel::mpsc::channel(16);
            txs.push(tx);
            rxs.push(rx);
        }
        let mut merger = MergeExecutor::new(
            Schema::default(),
            vec![],
            0,
            rxs,
            "MergerExecutor".to_string(),
        );

        let mut handles = Vec::with_capacity(CHANNEL_NUMBER);

        let epochs = (10..1000u64).step_by(10).collect_vec();

        for mut tx in txs {
            let epochs = epochs.clone();
            let handle = tokio::spawn(async move {
                for epoch in epochs {
                    tx.send(Message::Chunk(build_test_chunk(epoch)))
                        .await
                        .unwrap();
                    tx.send(Message::Barrier(Barrier::new_test_barrier(epoch)))
                        .await
                        .unwrap();
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
                tx.send(Message::Barrier(
                    Barrier::new_test_barrier(1000)
                        .with_mutation(Mutation::Stop(HashSet::default())),
                ))
                .await
                .unwrap();
            });
            handles.push(handle);
        }

        for epoch in epochs {
            // expect n chunks
            for _ in 0..CHANNEL_NUMBER {
                assert_matches!(merger.next().await.unwrap(), Message::Chunk(chunk) => {
                  assert_eq!(chunk.ops().len() as u64, epoch);
                });
            }
            // expect a barrier
            assert_matches!(merger.next().await.unwrap(), Message::Barrier(Barrier{epoch:barrier_epoch,mutation:_,..}) => {
              assert_eq!(barrier_epoch.curr, epoch);
            });
        }
        assert_matches!(
          merger.next().await.unwrap(),
          Message::Barrier(Barrier {
            mutation,
            ..
          }) if mutation.as_deref().unwrap().is_stop()
        );

        for handle in handles {
            handle.await.unwrap();
        }
    }

    struct FakeExchangeService {
        rpc_called: Arc<AtomicBool>,
    }

    #[async_trait::async_trait]
    impl ExchangeService for FakeExchangeService {
        type GetDataStream = ReceiverStream<std::result::Result<GetDataResponse, Status>>;
        type GetStreamStream = ReceiverStream<std::result::Result<GetStreamResponse, Status>>;

        async fn get_data(
            &self,
            _: Request<GetDataRequest>,
        ) -> std::result::Result<Response<Self::GetDataStream>, Status> {
            unimplemented!()
        }

        async fn get_stream(
            &self,
            _request: Request<GetStreamRequest>,
        ) -> std::result::Result<Response<Self::GetStreamStream>, Status> {
            let (tx, rx) = tokio::sync::mpsc::channel(10);
            self.rpc_called.store(true, Ordering::SeqCst);
            // send stream_chunk
            let stream_chunk = StreamChunk::default().to_protobuf().unwrap();
            tx.send(Ok(GetStreamResponse {
                message: Some(StreamMessage {
                    stream_message: Some(
                        risingwave_pb::data::stream_message::StreamMessage::StreamChunk(
                            stream_chunk,
                        ),
                    ),
                }),
            }))
            .await
            .unwrap();
            // send barrier
            let barrier = Barrier::new_test_barrier(12345);
            tx.send(Ok(GetStreamResponse {
                message: Some(StreamMessage {
                    stream_message: Some(
                        risingwave_pb::data::stream_message::StreamMessage::Barrier(
                            barrier.to_protobuf(),
                        ),
                    ),
                }),
            }))
            .await
            .unwrap();
            Ok(Response::new(ReceiverStream::new(rx)))
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_stream_exchange_client() {
        let rpc_called = Arc::new(AtomicBool::new(false));
        let server_run = Arc::new(AtomicBool::new(false));
        let addr = "127.0.0.1:12348".parse().unwrap();

        // Start a server.
        let (shutdown_send, mut shutdown_recv) = tokio::sync::mpsc::unbounded_channel();
        let exchange_svc = ExchangeServiceServer::new(FakeExchangeService {
            rpc_called: rpc_called.clone(),
        });
        let cp_server_run = server_run.clone();
        let join_handle = tokio::spawn(async move {
            cp_server_run.store(true, Ordering::SeqCst);
            tonic::transport::Server::builder()
                .add_service(exchange_svc)
                .serve_with_shutdown(addr, async move {
                    shutdown_recv.recv().await;
                })
                .await
                .unwrap();
        });

        sleep(Duration::from_secs(1));
        assert!(server_run.load(Ordering::SeqCst));
        let (tx, mut rx) = channel(16);
        let input_handle = tokio::spawn(async move {
            let mut remote_input = RemoteInput::create(addr, (0, 0), tx).await.unwrap();
            remote_input.run().await
        });
        assert_matches!(rx.next().await.unwrap(), Message::Chunk(chunk) => {
          let (ops, columns, visibility) = chunk.into_inner();
          assert_eq!(ops.len() as u64, 0);
          assert_eq!(columns.len() as u64, 0);
          assert_eq!(visibility, None);
        });
        assert_matches!(rx.next().await.unwrap(), Message::Barrier(Barrier { epoch: barrier_epoch, mutation: _, .. }) => {
          assert_eq!(barrier_epoch.curr, 12345);
        });
        assert!(rpc_called.load(Ordering::SeqCst));
        input_handle.await.unwrap();
        shutdown_send.send(()).unwrap();
        join_handle.await.unwrap();
    }
}
