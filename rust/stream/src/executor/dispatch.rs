use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use futures::channel::mpsc::{channel, Sender};
use futures::SinkExt;
use risingwave_common::array::{Op, RwError};
use risingwave_common::error::ErrorCode;
use risingwave_common::util::addr::{get_host_port, is_local_address};
use risingwave_common::util::hash_util::CRC32FastBuilder;

use super::{Barrier, Executor, Message, Mutation, Result, StreamChunk, StreamConsumer};
use crate::task::{SharedContext, LOCAL_OUTPUT_CHANNEL_SIZE};

/// `Output` provides an interface for `Dispatcher` to send data into downstream fragments.
#[async_trait]
pub trait Output: Debug + Send + Sync + 'static {
    async fn send(&mut self, message: Message) -> Result<()>;
}

/// `ChannelOutput` sends data to a local `mpsc::Channel`
pub struct ChannelOutput {
    ch: Sender<Message>,
}

impl Debug for ChannelOutput {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChannelOutput").finish()
    }
}

impl ChannelOutput {
    pub fn new(ch: Sender<Message>) -> Self {
        Self { ch }
    }
}

#[async_trait]
impl Output for ChannelOutput {
    async fn send(&mut self, message: Message) -> Result<()> {
        // local channel should never fail
        self.ch.send(message).await.unwrap();
        Ok(())
    }
}

/// `RemoteOutput` forwards data to`ExchangeServiceImpl`
pub struct RemoteOutput {
    ch: Sender<Message>,
}

impl Debug for RemoteOutput {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RemoteOutput").finish()
    }
}

impl RemoteOutput {
    pub fn new(ch: Sender<Message>) -> Self {
        Self { ch }
    }
}

#[async_trait]
impl Output for RemoteOutput {
    async fn send(&mut self, message: Message) -> Result<()> {
        // local channel should never fail
        self.ch.send(message).await.unwrap();
        Ok(())
    }
}

/// `DispatchExecutor` consumes messages and send them into downstream actors. Usually,
/// data chunks will be dispatched with some specified policy, while control message
/// such as barriers will be distributed to all receivers.
pub struct DispatchExecutor<Inner: DataDispatcher> {
    input: Box<dyn Executor>,
    inner: Inner,
    fragment_id: u32,
    context: Arc<SharedContext>,
}

impl<Inner: DataDispatcher> std::fmt::Debug for DispatchExecutor<Inner> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DispatchExecutor")
            .field("input", &self.input)
            .field("inner", &self.inner)
            .field("fragment_id", &self.fragment_id)
            .finish()
    }
}

impl<Inner: DataDispatcher + Send> DispatchExecutor<Inner> {
    pub fn new(
        input: Box<dyn Executor>,
        inner: Inner,
        fragment_id: u32,
        context: Arc<SharedContext>,
    ) -> Self {
        Self {
            input,
            inner,
            fragment_id,
            context,
        }
    }

    async fn dispatch(&mut self, msg: Message) -> Result<()> {
        match msg {
            Message::Chunk(chunk) => {
                self.inner.dispatch_data(chunk).await?;
            }
            Message::Barrier(barrier) => {
                self.update_outputs(&barrier).await?;
                self.dispatch_barrier(barrier).await?;
            }
        };
        Ok(())
    }
    async fn update_outputs(&mut self, barrier: &Barrier) -> Result<()> {
        match &barrier.mutation {
            Mutation::UpdateOutputs(updates) => {
                if let Some((_, v)) = updates.get_key_value(&self.fragment_id) {
                    let mut new_outputs = vec![];
                    let mut channel_pool_guard = self.context.channel_pool.lock().unwrap();
                    let mut exchange_pool_guard =
                        self.context.receivers_for_exchange_service.lock().unwrap();

                    let fragment_id = self.fragment_id;

                    // delete the old local connections in both local and remote pools;
                    channel_pool_guard.retain(|(x, _), _| *x != fragment_id);
                    exchange_pool_guard.retain(|(x, _), _| *x != fragment_id);

                    for act in v.iter() {
                        let down_id = act.get_fragment_id();
                        let up_down_ids = (fragment_id, down_id);
                        let host_addr = act.get_host();
                        let downstream_addr =
                            format!("{}:{}", host_addr.get_host(), host_addr.get_port());

                        if is_local_address(&get_host_port(&downstream_addr)?, &self.context.addr) {
                            // insert new connection
                            let (tx, rx) = channel(LOCAL_OUTPUT_CHANNEL_SIZE);
                            channel_pool_guard.insert(up_down_ids, (Some(tx), Some(rx)));

                            let tx = channel_pool_guard
                                .get_mut(&(fragment_id, down_id))
                                .ok_or_else(|| {
                                    RwError::from(ErrorCode::InternalError(format!(
                                        "channel between {} and {} does not exist",
                                        fragment_id, down_id
                                    )))
                                })?
                                .0
                                .take()
                                .ok_or_else(|| {
                                    RwError::from(ErrorCode::InternalError(format!(
                                        "sender from {} to {} does no exist",
                                        fragment_id, down_id
                                    )))
                                })?;
                            new_outputs.push(Box::new(ChannelOutput::new(tx)) as Box<dyn Output>)
                        } else {
                            let (tx, rx) = channel(LOCAL_OUTPUT_CHANNEL_SIZE);
                            exchange_pool_guard.insert(up_down_ids, rx);

                            new_outputs.push(Box::new(RemoteOutput::new(tx)) as Box<dyn Output>)
                        }
                    }
                    self.inner.update_outputs(new_outputs)
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }

    async fn dispatch_barrier(&mut self, barrier: Barrier) -> Result<()> {
        let outputs = self.inner.get_outputs();
        for output in outputs {
            output.send(Message::Barrier(barrier.clone())).await?;
        }
        Ok(())
    }
}

#[async_trait]
impl<Inner: DataDispatcher + Send + Sync + 'static> StreamConsumer for DispatchExecutor<Inner> {
    async fn next(&mut self) -> Result<bool> {
        let msg = self.input.next().await?;
        let terminated = msg.is_terminate();
        self.dispatch(msg).await?;
        Ok(!terminated)
    }
}

#[async_trait]
pub trait DataDispatcher: Debug + 'static {
    async fn dispatch_data(&mut self, chunk: StreamChunk) -> Result<()>;
    fn get_outputs(&mut self) -> &mut [Box<dyn Output>];
    fn update_outputs(&mut self, outputs: Vec<Box<dyn Output>>);
}

pub struct RoundRobinDataDispatcher {
    outputs: Vec<Box<dyn Output>>,
    cur: usize,
}

impl Debug for RoundRobinDataDispatcher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RoundRobinDataDispatcher")
            .field("outputs", &self.outputs)
            .finish()
    }
}

impl RoundRobinDataDispatcher {
    pub fn new(outputs: Vec<Box<dyn Output>>) -> Self {
        Self { outputs, cur: 0 }
    }
}

#[async_trait]
impl DataDispatcher for RoundRobinDataDispatcher {
    fn update_outputs(&mut self, outputs: Vec<Box<dyn Output>>) {
        self.outputs = outputs
    }
    fn get_outputs(&mut self) -> &mut [Box<dyn Output>] {
        &mut self.outputs
    }

    async fn dispatch_data(&mut self, chunk: StreamChunk) -> Result<()> {
        self.outputs[self.cur].send(Message::Chunk(chunk)).await?;
        self.cur += 1;
        self.cur %= self.outputs.len();
        Ok(())
    }
}

pub struct HashDataDispatcher {
    outputs: Vec<Box<dyn Output>>,
    keys: Vec<usize>,
}

impl Debug for HashDataDispatcher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HashDataDispatcher")
            .field("outputs", &self.outputs)
            .field("keys", &self.keys)
            .finish()
    }
}

impl HashDataDispatcher {
    pub fn new(outputs: Vec<Box<dyn Output>>, keys: Vec<usize>) -> Self {
        Self { outputs, keys }
    }
}

#[async_trait]
impl DataDispatcher for HashDataDispatcher {
    fn update_outputs(&mut self, outputs: Vec<Box<dyn Output>>) {
        self.outputs = outputs
    }
    fn get_outputs(&mut self) -> &mut [Box<dyn Output>] {
        &mut self.outputs
    }

    async fn dispatch_data(&mut self, chunk: StreamChunk) -> Result<()> {
        // FIXME: unnecessary compact.
        // See https://github.com/singularity-data/risingwave/issues/704
        let chunk = chunk.compact()?;

        // A chunk can be shuffled into multiple output chunks that to be sent to downstreams.
        // In these output chunks, the only difference are visibility map, which is calculated
        // by the hash value of each line in the input chunk.
        let num_outputs = self.outputs.len();

        // get hash value of every line by its key
        let hash_builder = CRC32FastBuilder {};
        let hash_values = chunk
            .get_hash_values(&self.keys, hash_builder)
            .unwrap()
            .iter()
            .map(|hash| *hash as usize % num_outputs)
            .collect::<Vec<_>>();

        let (ops, columns, visibility) = chunk.into_inner();
        assert!(visibility.is_none(), "must be compacted");

        // get visibility map for every output chunk
        let mut vis_maps = vec![vec![]; num_outputs];
        hash_values.iter().for_each(|hash| {
            for (output_idx, vis_map) in vis_maps.iter_mut().enumerate() {
                if *hash == output_idx {
                    vis_map.push(true);
                } else {
                    vis_map.push(false);
                }
            }
        });

        // The 'update' message, noted by an UpdateDelete and a successive UpdateInsert,
        // need to be rewritten to common Delete and Insert if they were dispatched to different
        // fragments.
        let mut last_hash_value_when_update_delete: usize = 0;
        let mut new_ops: Vec<Op> = Vec::new();
        for (hash_value, &op) in hash_values.into_iter().zip(ops.iter()) {
            if op == Op::UpdateDelete {
                last_hash_value_when_update_delete = hash_value;
            } else if op == Op::UpdateInsert {
                if hash_value != last_hash_value_when_update_delete {
                    new_ops.push(Op::Delete);
                    new_ops.push(Op::Insert);
                } else {
                    new_ops.push(Op::UpdateDelete);
                    new_ops.push(Op::UpdateInsert);
                }
            } else {
                new_ops.push(op);
            }
        }
        let ops = new_ops;

        // individually output StreamChunk integrated with vis_map
        for (vis_map, output) in vis_maps.into_iter().zip(self.outputs.iter_mut()) {
            let vis_map = vis_map.try_into().unwrap();
            let new_stream_chunk = StreamChunk {
                // columns is not changed in this function
                ops: ops.clone(),
                columns: columns.clone(),
                visibility: Some(vis_map),
            };
            output.send(Message::Chunk(new_stream_chunk)).await?;
        }
        Ok(())
    }
}

/// `BroadcastDispatcher` dispatches message to all outputs.
pub struct BroadcastDispatcher {
    outputs: Vec<Box<dyn Output>>,
}

impl Debug for BroadcastDispatcher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BroadcastDispatcher")
            .field("outputs", &self.outputs)
            .finish()
    }
}

impl BroadcastDispatcher {
    pub fn new(outputs: Vec<Box<dyn Output>>) -> Self {
        Self { outputs }
    }
}

#[async_trait]
impl DataDispatcher for BroadcastDispatcher {
    fn update_outputs(&mut self, outputs: Vec<Box<dyn Output>>) {
        self.outputs = outputs
    }
    fn get_outputs(&mut self) -> &mut [Box<dyn Output>] {
        &mut self.outputs
    }

    async fn dispatch_data(&mut self, chunk: StreamChunk) -> Result<()> {
        for output in &mut self.outputs {
            output.send(Message::Chunk(chunk.clone())).await?;
        }
        Ok(())
    }
}

/// `SimpleDispatcher` dispatches message to a single output.
pub struct SimpleDispatcher {
    output: Box<dyn Output>,
}

impl Debug for SimpleDispatcher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SimpleDispatcher")
            .field("output", &self.output)
            .finish()
    }
}

impl SimpleDispatcher {
    pub fn new(output: Box<dyn Output>) -> Self {
        Self { output }
    }
}

#[async_trait]
impl DataDispatcher for SimpleDispatcher {
    fn update_outputs(&mut self, outputs: Vec<Box<dyn Output>>) {
        self.output = outputs.into_iter().next().unwrap();
    }

    fn get_outputs(&mut self) -> &mut [Box<dyn Output>] {
        std::slice::from_mut(&mut self.output)
    }

    async fn dispatch_data(&mut self, chunk: StreamChunk) -> Result<()> {
        self.output.send(Message::Chunk(chunk)).await?;
        Ok(())
    }
}

/// `SenderConsumer` consumes data from input executor and send it into a channel.
pub struct SenderConsumer {
    input: Box<dyn Executor>,
    channel: Box<dyn Output>,
}

impl Debug for SenderConsumer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SenderConsumer")
            .field("input", &self.input)
            .field("channel", &self.channel)
            .finish()
    }
}

impl SenderConsumer {
    pub fn new(input: Box<dyn Executor>, channel: Box<dyn Output>) -> Self {
        Self { input, channel }
    }
}

#[async_trait]
impl StreamConsumer for SenderConsumer {
    async fn next(&mut self) -> Result<bool> {
        let message = self.input.next().await?;
        let terminated = message.is_terminate();
        self.channel.send(message).await?;
        Ok(!terminated)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::hash::{BuildHasher, Hasher};
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use std::sync::{Arc, Mutex};

    use itertools::Itertools;
    use risingwave_common::array::column::Column;
    use risingwave_common::array::{Array, ArrayBuilder, I32ArrayBuilder, Op};
    use risingwave_common::catalog::Schema;
    use risingwave_pb::common::HostAddress;
    use risingwave_pb::stream_service::ActorInfo;

    use super::*;
    use crate::executor::ReceiverExecutor;

    pub struct MockOutput {
        data: Arc<Mutex<Vec<Message>>>,
    }

    impl std::fmt::Debug for MockOutput {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("MockOutput").finish()
        }
    }

    impl MockOutput {
        pub fn new(data: Arc<Mutex<Vec<Message>>>) -> Self {
            Self { data }
        }
    }

    #[async_trait]
    impl Output for MockOutput {
        async fn send(&mut self, message: Message) -> Result<()> {
            self.data.lock().unwrap().push(message);
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_configuration_change() {
        let schema = Schema { fields: vec![] };
        let (mut tx, rx) = channel(16);
        let input = Box::new(ReceiverExecutor::new(schema.clone(), vec![], rx));
        let data_sink = Arc::new(Mutex::new(vec![]));
        let output = Box::new(MockOutput::new(data_sink));
        let fragment_id = 233;
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 2333);
        let ctx = Arc::new(SharedContext::new(addr));

        let mut executor = Box::new(DispatchExecutor::new(
            input,
            SimpleDispatcher::new(output),
            fragment_id,
            ctx.clone(),
        ));
        let mut updates1: HashMap<u32, Vec<ActorInfo>> = HashMap::new();

        updates1.insert(
            fragment_id,
            vec![
                ActorInfo {
                    fragment_id: 234,
                    host: Some(HostAddress {
                        host: String::from("127.0.0.1"),
                        port: 2333,
                    }),
                },
                ActorInfo {
                    fragment_id: 235,
                    host: Some(HostAddress {
                        host: String::from("127.0.0.1"),
                        port: 2333,
                    }),
                },
                ActorInfo {
                    fragment_id: 238,
                    host: Some(HostAddress {
                        host: String::from("172.1.1.2"),
                        port: 2334,
                    }),
                },
            ],
        );
        let b1 = Barrier {
            epoch: 0,
            mutation: Mutation::UpdateOutputs(updates1),
        };
        tx.send(Message::Barrier(b1)).await.unwrap();
        executor.next().await.unwrap();
        let tctx = ctx.clone();
        {
            let cp_guard = tctx.channel_pool.lock().unwrap();
            let ex_guard = tctx.receivers_for_exchange_service.lock().unwrap();
            assert_eq!(cp_guard.len(), 2);
            assert_eq!(ex_guard.len(), 1);
        }

        let mut updates2: HashMap<u32, Vec<ActorInfo>> = HashMap::new();
        updates2.insert(
            fragment_id,
            vec![ActorInfo {
                fragment_id: 235,
                host: Some(HostAddress {
                    host: String::from("127.0.0.1"),
                    port: 2333,
                }),
            }],
        );
        let b2 = Barrier {
            epoch: 0,
            mutation: Mutation::UpdateOutputs(updates2),
        };

        tx.send(Message::Barrier(b2)).await.unwrap();
        executor.next().await.unwrap();
        let tctx = ctx.clone();
        {
            let cp_guard = tctx.channel_pool.lock().unwrap();
            let ex_guard = tctx.receivers_for_exchange_service.lock().unwrap();
            assert_eq!(cp_guard.len(), 1);
            assert_eq!(ex_guard.len(), 0);
        }
    }

    #[tokio::test]
    async fn test_hash_dispatcher() {
        let num_outputs = 5;
        let cardinality = 10;
        let dimension = 4;
        let key_indices = &[0, 2];
        let output_data_vecs = (0..num_outputs)
            .map(|_| Arc::new(Mutex::new(Vec::new())))
            .collect::<Vec<_>>();
        let outputs = output_data_vecs
            .iter()
            .map(|data| Box::new(MockOutput::new(data.clone())) as Box<dyn Output>)
            .collect::<Vec<_>>();
        let mut hash_dispatcher = HashDataDispatcher::new(outputs, key_indices.to_vec());

        let mut ops = Vec::new();
        for idx in 0..cardinality {
            if idx % 2 == 0 {
                ops.push(Op::Insert);
            } else {
                ops.push(Op::Delete);
            }
        }

        let mut start = 19260817i32..;
        let mut builders = (0..dimension)
            .map(|_| I32ArrayBuilder::new(cardinality).unwrap())
            .collect_vec();
        let mut output_cols = vec![vec![vec![]; dimension]; num_outputs];
        let mut output_ops = vec![vec![]; num_outputs];
        for op in &ops {
            let hash_builder = CRC32FastBuilder {};
            let mut hasher = hash_builder.build_hasher();
            let one_row = (0..dimension).map(|_| start.next().unwrap()).collect_vec();
            for key_idx in key_indices.iter() {
                let val = one_row[*key_idx];
                let bytes = val.to_le_bytes();
                hasher.update(&bytes);
            }
            let output_idx = hasher.finish() as usize % num_outputs;
            for (builder, val) in builders.iter_mut().zip(one_row.iter()) {
                builder.append(Some(*val)).unwrap();
            }
            output_cols[output_idx]
                .iter_mut()
                .zip(one_row.iter())
                .for_each(|(each_column, val)| each_column.push(*val));
            output_ops[output_idx].push(op);
        }

        let columns = builders
            .into_iter()
            .map(|builder| {
                let array = builder.finish().unwrap();
                Column::new(Arc::new(array.into()))
            })
            .collect::<Vec<_>>();

        let chunk = StreamChunk {
            ops,
            columns,
            visibility: None,
        };
        hash_dispatcher.dispatch_data(chunk).await.unwrap();

        for (output_idx, output) in output_data_vecs.into_iter().enumerate() {
            let guard = output.lock().unwrap();
            assert_eq!(guard.len(), 1);
            let message = guard.get(0).unwrap();
            let real_chunk = match message {
                Message::Chunk(chunk) => chunk,
                _ => panic!(),
            };
            real_chunk
                .columns
                .iter()
                .zip(output_cols[output_idx].iter())
                .for_each(|(real_col, expect_col)| {
                    let real_vals = real_chunk
                        .visibility
                        .as_ref()
                        .unwrap()
                        .iter()
                        .enumerate()
                        .filter(|(_, vis)| *vis)
                        .map(|(row_idx, _)| {
                            real_col.array_ref().as_int32().value_at(row_idx).unwrap()
                        })
                        .collect::<Vec<_>>();
                    assert_eq!(real_vals.len(), expect_col.len());
                    assert_eq!(real_vals, *expect_col);
                });
        }
    }
}
