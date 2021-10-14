use super::{Message, Result, StreamChunk};
use crate::array::DataChunk;
use crate::buffer::Bitmap;
use crate::stream_op::{StreamConsumer, StreamOperator};
use crate::util::hash_util::CRC32FastBuilder;
use async_trait::async_trait;
use futures::channel::mpsc::Sender;
use futures::SinkExt;

/// `Output` provides an interface for `Dispatcher` to send data into downstream fragments.
#[async_trait]
pub trait Output: Send + Sync + 'static {
    async fn send(&mut self, message: Message) -> Result<()>;
}

/// `ChannelOutput` sends data to a local `mpsc::Channel`
pub struct ChannelOutput {
    ch: Sender<Message>,
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

/// `DispatchExecutor` consumes messages and send them into downstream actors. Usually,
/// data chunks will be dispatched with some specified policy, while control message
/// such as barriers will be distributed to all receivers.
pub struct DispatchExecutor<Inner: DataDispatcher> {
    input: Box<dyn StreamOperator>,
    inner: Inner,
}

impl<Inner: DataDispatcher + Send> DispatchExecutor<Inner> {
    pub fn new(input: Box<dyn StreamOperator>, inner: Inner) -> Self {
        Self { input, inner }
    }

    async fn dispatch(&mut self, msg: Message) -> Result<()> {
        if let Message::Chunk(chunk) = msg {
            self.inner.dispatch_data(chunk).await
        } else {
            let outputs = self.inner.get_outputs();
            for output in outputs {
                // TODO: clone here
                output
                    .send(match msg {
                        Message::Barrier(epoch) => Message::Barrier(epoch),
                        Message::Terminate => Message::Terminate,
                        _ => unreachable!(),
                    })
                    .await?;
            }
            Ok(())
        }
    }
}

#[async_trait]
impl<Inner: DataDispatcher + Send + Sync + 'static> StreamConsumer for DispatchExecutor<Inner> {
    async fn next(&mut self) -> Result<bool> {
        let msg = self.input.next().await?;
        let terminated = matches!(msg, Message::Terminate);
        self.dispatch(msg).await?;
        Ok(!terminated)
    }
}

#[async_trait]
pub trait DataDispatcher {
    async fn dispatch_data(&mut self, chunk: StreamChunk) -> Result<()>;
    fn get_outputs(&mut self) -> &mut [Box<dyn Output>];
}

pub struct BlackHoleDispatcher {}

impl BlackHoleDispatcher {
    pub fn new() -> Self {
        Self {}
    }
}

#[async_trait]
impl DataDispatcher for BlackHoleDispatcher {
    async fn dispatch_data(&mut self, _chunk: StreamChunk) -> Result<()> {
        Ok(())
    }

    fn get_outputs(&mut self) -> &mut [Box<dyn Output>] {
        &mut []
    }
}

pub struct RoundRobinDataDispatcher {
    outputs: Vec<Box<dyn Output>>,
    cur: usize,
}

impl RoundRobinDataDispatcher {
    pub fn new(outputs: Vec<Box<dyn Output>>) -> Self {
        Self { outputs, cur: 0 }
    }
}

#[async_trait]
impl DataDispatcher for RoundRobinDataDispatcher {
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

impl HashDataDispatcher {
    pub fn new(outputs: Vec<Box<dyn Output>>, keys: Vec<usize>) -> Self {
        Self { outputs, keys }
    }
}

#[async_trait]
impl DataDispatcher for HashDataDispatcher {
    fn get_outputs(&mut self) -> &mut [Box<dyn Output>] {
        &mut self.outputs
    }

    async fn dispatch_data(&mut self, chunk: StreamChunk) -> Result<()> {
        let num_outputs = self.outputs.len();
        let StreamChunk {
            ops,
            columns: arrays,
            visibility,
        } = chunk;

        let data_chunk = {
            let data_chunk_builder = DataChunk::builder().columns(arrays.clone());
            if let Some(visibility) = visibility {
                data_chunk_builder.visibility(visibility).build()
            } else {
                data_chunk_builder.build()
            }
        };
        let hash_builder = CRC32FastBuilder {};
        let hash_values = data_chunk
            .get_hash_values(&self.keys, hash_builder)
            .unwrap()
            .iter()
            .map(|hash| *hash as usize % num_outputs)
            .collect::<Vec<_>>();

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

        for (vis_map, output) in vis_maps.into_iter().zip(self.outputs.iter_mut()) {
            let vis_map = Bitmap::from_vec(vis_map).unwrap();
            let new_stream_chunk = StreamChunk {
                ops: ops.clone(),
                columns: arrays.clone(),
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

impl BroadcastDispatcher {
    pub fn new(outputs: Vec<Box<dyn Output>>) -> Self {
        Self { outputs }
    }
}

#[async_trait]
impl DataDispatcher for BroadcastDispatcher {
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

/// `SimpleDispatcher` dispatches message to a single
pub struct SimpleDispatcher {
    output: Box<dyn Output>,
}

impl SimpleDispatcher {
    pub fn new(output: Box<dyn Output>) -> Self {
        Self { output }
    }
}

#[async_trait]
impl DataDispatcher for SimpleDispatcher {
    fn get_outputs(&mut self) -> &mut [Box<dyn Output>] {
        std::slice::from_mut(&mut self.output)
    }

    async fn dispatch_data(&mut self, chunk: StreamChunk) -> Result<()> {
        self.output.send(Message::Chunk(chunk)).await?;
        Ok(())
    }
}

/// `SenderConsumer` consumes data from input operator and send it into a channel
pub struct SenderConsumer {
    input: Box<dyn StreamOperator>,
    channel: Box<dyn Output>,
}

impl SenderConsumer {
    pub fn new(input: Box<dyn StreamOperator>, channel: Box<dyn Output>) -> Self {
        Self { input, channel }
    }
}

#[async_trait]
impl StreamConsumer for SenderConsumer {
    async fn next(&mut self) -> Result<bool> {
        let message = self.input.next().await?;
        let terminated = matches!(message, Message::Terminate);
        self.channel.send(message).await?;
        Ok(!terminated)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::array::column::Column;
    use crate::array::{Array, ArrayBuilder, I32ArrayBuilder};
    use crate::stream_op::Op;
    use crate::types::Int32Type;
    use std::hash::{BuildHasher, Hasher};
    use std::sync::{Arc, Mutex};

    pub struct MockOutput {
        data: Arc<Mutex<Vec<Message>>>,
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

        let mut start = 19260817..;
        let mut builders = (0..dimension)
            .map(|_| I32ArrayBuilder::new(cardinality).unwrap())
            .collect::<Vec<_>>();
        let mut output_cols = vec![vec![vec![]; dimension]; num_outputs];
        let mut output_ops = vec![vec![]; num_outputs];
        for op in ops.iter() {
            let hash_builder = CRC32FastBuilder {};
            let mut hasher = hash_builder.build_hasher();
            let one_row: Vec<i32> = (0..dimension)
                .map(|_| start.next().unwrap())
                .collect::<Vec<_>>();
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
                Column::new(Arc::new(array.into()), Int32Type::create(false))
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
