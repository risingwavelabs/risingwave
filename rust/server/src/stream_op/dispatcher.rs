use super::{Message, Result, StreamChunk};
use crate::array::DataChunk;
use crate::buffer::Bitmap;
use crate::stream_op::{StreamConsumer, StreamOperator};
use crate::util::hash_util::CRC32FastBuilder;
use async_trait::async_trait;
use futures::channel::mpsc::Sender;
use futures::SinkExt;

// Note(eric): the wrapper struct `Dispatcher` seems redundant. We could implement them
// as StreamConsumer directly like `SenderConsumer`
pub struct Dispatcher<Inner: DataDispatcher> {
    input: Box<dyn StreamOperator>,
    inner: Inner,
}

impl<Inner: DataDispatcher + Send> Dispatcher<Inner> {
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
                    .await
                    .unwrap(); // TODO: do not use unwrap
            }
            Ok(())
        }
    }
}

#[async_trait]
impl<Inner: DataDispatcher + Send + Sync + 'static> StreamConsumer for Dispatcher<Inner> {
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
    fn get_outputs(&mut self) -> &mut [Sender<Message>];
}

pub struct BlackHoleDispatcher(Vec<Sender<Message>>);

impl BlackHoleDispatcher {
    pub fn new() -> Self {
        Self(vec![])
    }
}

#[async_trait]
impl DataDispatcher for BlackHoleDispatcher {
    async fn dispatch_data(&mut self, _chunk: StreamChunk) -> Result<()> {
        Ok(())
    }

    fn get_outputs(&mut self) -> &mut [Sender<Message>] {
        &mut self.0
    }
}

pub struct RoundRobinDataDispatcher {
    outputs: Vec<Sender<Message>>,
    cur: usize,
}

impl RoundRobinDataDispatcher {
    pub fn new(outputs: Vec<Sender<Message>>) -> Self {
        Self { outputs, cur: 0 }
    }
}

#[async_trait]
impl DataDispatcher for RoundRobinDataDispatcher {
    fn get_outputs(&mut self) -> &mut [Sender<Message>] {
        &mut self.outputs
    }

    async fn dispatch_data(&mut self, chunk: StreamChunk) -> Result<()> {
        self.outputs[self.cur]
            .send(Message::Chunk(chunk))
            .await
            .unwrap();
        self.cur += 1;
        self.cur %= self.outputs.len();
        Ok(())
    }
}

pub struct HashDataDispatcher {
    outputs: Vec<Sender<Message>>,
    keys: Vec<usize>,
}

impl HashDataDispatcher {
    pub fn new(outputs: Vec<Sender<Message>>, keys: Vec<usize>) -> Self {
        Self { outputs, keys }
    }
}

#[async_trait]
impl DataDispatcher for HashDataDispatcher {
    fn get_outputs(&mut self) -> &mut [Sender<Message>] {
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
            output.send(Message::Chunk(new_stream_chunk)).await.unwrap();
        }
        Ok(())
    }
}

/// `BroadcastDispatcher` dispatches message to all outputs.
pub struct BroadcastDispatcher {
    outputs: Vec<Sender<Message>>,
}

impl BroadcastDispatcher {
    pub fn new(outputs: Vec<Sender<Message>>) -> Self {
        Self { outputs }
    }
}

#[async_trait]
impl DataDispatcher for BroadcastDispatcher {
    fn get_outputs(&mut self) -> &mut [Sender<Message>] {
        &mut self.outputs
    }

    async fn dispatch_data(&mut self, chunk: StreamChunk) -> Result<()> {
        for output in &mut self.outputs {
            output.send(Message::Chunk(chunk.clone())).await.unwrap(); // TODO: do not use unwrap
        }
        Ok(())
    }
}

/// `SimpleDispatcher` dispatches message to a single
pub struct SimpleDispatcher {
    output: Sender<Message>,
}

impl SimpleDispatcher {
    pub fn new(output: Sender<Message>) -> Self {
        Self { output }
    }
}

#[async_trait]
impl DataDispatcher for SimpleDispatcher {
    fn get_outputs(&mut self) -> &mut [Sender<Message>] {
        std::slice::from_mut(&mut self.output)
    }

    async fn dispatch_data(&mut self, chunk: StreamChunk) -> Result<()> {
        self.output.send(Message::Chunk(chunk)).await.unwrap();
        Ok(())
    }
}

/// `SenderConsumer` consumes data from input operator and send it into a channel
pub struct SenderConsumer {
    input: Box<dyn StreamOperator>,
    channel: Sender<Message>,
}

impl SenderConsumer {
    pub fn new(input: Box<dyn StreamOperator>, channel: Sender<Message>) -> Self {
        Self { input, channel }
    }
}

#[async_trait]
impl StreamConsumer for SenderConsumer {
    async fn next(&mut self) -> Result<bool> {
        let message = self.input.next().await?;
        let terminated = matches!(message, Message::Terminate);
        self.channel.send(message).await.unwrap(); // TODO: do not use unwrap
        Ok(!terminated)
    }
}
