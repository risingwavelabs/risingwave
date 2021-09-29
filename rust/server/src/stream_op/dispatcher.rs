use super::{Message, Output, Result, StreamChunk};
use crate::array2::DataChunk;
use crate::buffer::Bitmap;
use crate::util::hash_util::CRC32FastBuilder;
use async_trait::async_trait;

pub struct Dispatcher<Inner: DataDispatcher> {
    inner: Inner,
}

impl<Inner: DataDispatcher + Send> Dispatcher<Inner> {
    pub fn new(inner: Inner) -> Self {
        Self { inner }
    }

    pub async fn dispatch(&mut self, msg: Message) -> Result<()> {
        if let Message::Chunk(chunk) = msg {
            self.inner.dispatch_data(chunk).await
        } else {
            let outputs = self.inner.get_outputs();
            for output in outputs {
                // TODO: clone here
                output
                    .collect(match msg {
                        Message::Barrier(epoch) => Message::Barrier(epoch),
                        Message::Terminate => Message::Terminate,
                        _ => unreachable!(),
                    })
                    .await?
            }
            Ok(())
        }
    }
}

#[async_trait]
impl<Inner: DataDispatcher + Send + Sync + 'static> Output for Dispatcher<Inner> {
    async fn collect(&mut self, msg: Message) -> Result<()> {
        self.dispatch(msg).await
    }
}

#[async_trait]
pub trait DataDispatcher {
    async fn dispatch_data(&mut self, chunk: StreamChunk) -> Result<()>;
    fn get_outputs(&mut self) -> &mut [Box<dyn Output>];
}

pub struct BlackHoleDispatcher(Vec<Box<dyn Output>>);

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

    fn get_outputs(&mut self) -> &mut [Box<dyn Output>] {
        &mut self.0
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
        self.outputs[self.cur]
            .collect(Message::Chunk(chunk))
            .await?;
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
            output.collect(Message::Chunk(new_stream_chunk)).await?;
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
            output.collect(Message::Chunk(chunk.clone())).await?;
        }
        Ok(())
    }
}
