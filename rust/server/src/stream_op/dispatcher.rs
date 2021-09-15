use super::{Message, Output, Result, StreamChunk};
use async_trait::async_trait;

pub struct Dispatcher<Inner: DataDispatcher> {
    inner: Inner,
}

impl<Inner: DataDispatcher> Dispatcher<Inner> {
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
pub trait DataDispatcher {
    async fn dispatch_data(&mut self, chunk: StreamChunk) -> Result<()>;
    fn get_outputs(&mut self) -> &mut [Box<dyn Output>];
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
