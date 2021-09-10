use super::{Result, UnaryStreamOperator};
use async_trait::async_trait;
use futures::channel::mpsc::Receiver;
use futures::StreamExt;

use super::Message;

#[async_trait]
pub trait Merger {
    async fn run(self) -> Result<()>;
}

pub struct UnaryMerger {
    inputs: Vec<Receiver<Message>>,
    operator_head: Box<dyn UnaryStreamOperator>,
}

#[async_trait]
impl Merger for UnaryMerger {
    async fn run(mut self) -> Result<()> {
        let fs = self
            .inputs
            .into_iter()
            .enumerate()
            .map(|(id, receiver)| receiver.map(move |x| (id, x)));
        let mut stream = futures::stream::select_all(fs);
        while let Some((_channel_id, msg)) = stream.next().await {
            match msg {
                Message::Barrier(_barrier_id) => todo!("align barriers here."),
                Message::Chunk(chunk) => self.operator_head.consume(chunk).await?,
                Message::Terminate => todo!("cleanup here."),
            }
        }
        Ok(())
    }
}
