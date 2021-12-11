use risingwave_common::error::Result;

use crate::stream_op::StreamConsumer;

/// `Actor` is the basic execution unit in the streaming framework.
pub struct Actor {
    consumer: Box<dyn StreamConsumer>,
}

impl Actor {
    pub fn new(consumer: Box<dyn StreamConsumer>) -> Self {
        Self { consumer }
    }

    pub async fn run(mut self) -> Result<()> {
        // Drive the streaming task with an infinite loop
        loop {
            let has_next = self.consumer.next().await?;
            if !has_next {
                break;
            }
        }
        Ok(())
    }
}
