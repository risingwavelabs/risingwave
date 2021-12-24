use risingwave_common::error::Result;

use super::StreamConsumer;

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
            match self.consumer.next().await {
                Ok(has_next) => {
                    if !has_next {
                        break;
                    }
                }
                Err(err) => {
                    warn!("Actor polling failed: {:?}", err);
                    return Err(err);
                }
            }
        }
        Ok(())
    }
}
