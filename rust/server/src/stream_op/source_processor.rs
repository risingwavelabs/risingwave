use crate::error::Result;
use crate::stream_op::{DataSource, Output, Processor};
use async_trait::async_trait;
use futures::channel::oneshot;

/// `SourceProcessor` wraps a `DataSource` into a `Processor`
pub struct SourceProcessor {
    data_source: Box<dyn DataSource>,
    output: Option<Box<dyn Output>>,
    cancel: Option<oneshot::Receiver<()>>,
}

impl SourceProcessor {
    pub fn new(
        data_source: Box<dyn DataSource>,
        output: Box<dyn Output>,
        cancel: oneshot::Receiver<()>,
    ) -> Self {
        Self {
            data_source,
            output: Some(output),
            cancel: Some(cancel),
        }
    }
}

#[async_trait]
impl Processor for SourceProcessor {
    async fn run(mut self) -> Result<()> {
        use log::info;

        let output = self.output.take().unwrap();
        let cancel = self.cancel.take().unwrap();

        info!("Source {:?} starts", self.data_source);
        self.data_source.run(output, cancel).await?;
        info!("Source {:?} terminated", self.data_source);

        Ok(())
    }
}
