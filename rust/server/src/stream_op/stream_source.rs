use std::fmt::{Debug, Formatter};

use async_trait::async_trait;
use futures::channel::mpsc::UnboundedReceiver;
use futures::StreamExt;
use itertools::Itertools;

use risingwave_common::array::{InternalError, RwError};
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::error::ErrorCode::ProtocolError;
use risingwave_common::error::Result;
use SourceReaderContext::HighLevelKafka;

use crate::source::{
    HighLevelKafkaSourceReaderContext, SourceDesc, SourceImpl, SourceReaderContext,
    StreamSourceReader,
};
use crate::stream_op::{Executor, Message};

/// `StreamSourceExecutor` is a streaming source from external systems such as Kafka
pub struct StreamSourceExecutor {
    schema: Schema,
    reader: Box<dyn StreamSourceReader>,
    barrier_receiver: UnboundedReceiver<Message>,
}

impl StreamSourceExecutor {
    pub fn new(
        source_desc: SourceDesc,
        barrier_receiver: UnboundedReceiver<Message>,
    ) -> Result<Self> {
        let fields = source_desc
            .columns
            .iter()
            .map(|col| Field {
                data_type: col.data_type.clone(),
            })
            .collect_vec();

        let schema = Schema { fields };

        let source = source_desc.source;
        let reader = match source.as_ref() {
            SourceImpl::HighLevelKafka(_) => source.stream_reader(
                HighLevelKafka(HighLevelKafkaSourceReaderContext {
                    query_id: None,
                    bound_timestamp_ms: None,
                }),
                vec![],
            ),
            _ => Err(RwError::from(ProtocolError(
                "Stream source only supports external source".to_string(),
            ))),
        }?;

        Ok(Self {
            schema,
            reader,
            barrier_receiver,
        })
    }
}

#[async_trait]
impl Executor for StreamSourceExecutor {
    async fn next(&mut self) -> Result<Message> {
        tokio::select! {
          chunk = self.reader.next() => {
            chunk.map(Message::Chunk)
          }
          message = self.barrier_receiver.next() => {
            message.ok_or_else(|| RwError::from(InternalError("stream closed unexpectedly".to_string())))
          }
        }
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}

impl Debug for StreamSourceExecutor {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamSourceExecutor").finish()
    }
}
