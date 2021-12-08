use std::fmt::{Debug, Formatter};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use futures::channel::mpsc::UnboundedReceiver;
use futures::StreamExt;

use risingwave_common::array::column::Column;
use risingwave_common::array::{
    ArrayBuilder, ArrayImpl, I64ArrayBuilder, InternalError, RwError, StreamChunk,
};
use risingwave_common::catalog::Schema;
use risingwave_common::error::ErrorCode::ProtocolError;
use risingwave_common::error::Result;
use risingwave_common::types::Int64Type;
use SourceReaderContext::HighLevelKafka;

use crate::source::{
    HighLevelKafkaSourceReaderContext, SourceDesc, SourceImpl, SourceReaderContext,
    StreamSourceReader,
};
use crate::stream_op::PKVec;
use crate::stream_op::{Executor, Message};

/// `StreamSourceExecutor` is a streaming source from external systems such as Kafka
pub struct StreamSourceExecutor {
    source_desc: SourceDesc,
    column_ids: Vec<i32>,
    schema: Schema,
    pk_indices: PKVec,
    reader: Box<dyn StreamSourceReader>,
    barrier_receiver: UnboundedReceiver<Message>,
    /// current allocated row id
    next_row_id: AtomicU64,
}

impl StreamSourceExecutor {
    pub fn new(
        source_desc: SourceDesc,
        column_ids: Vec<i32>,
        schema: Schema,
        pk_indices: PKVec,
        barrier_receiver: UnboundedReceiver<Message>,
    ) -> Result<Self> {
        let source = source_desc.clone().source;
        let reader = match source.as_ref() {
            SourceImpl::HighLevelKafka(_) => source.stream_reader(
                HighLevelKafka(HighLevelKafkaSourceReaderContext {
                    query_id: None,
                    bound_timestamp_ms: None,
                }),
                column_ids.clone(),
            ),
            _ => Err(RwError::from(ProtocolError(
                "Stream source only supports external source".to_string(),
            ))),
        }?;

        Ok(Self {
            source_desc,
            column_ids,
            schema,
            pk_indices,
            reader,
            barrier_receiver,
            next_row_id: AtomicU64::from(0u64),
        })
    }
}

impl StreamSourceExecutor {
    fn gen_row_column(&mut self, len: usize) -> Column {
        let mut builder = I64ArrayBuilder::new(len).unwrap();

        for _ in 0..len {
            builder
                .append(Some(self.next_row_id.fetch_add(1, Ordering::Relaxed) as i64))
                .unwrap();
        }

        Column::new(
            Arc::new(ArrayImpl::from(builder.finish().unwrap())),
            Int64Type::create(false),
        )
    }

    fn refill_row_id_column(&mut self, chunk: &mut StreamChunk) {
        if let Some(idx) = self
            .column_ids
            .iter()
            .position(|column_id| *column_id == self.source_desc.row_id_column_id)
        {
            chunk.columns[idx] = self.gen_row_column(chunk.cardinality());
        }
    }
}

#[async_trait]
impl Executor for StreamSourceExecutor {
    async fn next(&mut self) -> Result<Message> {
        tokio::select! {
          chunk = self.reader.next() => {
             chunk.map(|mut c| {
              self.refill_row_id_column(&mut c);
              Message::Chunk(c)
            })
          }
          message = self.barrier_receiver.next() => {
            message.ok_or_else(|| RwError::from(InternalError("stream closed unexpectedly".to_string())))
          }
        }
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn pk_indices(&self) -> &[usize] {
        &self.pk_indices
    }
}

impl Debug for StreamSourceExecutor {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamSourceExecutor").finish()
    }
}
