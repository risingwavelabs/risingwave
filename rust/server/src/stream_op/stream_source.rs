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
use crate::stream_op::{Executor, Message};

/// `StreamSourceExecutor` is a streaming source from external systems such as Kafka
pub struct StreamSourceExecutor {
    source_desc: SourceDesc,
    column_ids: Vec<i32>,
    schema: Schema,
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
        barrier_receiver: UnboundedReceiver<Message>,
    ) -> Result<Self> {
        let source = source_desc.clone().source;
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
            source_desc,
            column_ids,
            schema,
            reader,
            barrier_receiver,
            next_row_id: AtomicU64::from(0u64),
        })
    }
}

impl StreamSourceExecutor {
    fn add_row_id(chunk: &mut StreamChunk, column: Column) {
        let mut cols = Vec::with_capacity(chunk.columns.len() + 1);
        cols.insert(0, column);
        cols.append(&mut chunk.columns);
        chunk.columns = cols;
    }

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

    fn rearrange_chunk_by_schema(&mut self, chunk: &mut StreamChunk) {
        let card = chunk.cardinality();

        Self::add_row_id(chunk, self.gen_row_column(card));

        let mut columns = vec![];

        for id in self.column_ids.iter() {
            let (idx, _) = self
                .source_desc
                .columns
                .iter()
                .enumerate()
                .find(|i| i.1.column_id == *id)
                .unwrap();
            columns.push(chunk.columns[idx].clone());
        }

        chunk.columns = columns;
    }
}

#[async_trait]
impl Executor for StreamSourceExecutor {
    async fn next(&mut self) -> Result<Message> {
        tokio::select! {
          chunk = self.reader.next() => {
             chunk.map(|mut c| {
              self.rearrange_chunk_by_schema(&mut c);
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
}

impl Debug for StreamSourceExecutor {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamSourceExecutor").finish()
    }
}
