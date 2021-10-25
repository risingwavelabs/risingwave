use std::fmt::{Debug, Formatter};

use itertools::Itertools;
use protobuf::Message;

use pb_convert::FromProtobuf;
use risingwave_proto::plan::StreamScanNode;

use crate::array::DataChunk;
use crate::catalog::TableId;
use crate::error::ErrorCode;
use crate::error::ErrorCode::InternalError;
use crate::error::{Result, RwError};
use crate::executor::{Executor, ExecutorBuilder, ExecutorResult};
use crate::source::{ChunkReader, JSONParser, SourceColumnDesc, SourceFormat, SourceParser};

use super::{BoxedExecutor, BoxedExecutorBuilder};

const K_STREAM_SCAN_CHUNK_SIZE: usize = 1024;

pub struct StreamScanExecutor {
    reader: ChunkReader,
    columns: Vec<SourceColumnDesc>,
    done: bool,
}

impl StreamScanExecutor {
    /// This function returns `DataChunkRef` because it will be used by both olap and streaming.
    /// The streaming side may customize a bit from `DataChunk` to `StreamChunk`.
    pub async fn next_data_chunk(&mut self) -> Result<Option<DataChunk>> {
        match self.reader.next_chunk(K_STREAM_SCAN_CHUNK_SIZE).await? {
            None => Ok(None),
            Some(chunk) => Ok(Some(chunk)),
        }
    }
}

impl BoxedExecutorBuilder for StreamScanExecutor {
    /// This function is designed for OLAP to initialize the `StreamScanExecutor`
    /// Things needed for initialization is
    /// 1. `StreamScanNode` whose definition can be shared by OLAP and Streaming
    /// 2. `SourceManager` whose definition can also be shared. But is it physically shared?
    fn new_boxed_executor(source: &ExecutorBuilder) -> Result<BoxedExecutor> {
        let stream_scan_node = unpack_from_any!(source.plan_node().get_body(), StreamScanNode);

        let table_id = TableId::from_protobuf(stream_scan_node.get_table_ref_id())
            .map_err(|e| InternalError(format!("Failed to parse table id: {:?}", e)))?;

        let source_desc = source
            .global_task_env()
            .source_manager()
            .get_source(&table_id)?;

        let column_idxes = stream_scan_node
            .get_column_ids()
            .iter()
            .map(|id| {
                source_desc
                    .columns
                    .iter()
                    .position(|c| c.column_id == *id)
                    .ok_or_else::<RwError, _>(|| {
                        InternalError(format!(
                            "column id {:?} not found in table {:?}",
                            id, table_id
                        ))
                        .into()
                    })
            })
            .try_collect::<_, Vec<usize>, _>()?;

        let columns = column_idxes
            .iter()
            .map(|idx| source_desc.columns[*idx].clone())
            .collect::<Vec<SourceColumnDesc>>();

        let parser: Box<dyn SourceParser> = match source_desc.format {
            SourceFormat::Json => Box::new(JSONParser {}),
            _ => unimplemented!(),
        };

        Ok(Box::new(Self {
            reader: ChunkReader::new(&columns, source_desc.source.reader()?, parser),
            columns,
            done: false,
        }))
    }
}

#[async_trait::async_trait]
impl Executor for StreamScanExecutor {
    fn init(&mut self) -> Result<()> {
        Ok(())
    }

    async fn execute(&mut self) -> Result<ExecutorResult> {
        if self.done {
            return Ok(ExecutorResult::Done);
        }
        let next_chunk = async_std::task::block_on(self.next_data_chunk())?;
        match next_chunk {
            Some(chunk) => Ok(ExecutorResult::Batch(chunk)),
            None => Ok(ExecutorResult::Done),
        }
    }

    fn clean(&mut self) -> Result<()> {
        async_std::task::block_on(self.reader.cancel())
    }
}

impl Debug for StreamScanExecutor {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamScanExecutor")
            .field("reader", &self.reader)
            .field("columns", &self.columns)
            .field("done", &self.done)
            .finish()
    }
}
