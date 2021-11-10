use std::fmt::{Debug, Formatter};

use itertools::Itertools;
use prost::Message;

use pb_convert::FromProtobuf;
use risingwave_pb::plan::StreamScanNode;
use risingwave_pb::ToProto;

use crate::executor::{Executor, ExecutorBuilder, ExecutorResult};
use crate::source::{ChunkReader, JSONParser, SourceColumnDesc, SourceFormat, SourceParser};
use risingwave_common::array::DataChunk;
use risingwave_common::catalog::TableId;
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::error::ErrorCode::{InternalError, ProstError};
use risingwave_common::error::{Result, RwError};

use super::{BoxedExecutor, BoxedExecutorBuilder};

const K_STREAM_SCAN_CHUNK_SIZE: usize = 1024;

pub struct StreamScanExecutor {
    reader: ChunkReader,
    columns: Vec<SourceColumnDesc>,
    done: bool,
    schema: Schema,
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

    pub fn columns(&self) -> &[SourceColumnDesc] {
        self.columns.as_slice()
    }
}

impl BoxedExecutorBuilder for StreamScanExecutor {
    /// This function is designed for OLAP to initialize the `StreamScanExecutor`
    /// Things needed for initialization is
    /// 1. `StreamScanNode` whose definition can be shared by OLAP and Streaming
    /// 2. `SourceManager` whose definition can also be shared. But is it physically shared?
    fn new_boxed_executor(source: &ExecutorBuilder) -> Result<BoxedExecutor> {
        let stream_scan_node = StreamScanNode::decode(&(source.plan_node()).get_body().value[..])
            .map_err(ProstError)?;

        let table_id = TableId::from_protobuf(
            stream_scan_node
                .to_proto::<risingwave_proto::plan::StreamScanNode>()
                .get_table_ref_id(),
        )
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

        let fields = columns
            .iter()
            .map(|col| Field {
                data_type: col.data_type.clone(),
            })
            .collect::<Vec<Field>>();

        let parser: Box<dyn SourceParser> = match source_desc.format {
            SourceFormat::Json => Box::new(JSONParser {}),
            _ => unimplemented!(),
        };

        Ok(Box::new(Self {
            reader: ChunkReader::new(&columns, source_desc.source.reader()?, parser),
            columns,
            done: false,
            schema: Schema { fields },
        }))
    }
}

#[async_trait::async_trait]
impl Executor for StreamScanExecutor {
    fn init(&mut self) -> Result<()> {
        // TODO: make init() async
        // self.reader.init().await?;
        Ok(())
    }

    async fn execute(&mut self) -> Result<ExecutorResult> {
        if self.done {
            return Ok(ExecutorResult::Done);
        }
        let next_chunk = self.next_data_chunk().await?;
        match next_chunk {
            Some(chunk) => Ok(ExecutorResult::Batch(chunk)),
            None => Ok(ExecutorResult::Done),
        }
    }

    fn clean(&mut self) -> Result<()> {
        async_std::task::block_on(self.reader.cancel())
    }

    fn schema(&self) -> &Schema {
        &self.schema
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
