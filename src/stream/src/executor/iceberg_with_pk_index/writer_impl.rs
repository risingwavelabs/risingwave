// Copyright 2026 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use iceberg::table::Table;
use iceberg::writer::PositionDeleteInput;
use risingwave_common::array::DataChunk;
use risingwave_connector::sink::SinkWriterParam;
use risingwave_connector::sink::iceberg::{IcebergConfig, IcebergSinkWriterInner};
use risingwave_pb::id::SinkId;

use super::writer::{IcebergWriter, IcebergWriterFlushOutput};
use crate::executor::{StreamExecutorError, StreamExecutorResult};

pub struct IcebergWriterImpl {
    inner: IcebergSinkWriterInner,
    sink_id: SinkId,
}

impl IcebergWriterImpl {
    pub fn build(
        config: &IcebergConfig,
        table: Table,
        writer_param: &SinkWriterParam,
    ) -> StreamExecutorResult<Self> {
        let sink_id = writer_param.sink_id;
        let inner = IcebergSinkWriterInner::build_append_only(config, table, writer_param)
            .map_err(|e| StreamExecutorError::sink_error(e, sink_id))?;

        Ok(Self { inner, sink_id })
    }
}

#[async_trait::async_trait]
impl IcebergWriter for IcebergWriterImpl {
    async fn write_chunk(
        &mut self,
        chunk: DataChunk,
    ) -> StreamExecutorResult<Vec<PositionDeleteInput>> {
        let positions = self
            .inner
            .write_batch_with_position(chunk.into())
            .await
            .map_err(|e| StreamExecutorError::sink_error(e, self.sink_id))?;
        Ok(positions)
    }

    async fn flush(&mut self) -> StreamExecutorResult<Option<IcebergWriterFlushOutput>> {
        let Some(data_files) = self
            .inner
            .close()
            .await
            .map_err(|e| StreamExecutorError::sink_error(e, self.sink_id))?
        else {
            return Ok(None);
        };

        let partition_type = self.inner.partition_type();
        let partitions = data_files
            .iter()
            .map(|f| {
                Ok((
                    f.file_path().to_owned(),
                    super::serialize_partition_struct(f.partition(), partition_type)?,
                ))
            })
            .collect::<StreamExecutorResult<Vec<_>>>()?;
        let metadata = self
            .inner
            .generate_commit_metadata(data_files)
            .map_err(|e| StreamExecutorError::sink_error(e, self.sink_id))?;

        Ok(Some((metadata, partitions)))
    }
}
