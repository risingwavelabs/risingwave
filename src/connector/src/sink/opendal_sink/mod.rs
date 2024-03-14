// Copyright 2024 RisingWave Labs
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

pub mod gcs;
pub mod s3;
use std::collections::HashMap;
use std::sync::Arc;

use anyhow::anyhow;
use arrow_schema::{DataType as ArrowDataType, Field as ArrowField, SchemaRef};
use async_trait::async_trait;
use icelake::config::ParquetWriterConfig;
use opendal::{Operator, Writer as OpendalWriter};
use parquet::arrow::AsyncArrowWriter;
use parquet::file::properties::{WriterProperties, WriterVersion};
use risingwave_common::array::{to_record_batch_with_schema, Op, StreamChunk};
use risingwave_common::buffer::Bitmap;
use risingwave_common::catalog::Schema;

use crate::sink::{Result, SinkError, SinkWriter};

const SINK_WRITE_BUFFER_SIZE: usize = 16 * 1024 * 1024;

pub struct OpenDalSinkWriter {
    schema: SchemaRef,
    operator: Operator,
    sink_writer: Option<AsyncArrowWriter<OpendalWriter>>,
    pk_indices: Vec<usize>,
    is_append_only: bool,
    write_path: String,
}

#[async_trait]
impl SinkWriter for OpenDalSinkWriter {
    async fn write_batch(&mut self, chunk: StreamChunk) -> Result<()> {
        if self.sink_writer.is_none() {
            self.create_sink_writer().await?;
        }
        if self.is_append_only {
            self.append_only(chunk).await
        } else {
            unimplemented!()
        }
    }

    async fn begin_epoch(&mut self, _epoch: u64) -> Result<()> {
        Ok(())
    }

    async fn abort(&mut self) -> Result<()> {
        Ok(())
    }

    async fn barrier(&mut self, is_checkpoint: bool) -> Result<()> {
        if is_checkpoint {
            let sink_writer = self
                .sink_writer
                .take()
                .ok_or_else(|| SinkError::Opendal("Can't get sink writer".to_string()))?;
            sink_writer.close().await?;
        }

        Ok(())
    }

    async fn update_vnode_bitmap(&mut self, _vnode_bitmap: Arc<Bitmap>) -> Result<()> {
        Ok(())
    }
}

impl OpenDalSinkWriter {
    pub fn new(
        operator: Operator,
        write_path: &str,
        rw_schema: Schema,
        pk_indices: Vec<usize>,
        is_append_only: bool,
    ) -> Result<Self> {
        let arrow_schema = convert_rw_schema_to_arrow_schema(rw_schema)?;
        Ok(Self {
            schema: Arc::new(arrow_schema),
            write_path: write_path.to_string(),
            pk_indices,
            operator,
            sink_writer: None,
            is_append_only,
        })
    }

    async fn create_sink_writer(&mut self) -> Result<()> {
        let object_store_writer = self
            .operator
            .writer_with(&self.write_path)
            .concurrent(8)
            .buffer(SINK_WRITE_BUFFER_SIZE)
            .await?;
        let parquet_config = ParquetWriterConfig::default();
        let mut props = WriterProperties::builder()
            .set_writer_version(WriterVersion::PARQUET_1_0)
            .set_bloom_filter_enabled(parquet_config.enable_bloom_filter)
            .set_compression(parquet_config.compression)
            .set_max_row_group_size(parquet_config.max_row_group_size)
            .set_write_batch_size(parquet_config.write_batch_size)
            .set_data_page_size_limit(parquet_config.data_page_size);
        if let Some(created_by) = parquet_config.created_by.as_ref() {
            props = props.set_created_by(created_by.to_string());
        }
        self.sink_writer = Some(AsyncArrowWriter::try_new(
            object_store_writer,
            self.schema.clone(),
            SINK_WRITE_BUFFER_SIZE,
            Some(props.build()),
        )?);
        Ok(())
    }

    async fn append_only(&mut self, chunk: StreamChunk) -> Result<()> {
        let (mut chunk, ops) = chunk.compact().into_parts();
        let filters =
            chunk.visibility() & ops.iter().map(|op| *op == Op::Insert).collect::<Bitmap>();
        chunk.set_visibility(filters);

        let batch = to_record_batch_with_schema(self.schema.clone(), &chunk.compact())?;

        self.sink_writer
            .as_mut()
            .ok_or_else(|| SinkError::Opendal("Sink writer is not created.".to_string()))?
            .write(&batch)
            .await?;

        Ok(())
    }
}

fn convert_rw_schema_to_arrow_schema(
    rw_schema: risingwave_common::catalog::Schema,
) -> anyhow::Result<arrow_schema::Schema> {
    let mut schema_fields = HashMap::new();
    rw_schema.fields.iter().for_each(|field| {
        let res = schema_fields.insert(&field.name, &field.data_type);
        // This assert is to make sure there is no duplicate field name in the schema.
        assert!(res.is_none())
    });
    let mut arrow_fileds = vec![];
    for rw_field in &rw_schema.fields {
        let converted_arrow_data_type =
            ArrowDataType::try_from(rw_field.data_type.clone()).map_err(|e| anyhow!(e))?;
        arrow_fileds.push(ArrowField::new(
            rw_field.name.clone(),
            converted_arrow_data_type,
            false,
        ));
    }

    Ok(arrow_schema::Schema::new(arrow_fileds))
}
