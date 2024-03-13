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

use std::sync::Arc;


use async_trait::async_trait;
use opendal::Writer as OpendalWriter;
use parquet::arrow::async_writer::AsyncArrowWriter;
use risingwave_common::array::{to_record_batch_with_schema, Op, StreamChunk};
use risingwave_common::buffer::Bitmap;
use risingwave_common::catalog::Schema;


use crate::sink::encoder::{RowEncoder};
use crate::sink::{Result, SinkWriter};

pub const GCS_SINK: &str = "gcs";

pub struct OpenDalSinkWriter {
    schema: Schema,
    writer: AsyncArrowWriter<OpendalWriter>,
    pk_indices: Vec<usize>,
    is_append_only: bool,
}

#[async_trait]
impl SinkWriter for OpenDalSinkWriter {
    async fn write_batch(&mut self, chunk: StreamChunk) -> Result<()> {
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
           todo!()
        }

        Ok(())
    }

    async fn update_vnode_bitmap(&mut self, _vnode_bitmap: Arc<Bitmap>) -> Result<()> {
        Ok(())
    }
}

impl OpenDalSinkWriter {
    pub fn new(
        writer: AsyncArrowWriter<OpendalWriter>,
        schema: Schema,
        pk_indices: Vec<usize>,
        is_append_only: bool,
    ) -> Result<Self> {
        Ok(Self {
            schema: schema.clone(),
            pk_indices,
            writer,
            is_append_only,
        })
    }

    async fn append_only(&mut self, chunk: StreamChunk) -> Result<()> {
        let (mut chunk, ops) = chunk.compact().into_parts();
        let filters =
            chunk.visibility() & ops.iter().map(|op| *op == Op::Insert).collect::<Bitmap>();
        chunk.set_visibility(filters);
        let arrow_schema = change_schema_to_arrow_schema(self.schema.clone());
        let batch = to_record_batch_with_schema(Arc::new(arrow_schema), &chunk.compact())?;
        self.writer.write(&batch).await?;

        Ok(())
    }
}

fn change_schema_to_arrow_schema(
    _schema: risingwave_common::catalog::Schema,
) -> arrow_schema::Schema {
    todo!()
}
