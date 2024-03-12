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

use async_trait::async_trait;
use opendal::Writer;
use risingwave_common::array::{Op, StreamChunk};
use risingwave_common::buffer::Bitmap;
use risingwave_common::catalog::Schema;
use serde_json::Value;

use crate::sink::encoder::{JsonEncoder, RowEncoder};
use crate::sink::{Result, SinkWriter};

pub const GCS_SINK: &str = "gcs";

pub struct OpenDalSinkWriter {
    schema: Schema,
    writer: Writer,
    pk_indices: Vec<usize>,
    is_append_only: bool,
    row_encoder: JsonEncoder,
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
        self.writer.abort().await?;
        Ok(())
    }

    async fn barrier(&mut self, is_checkpoint: bool) -> Result<()> {
        if is_checkpoint {
            match self.writer.close().await {
                Ok(_) => (),
                Err(err) => {
                    self.writer.abort().await?;
                    return Err(err.into());
                }
            };
        }

        Ok(())
    }

    async fn update_vnode_bitmap(&mut self, _vnode_bitmap: Arc<Bitmap>) -> Result<()> {
        Ok(())
    }
}

impl OpenDalSinkWriter {
    pub fn new(
        writer: Writer,
        schema: Schema,
        pk_indices: Vec<usize>,
        is_append_only: bool,
    ) -> Result<Self> {
        let decimal_map = HashMap::default();
        Ok(Self {
            schema: schema.clone(),
            pk_indices,
            writer,
            is_append_only,
            row_encoder: JsonEncoder::new_with_s3(schema, None, decimal_map),
        })
    }

    async fn append_only(&mut self, chunk: StreamChunk) -> Result<()> {
        for (op, row) in chunk.rows() {
            if op != Op::Insert {
                continue;
            }
            let row_json_string = Value::Object(self.row_encoder.encode(row)?).to_string();
            self.writer.write(row_json_string).await?;
        }
        Ok(())
    }
}
