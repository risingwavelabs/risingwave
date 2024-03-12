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
use async_trait::async_trait;
use bytes::Bytes;
use itertools::Itertools;
use opendal::{Metakey, Operator};
use risingwave_common::array::{Op, StreamChunk};
use risingwave_common::buffer::Bitmap;
use risingwave_common::catalog::Schema;
use risingwave_common::types::DataType;
use serde::Deserialize;
use serde_derive::Serialize;
use serde_json::Value;
use serde_with::serde_as;
use thiserror_ext::AsReport;
use with_options::WithOptions;

use crate::error::ConnectorError;
use crate::sink::encoder::{JsonEncoder, RowEncoder, TimestampHandlingMode};
use crate::sink::writer::{LogSinkerOf, SinkWriterExt};
use crate::sink::{
    DummySinkCommitCoordinator, Result, Sink, SinkError, SinkParam, SinkWriter, SinkWriterParam,
    SINK_TYPE_APPEND_ONLY, SINK_TYPE_OPTION, SINK_TYPE_UPSERT,
};

pub const GCS_SINK: &str = "gcs";


pub struct OpenDalSinkWriter {
    schema: Schema,
    op: Operator,
    pk_indices: Vec<usize>,
    is_append_only: bool,
    row_encoder: JsonEncoder,
    path: String,
}

#[async_trait]
impl SinkWriter for OpenDalSinkWriter {
    async fn write_batch(&mut self, chunk: StreamChunk) -> Result<()> {
        let path = &self.path.clone();
        if self.is_append_only {
            self.append_only(chunk, path).await
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

    async fn barrier(&mut self, _is_checkpoint: bool) -> Result<()> {
        todo!()
    }

    async fn update_vnode_bitmap(&mut self, _vnode_bitmap: Arc<Bitmap>) -> Result<()> {
        Ok(())
    }
}

impl OpenDalSinkWriter {
    pub async fn new(
        op: Operator,
        schema: Schema,
        pk_indices: Vec<usize>,
        is_append_only: bool,
        path: &str,
    ) -> Result<Self> {
        let mut decimal_map = HashMap::default();
        Ok(Self {
            schema: schema.clone(),
            pk_indices,
            op,
            is_append_only,
            row_encoder: JsonEncoder::new_with_s3(schema, None, decimal_map),
            path: path.to_string(),
        })
    }

    async fn append_only(&mut self, chunk: StreamChunk, path: &str) -> Result<()> {
        for (op, row) in chunk.rows() {
            if op != Op::Insert {
                continue;
            }
            let row_json_string = Value::Object(self.row_encoder.encode(row)?).to_string();
            self.op
                .write(path, row_json_string)
                .await
                .map_err(|e| SinkError::Connector(e.into()))?;
        }
        Ok(())
    }
}
