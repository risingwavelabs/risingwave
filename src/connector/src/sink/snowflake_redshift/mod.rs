// Copyright 2025 RisingWave Labs
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
use std::fmt::Write;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::BytesMut;
use opendal::Operator;
use risingwave_common::array::{ArrayImpl, DataChunk, Op, PrimitiveArray, StreamChunk, Utf8Array};
use risingwave_common::catalog::Schema;
use risingwave_common::row::Row;
use serde_json::{Map, Value};
use thiserror_ext::AsReport;

use crate::sink::encoder::{
    JsonEncoder, JsonbHandlingMode, RowEncoder, TimeHandlingMode, TimestampHandlingMode,
    TimestamptzHandlingMode,
};
use crate::sink::file_sink::opendal_sink::FileSink;
use crate::sink::file_sink::s3::{S3Common, S3Sink};
use crate::sink::{Result, SinkError, SinkWriterParam};

pub mod file_manager_util;
pub mod redshift;
pub mod snowflake;

pub const __ROW_ID: &str = "__row_id";
pub const __OP: &str = "__op";

pub struct AugmentedRow {
    row_encoder: JsonEncoder,
    current_epoch: u64,
    current_row_count: usize,
    is_append_only: bool,
}

impl AugmentedRow {
    pub fn new(current_epoch: u64, is_append_only: bool, schema: Schema) -> Self {
        let row_encoder = JsonEncoder::new(
            schema,
            None,
            crate::sink::encoder::DateHandlingMode::String,
            TimestampHandlingMode::String,
            TimestamptzHandlingMode::UtcString,
            TimeHandlingMode::String,
            JsonbHandlingMode::String,
        );
        Self {
            row_encoder,
            current_epoch,
            current_row_count: 0,
            is_append_only,
        }
    }

    pub fn reset_epoch(&mut self, current_epoch: u64) {
        if self.is_append_only || current_epoch == self.current_epoch {
            return;
        }
        self.current_epoch = current_epoch;
        self.current_row_count = 0;
    }

    pub fn augmented_row(&mut self, row: impl Row, op: Op) -> Result<Map<String, Value>> {
        let mut row = self.row_encoder.encode(row)?;
        if self.is_append_only {
            return Ok(row);
        }
        self.current_row_count += 1;
        row.insert(
            __ROW_ID.to_owned(),
            Value::String(format!("{}_{}", self.current_epoch, self.current_row_count)),
        );
        row.insert(
            __OP.to_owned(),
            Value::Number(serde_json::Number::from(op.to_i16())),
        );
        Ok(row)
    }
}

pub struct AugmentedChunk {
    current_epoch: u64,
    current_row_count: usize,
    is_append_only: bool,
}

impl AugmentedChunk {
    pub fn new(current_epoch: u64, is_append_only: bool) -> Self {
        Self {
            current_epoch,
            current_row_count: 0,
            is_append_only,
        }
    }

    pub fn reset_epoch(&mut self, current_epoch: u64) {
        if self.is_append_only || current_epoch == self.current_epoch {
            return;
        }
        self.current_epoch = current_epoch;
        self.current_row_count = 0;
    }

    pub fn augmented_chunk(&mut self, chunk: StreamChunk) -> Result<StreamChunk> {
        if self.is_append_only {
            return Ok(chunk);
        }
        let (data_chunk, ops) = chunk.into_parts();
        let chunk_row_count = data_chunk.capacity();
        let (columns, visibility) = data_chunk.into_parts();

        let op_column = ops.iter().map(|op| op.to_i16() as i32).collect::<Vec<_>>();
        let row_column_strings: Vec<String> = (0..chunk_row_count)
            .map(|i| format!("{}_{}", self.current_epoch, self.current_row_count + i))
            .collect();

        let row_column_refs: Vec<&str> = row_column_strings.iter().map(|s| s.as_str()).collect();
        self.current_row_count += chunk_row_count;

        let mut arrays: Vec<Arc<ArrayImpl>> = columns;
        arrays.push(Arc::new(ArrayImpl::Utf8(Utf8Array::from_iter(
            row_column_refs,
        ))));
        arrays.push(Arc::new(ArrayImpl::Int32(
            PrimitiveArray::<i32>::from_iter(op_column),
        )));

        let chunk = DataChunk::new(arrays, visibility);
        let ops = vec![Op::Insert; chunk_row_count];
        let chunk = StreamChunk::from_parts(ops, chunk);
        Ok(chunk)
    }
}

pub struct SnowflakeRedshiftSinkS3Writer {
    s3_config: S3Common,
    s3_operator: Operator,
    augmented_row: AugmentedRow,
    opendal_writer_path: Option<(opendal::Writer, String)>,
    executor_id: u64,
    target_table_name: Option<String>,
}

pub async fn build_opendal_writer_path(
    s3_config: &S3Common,
    executor_id: u64,
    operator: &Operator,
    target_table_name: &Option<String>,
) -> Result<(opendal::Writer, String)> {
    let mut base_path = s3_config.path.clone().unwrap_or("".to_owned());
    if !base_path.ends_with('/') {
        base_path.push('/');
    }
    if let Some(table_name) = &target_table_name {
        base_path.push_str(&format!("{}/", table_name));
    }
    let create_time = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards");
    let object_name = format!(
        "{}{}_{}.{}",
        base_path,
        executor_id,
        create_time.as_secs(),
        "json",
    );
    let all_path = format!("s3://{}/{}", s3_config.bucket_name, object_name);
    Ok((
        operator.writer_with(&object_name).concurrent(8).await?,
        all_path,
    ))
}

impl SnowflakeRedshiftSinkS3Writer {
    pub fn new(
        s3_config: S3Common,
        schema: Schema,
        is_append_only: bool,
        executor_id: u64,
        target_table_name: Option<String>,
    ) -> Result<Self> {
        let s3_operator = FileSink::<S3Sink>::new_s3_sink(&s3_config)?;
        Ok(Self {
            s3_config,
            s3_operator,
            opendal_writer_path: None,
            executor_id,
            augmented_row: AugmentedRow::new(0, is_append_only, schema),
            target_table_name,
        })
    }

    pub fn begin_epoch(&mut self, epoch: u64) -> Result<()> {
        self.augmented_row.reset_epoch(epoch);
        Ok(())
    }

    pub async fn write_batch(&mut self, chunk: StreamChunk) -> Result<()> {
        if self.opendal_writer_path.is_none() {
            let opendal_writer_path = build_opendal_writer_path(
                &self.s3_config,
                self.executor_id,
                &self.s3_operator,
                &self.target_table_name,
            )
            .await?;
            self.opendal_writer_path = Some(opendal_writer_path);
        }
        let mut chunk_buf = BytesMut::new();
        for (op, row) in chunk.rows() {
            let encoded_row = self.augmented_row.augmented_row(row, op)?;
            writeln!(chunk_buf, "{}", Value::Object(encoded_row)).unwrap(); // write to a `BytesMut` should never fail
        }
        self.opendal_writer_path
            .as_mut()
            .ok_or_else(|| SinkError::File("Sink writer is not created.".to_owned()))?
            .0
            .write(chunk_buf.freeze())
            .await?;
        Ok(())
    }

    pub async fn barrier(&mut self, is_checkpoint: bool) -> Result<Option<String>> {
        if is_checkpoint && let Some((mut writer, path)) = self.opendal_writer_path.take() {
            writer
                .close()
                .await
                .map_err(|e| SinkError::File(e.to_report_string()))?;
            Ok(Some(path))
        } else {
            Ok(None)
        }
    }
}
