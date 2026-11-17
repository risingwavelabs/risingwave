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
use risingwave_common::catalog::{ColumnDesc, ColumnId, Schema};
use risingwave_common::hash::ActorId;
use risingwave_common::row::Row;
use risingwave_common::types::DataType;
use serde_json::{Map, Value};
use thiserror_ext::AsReport;
use uuid::Uuid;

use crate::sink::encoder::{
    JsonEncoder, JsonbHandlingMode, RowEncoder, TimeHandlingMode, TimestampHandlingMode,
    TimestamptzHandlingMode,
};
use crate::sink::file_sink::opendal_sink::FileSink;
use crate::sink::file_sink::s3::{S3Common, S3Sink};
use crate::sink::remote::CoordinatedRemoteSinkWriter;
use crate::sink::writer::SinkWriter;
use crate::sink::{Result, SinkError, SinkParam, SinkWriterMetrics, SinkWriterParam};

pub mod redshift;
pub mod snowflake;

pub const __ROW_ID: &str = "__row_id";
pub const __OP: &str = "__op";

/// Sort by epoch, writer-local sequence, then actor. Updates to the same key are
/// routed to the same actor within an epoch; the actor suffix disambiguates writers.
/// The widths cover u64/u64/u32 and match Redshift's legacy ID normalization.
fn cdc_row_id(epoch: u64, row_count: usize, actor_id: ActorId) -> String {
    format!("{epoch:020}_{row_count:020}_{:010}", actor_id.as_raw_id())
}

pub struct AugmentedRow {
    row_encoder: JsonEncoder,
    current_epoch: u64,
    current_row_count: usize,
    actor_id: ActorId,
    is_append_only: bool,
}

impl AugmentedRow {
    pub fn new(
        current_epoch: u64,
        is_append_only: bool,
        schema: Schema,
        actor_id: ActorId,
    ) -> Self {
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
            actor_id,
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
        let row_id = cdc_row_id(self.current_epoch, self.current_row_count, self.actor_id);
        row.insert(__ROW_ID.to_owned(), Value::String(row_id));
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
    actor_id: ActorId,
    is_append_only: bool,
}

impl AugmentedChunk {
    pub fn new(current_epoch: u64, is_append_only: bool, actor_id: ActorId) -> Self {
        Self {
            current_epoch,
            current_row_count: 0,
            actor_id,
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
            .map(|i| {
                cdc_row_id(
                    self.current_epoch,
                    self.current_row_count + i,
                    self.actor_id,
                )
            })
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
    target_table_name: String,
}

impl SnowflakeRedshiftSinkS3Writer {
    pub fn new(
        s3_config: S3Common,
        schema: Schema,
        is_append_only: bool,
        target_table_name: String,
        actor_id: ActorId,
    ) -> Result<Self> {
        let s3_operator = FileSink::<S3Sink>::new_s3_sink(&s3_config)?;
        Ok(Self {
            s3_config,
            s3_operator,
            opendal_writer_path: None,
            augmented_row: AugmentedRow::new(0, is_append_only, schema, actor_id),
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
                &self.s3_operator,
                None,
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

pub async fn build_opendal_writer_path(
    s3_config: &S3Common,
    operator: &Operator,
    dir: Option<&str>,
    target_table_name: &str,
) -> Result<(opendal::Writer, String)> {
    let mut base_path = s3_config.path.clone().unwrap_or("".to_owned());
    if !base_path.ends_with('/') {
        base_path.push('/');
    }
    base_path.push_str(&format!("{}/", target_table_name));
    if let Some(dir) = dir {
        base_path.push_str(&format!("{}/", dir));
    }
    let create_time = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards");
    let object_name = format!(
        "{}{}_{}.{}",
        base_path,
        Uuid::new_v4(),
        create_time.as_millis(),
        "json",
    );
    let all_path = format!("s3://{}/{}", s3_config.bucket_name, object_name);
    Ok((
        operator.writer_with(&object_name).concurrent(8).await?,
        all_path,
    ))
}

/// Generic JDBC writer for both Redshift and Snowflake sinks
pub struct SnowflakeRedshiftSinkJdbcWriter {
    augmented_row: AugmentedChunk,
    jdbc_sink_writer: CoordinatedRemoteSinkWriter,
}

impl SnowflakeRedshiftSinkJdbcWriter {
    pub async fn new(
        is_append_only: bool,
        writer_param: SinkWriterParam,
        mut param: SinkParam,
        full_table_name: String,
    ) -> Result<Self> {
        let metrics = SinkWriterMetrics::new(&writer_param);
        let column_descs = &mut param.columns;

        // Build full table name based on connector type
        if !is_append_only {
            // Add CDC-specific columns for upsert mode
            let max_column_id = column_descs
                .iter()
                .map(|column| column.column_id.get_id())
                .max()
                .unwrap_or(0);

            (*column_descs).push(ColumnDesc::named(
                __ROW_ID,
                ColumnId::new(max_column_id + 1),
                DataType::Varchar,
            ));
            (*column_descs).push(ColumnDesc::named(
                __OP,
                ColumnId::new(max_column_id + 2),
                DataType::Int32,
            ));
        };

        if let Some(schema_name) = param.properties.remove("schema") {
            param
                .properties
                .insert("schema.name".to_owned(), schema_name);
        }
        if let Some(database_name) = param.properties.remove("database") {
            param
                .properties
                .insert("database.name".to_owned(), database_name);
        }
        param
            .properties
            .insert("table.name".to_owned(), full_table_name.clone());
        param
            .properties
            .insert("type".to_owned(), "append-only".to_owned());

        let jdbc_sink_writer =
            CoordinatedRemoteSinkWriter::new(param.clone(), metrics.clone()).await?;

        Ok(Self {
            augmented_row: AugmentedChunk::new(0, is_append_only, writer_param.actor_id),
            jdbc_sink_writer,
        })
    }

    pub async fn begin_epoch(&mut self, epoch: u64) -> Result<()> {
        self.augmented_row.reset_epoch(epoch);
        self.jdbc_sink_writer.begin_epoch(epoch).await?;
        Ok(())
    }

    pub async fn write_batch(&mut self, chunk: StreamChunk) -> Result<()> {
        let chunk = self.augmented_row.augmented_chunk(chunk)?;
        self.jdbc_sink_writer.write_batch(chunk).await?;
        Ok(())
    }

    pub async fn barrier(&mut self, is_checkpoint: bool) -> Result<()> {
        self.jdbc_sink_writer.barrier(is_checkpoint).await?;
        Ok(())
    }

    pub async fn abort(&mut self) -> Result<()> {
        // TODO: abort should clean up all the data written in this epoch
        self.jdbc_sink_writer.abort().await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::array::stream_chunk::StreamChunkTestExt;
    use risingwave_common::catalog::Field;
    use risingwave_common::types::ScalarRefImpl;

    use super::*;

    fn schema() -> Schema {
        Schema::new(vec![Field::with_name(DataType::Int64, "id")])
    }

    fn chunk_row_ids(chunk: &StreamChunk) -> Vec<String> {
        chunk
            .rows()
            .map(|(op, row)| {
                assert_eq!(op, Op::Insert);
                let Some(ScalarRefImpl::Utf8(id)) = row.datum_at(1) else {
                    panic!("missing CDC row ID");
                };
                id.to_owned()
            })
            .collect()
    }

    #[test]
    fn test_cdc_row_id_ordering() {
        let tuples = [
            (99, usize::MAX, u32::MAX),
            (100, 9, u32::MAX),
            (100, 10, 9),
            (100, 10, 10),
            (u64::MAX, usize::MAX, u32::MAX),
        ];
        let ids: Vec<_> = tuples
            .into_iter()
            .map(|(epoch, count, actor)| cdc_row_id(epoch, count, actor.into()))
            .collect();
        assert!(ids.windows(2).all(|pair| pair[0] < pair[1]));
        assert!(ids.iter().all(|id| id.len() == 52));
    }

    #[test]
    fn test_augmented_row_ids() {
        let chunk = StreamChunk::from_pretty("I\n + 1");
        let (_, row) = chunk.rows().next().unwrap();
        let mut writer = AugmentedRow::new(99, false, schema(), 9.into());
        let mut other_writer = AugmentedRow::new(99, false, schema(), 10.into());
        let mut ids = Vec::new();
        for _ in 0..12 {
            let encoded = writer.augmented_row(row, Op::UpdateInsert).unwrap();
            assert_eq!(encoded[__OP], Op::UpdateInsert.to_i16());
            ids.push(encoded[__ROW_ID].as_str().unwrap().to_owned());
            // Re-entering the same epoch must not reuse IDs.
            writer.reset_epoch(99);
        }
        assert!(ids.windows(2).all(|pair| pair[0] < pair[1]));
        let other = other_writer.augmented_row(row, Op::Insert).unwrap();
        assert_ne!(ids[0], other[__ROW_ID].as_str().unwrap());
        writer.reset_epoch(100);
        let next = writer.augmented_row(row, Op::Delete).unwrap();
        assert!(ids.last().unwrap().as_str() < next[__ROW_ID].as_str().unwrap());
        assert_eq!(writer.current_row_count, 1);
    }

    #[test]
    fn test_augmented_chunk_ids() {
        let chunk = StreamChunk::from_pretty("I\n + 1\n - 2 D\n U- 3\n U+ 4");
        let mut writer = AugmentedChunk::new(99, false, 9.into());
        let mut other_writer = AugmentedChunk::new(99, false, 10.into());
        let mut ids = Vec::new();
        for _ in 0..4 {
            let augmented = writer.augmented_chunk(chunk.clone()).unwrap();
            assert_eq!(augmented.visibility(), chunk.visibility());
            for ((original_op, original), (_, augmented)) in chunk.rows().zip(augmented.rows()) {
                assert_eq!(original.datum_at(0), augmented.datum_at(0));
                assert_eq!(
                    augmented.datum_at(2),
                    Some(ScalarRefImpl::Int32(original_op.to_i16() as i32))
                );
            }
            ids.extend(chunk_row_ids(&augmented));
            writer.reset_epoch(99);
        }
        assert!(ids.windows(2).all(|pair| pair[0] < pair[1]));
        let other = chunk_row_ids(&other_writer.augmented_chunk(chunk.clone()).unwrap());
        assert_ne!(ids[0], other[0]);
        writer.reset_epoch(100);
        let next = chunk_row_ids(&writer.augmented_chunk(chunk.clone()).unwrap());
        assert!(ids.last().unwrap() < &next[0]);
        assert_eq!(writer.current_row_count, chunk.capacity());
    }

    #[test]
    fn test_append_only_is_not_augmented() {
        let chunk = StreamChunk::from_pretty("I\n + 1\n + 2");
        let mut writer = AugmentedChunk::new(100, true, 1.into());
        assert_eq!(writer.augmented_chunk(chunk.clone()).unwrap(), chunk);
        let mut writer = AugmentedRow::new(100, true, schema(), 1.into());
        for (op, row) in chunk.rows() {
            let encoded = writer.augmented_row(row, op).unwrap();
            assert_eq!(encoded.len(), 1);
            assert!(encoded.contains_key("id"));
        }
    }
}
