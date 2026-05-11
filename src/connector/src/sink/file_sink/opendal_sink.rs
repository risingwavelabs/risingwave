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

use std::collections::{BTreeMap, HashMap};
use std::fmt::Write;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::anyhow;
use bytes::BytesMut;
use chrono::{DateTime, TimeZone, Utc};
use opendal::{FuturesAsyncWriter, Operator, Writer as OpendalWriter};
use parquet::arrow::AsyncArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use risingwave_common::array::arrow::IcebergArrowConvert;
use risingwave_common::array::arrow::arrow_schema_iceberg::{self, SchemaRef};
use risingwave_common::array::{Op, StreamChunk};
use risingwave_common::bitmap::BitmapBuilder;
use risingwave_common::catalog::Schema;
use risingwave_common::row::Row;
use risingwave_common::types::ToText;
use risingwave_pb::id::ExecutorId;
use serde::Deserialize;
use serde_json::Value;
use serde_with::{DisplayFromStr, serde_as};
use strum_macros::{Display, EnumString};
use tokio_util::compat::{Compat, FuturesAsyncWriteCompatExt};
use uuid::Uuid;
use with_options::WithOptions;

use crate::enforce_secret::EnforceSecret;
use crate::sink::catalog::SinkEncode;
use crate::sink::encoder::{
    JsonEncoder, JsonbHandlingMode, RowEncoder, TimeHandlingMode, TimestampHandlingMode,
    TimestamptzHandlingMode,
};
use crate::sink::file_sink::batching_log_sink::BatchingLogSinker;
use crate::sink::{Result, Sink, SinkError, SinkFormatDesc, SinkParam};
use crate::source::TryFromBTreeMap;
use crate::with_options::WithOptions;

pub const DEFAULT_ROLLOVER_SECONDS: usize = 10;
pub const DEFAULT_MAX_ROW_COUNR: usize = 10240;

pub fn default_rollover_seconds() -> usize {
    DEFAULT_ROLLOVER_SECONDS
}
pub fn default_max_row_count() -> usize {
    DEFAULT_MAX_ROW_COUNR
}

/// A parsed `path_partition_format` template.
///
/// The template is split into segments so that each row can be routed to the
/// correct Hive-style partition directory. Literal segments may contain chrono
/// `strftime` tokens (e.g. `year=%Y/month=%m/`). Column segments reference a
/// row column by name (e.g. `{symbol}`) and are substituted per-row.
#[derive(Debug, Clone)]
pub(crate) struct PartitionTemplate {
    segments: Vec<PartitionSegment>,
    has_column_refs: bool,
}

#[derive(Debug, Clone)]
enum PartitionSegment {
    /// Literal text that may contain chrono `strftime` tokens.
    Literal(String),
    /// Reference to a column by its (name, schema index). The name is kept
    /// alongside the index for diagnostics and error messages.
    Column {
        #[allow(dead_code)]
        name: String,
        index: usize,
    },
}

impl PartitionTemplate {
    /// Parse a `path_partition_format` template, resolving `{column}` placeholders
    /// against `rw_schema`.
    pub(crate) fn parse(fmt: &str, rw_schema: &Schema) -> Result<Self> {
        let mut segments = Vec::new();
        let mut has_column_refs = false;
        let mut buf = String::new();
        let mut chars = fmt.chars().peekable();
        while let Some(c) = chars.next() {
            match c {
                '{' => {
                    if chars.peek() == Some(&'{') {
                        // Escaped `{{` -> literal `{`.
                        chars.next();
                        buf.push('{');
                        continue;
                    }
                    if !buf.is_empty() {
                        segments.push(PartitionSegment::Literal(std::mem::take(&mut buf)));
                    }
                    let mut name = String::new();
                    let mut closed = false;
                    for nc in chars.by_ref() {
                        if nc == '}' {
                            closed = true;
                            break;
                        }
                        name.push(nc);
                    }
                    if !closed {
                        return Err(SinkError::Config(anyhow!(
                            "unterminated column placeholder in path_partition_format: {fmt}"
                        )));
                    }
                    let name = name.trim().to_owned();
                    if name.is_empty() {
                        return Err(SinkError::Config(anyhow!(
                            "empty column placeholder in path_partition_format: {fmt}"
                        )));
                    }
                    let index = rw_schema
                        .fields
                        .iter()
                        .position(|f| f.name == name)
                        .ok_or_else(|| {
                            SinkError::Config(anyhow!(
                                "path_partition_format references unknown column `{name}`; \
                                 available columns: [{}]",
                                rw_schema
                                    .fields
                                    .iter()
                                    .map(|f| f.name.as_str())
                                    .collect::<Vec<_>>()
                                    .join(", ")
                            ))
                        })?;
                    segments.push(PartitionSegment::Column { name, index });
                    has_column_refs = true;
                }
                '}' if chars.peek() == Some(&'}') => {
                    // Escaped `}}` -> literal `}`.
                    chars.next();
                    buf.push('}');
                }
                _ => buf.push(c),
            }
        }
        if !buf.is_empty() {
            segments.push(PartitionSegment::Literal(buf));
        }
        Ok(Self {
            segments,
            has_column_refs,
        })
    }

    pub(crate) fn has_column_refs(&self) -> bool {
        self.has_column_refs
    }

    pub(crate) fn render_with_row<R: Row>(&self, datetime: DateTime<Utc>, row: &R) -> String {
        let mut out = String::new();
        for segment in &self.segments {
            match segment {
                PartitionSegment::Literal(lit) => {
                    // chrono's `format` accepts arbitrary literals; only `%`-prefixed
                    // tokens are interpreted, so it is safe to feed both chrono tokens
                    // and Hive-style fragments through here.
                    let _ = write!(out, "{}", datetime.format(lit));
                }
                PartitionSegment::Column { index, .. } => match row.datum_at(*index) {
                    Some(scalar) => sanitize_partition_value(&scalar.to_text(), &mut out),
                    None => out.push_str("__HIVE_DEFAULT_PARTITION__"),
                },
            }
        }
        out
    }

    /// Group the visible rows of `chunk` by the partition path they render to.
    /// Returns `path -> row_indices` so the writer can build a per-partition
    /// sub-chunk for each entry. Uses a single `datetime` for every row; when
    /// per-row event time is required, the writer drives grouping itself.
    pub(crate) fn group_chunk_rows(
        &self,
        chunk: &StreamChunk,
        datetime: DateTime<Utc>,
    ) -> HashMap<String, Vec<usize>> {
        let data_chunk = chunk.data_chunk();
        let capacity = data_chunk.capacity();
        let visibility = data_chunk.visibility();
        let mut groups: HashMap<String, Vec<usize>> = HashMap::new();
        for idx in 0..capacity {
            if !visibility.is_set(idx) {
                continue;
            }
            let row = data_chunk.row_at_unchecked_vis(idx);
            let path = if self.has_column_refs {
                self.render_with_row(datetime, &row)
            } else {
                self.render_time_only(datetime)
            };
            groups.entry(path).or_default().push(idx);
        }
        groups
    }

    pub(crate) fn render_time_only(&self, datetime: DateTime<Utc>) -> String {
        debug_assert!(!self.has_column_refs);
        let mut out = String::new();
        for segment in &self.segments {
            if let PartitionSegment::Literal(lit) = segment {
                let _ = write!(out, "{}", datetime.format(lit));
            }
        }
        out
    }
}

/// Replace characters that are problematic in object paths (`/` and control bytes)
/// so that a column value with unexpected characters cannot escape its partition
/// directory. We follow Hive's convention of percent-encoding the offending bytes.
fn sanitize_partition_value(value: &str, out: &mut String) {
    for ch in value.chars() {
        match ch {
            '/' => out.push_str("%2F"),
            '\\' => out.push_str("%5C"),
            '\0' => out.push_str("%00"),
            c if (c as u32) < 0x20 => {
                let _ = write!(out, "%{:02X}", c as u32);
            }
            c => out.push(c),
        }
    }
}

/// Resolve the schema index referenced by `event_time_field`, validating that
/// the column has a temporal type we can convert into a chrono `DateTime<Utc>`.
fn resolve_event_time_field_index(
    strategy: &BatchingStrategy,
    rw_schema: &Schema,
) -> Result<Option<usize>> {
    let Some(name) = strategy.event_time_field.as_deref() else {
        return Ok(None);
    };
    let name = name.trim();
    if name.is_empty() {
        return Ok(None);
    }
    let (idx, field) = rw_schema
        .fields
        .iter()
        .enumerate()
        .find(|(_, f)| f.name == name)
        .ok_or_else(|| {
            SinkError::Config(anyhow!(
                "`event_time_field` references unknown column `{name}`; \
                 available columns: [{}]",
                rw_schema
                    .fields
                    .iter()
                    .map(|f| f.name.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            ))
        })?;
    match field.data_type {
        risingwave_common::types::DataType::Timestamp
        | risingwave_common::types::DataType::Timestamptz => Ok(Some(idx)),
        ref other => Err(SinkError::Config(anyhow!(
            "`event_time_field` column `{name}` must be TIMESTAMP or TIMESTAMPTZ, \
             got {other:?}"
        ))),
    }
}

/// Extract a UTC `DateTime` from a row's column value. Used by the partition
/// renderer when `event_time_field` is set. Naive timestamps (TIMESTAMP) are
/// interpreted as UTC; TIMESTAMPTZ values are converted directly.
fn row_event_datetime<R: Row>(row: &R, index: usize) -> Option<DateTime<Utc>> {
    let scalar = row.datum_at(index)?;
    match scalar {
        risingwave_common::types::ScalarRefImpl::Timestamp(ts) => {
            Some(DateTime::<Utc>::from_naive_utc_and_offset(ts.0, Utc))
        }
        risingwave_common::types::ScalarRefImpl::Timestamptz(ts) => Some(ts.to_datetime_utc()),
        _ => None,
    }
}

/// The `FileSink` struct represents a file sink that uses the `OpendalSinkBackend` trait for its backend implementation.
///
/// # Type Parameters
///
/// - S: The type parameter S represents the concrete implementation of the `OpendalSinkBackend` trait used by this file sink.
#[derive(Debug, Clone)]
pub struct FileSink<S: OpendalSinkBackend> {
    pub(crate) op: Operator,
    /// The path to the file where the sink writes data.
    pub(crate) path: String,
    /// The schema describing the structure of the data being written to the file sink.
    pub(crate) schema: Schema,
    pub(crate) is_append_only: bool,
    /// The batching strategy for sinking data to files.
    pub(crate) batching_strategy: BatchingStrategy,

    /// The description of the sink's format.
    pub(crate) format_desc: SinkFormatDesc,
    pub(crate) engine_type: EngineType,
    pub(crate) _marker: PhantomData<S>,
}

impl<S: OpendalSinkBackend> EnforceSecret for FileSink<S> {}

/// The `OpendalSinkBackend` trait unifies the behavior of various sink backends
/// implemented through `OpenDAL`(`<https://github.com/apache/opendal>`).
///
/// # Type Parameters
///
/// - Properties: Represents the necessary parameters for establishing a backend.
///
/// # Constants
///
/// - `SINK_NAME`: A static string representing the name of the sink.
///
/// # Functions
///
/// - `from_btreemap`: Automatically parse the required parameters from the input create sink statement.
/// - `new_operator`: Creates a new operator using the provided backend properties.
/// - `get_path`: Returns the path of the sink file specified by the user's create sink statement.
pub trait OpendalSinkBackend: Send + Sync + 'static + Clone + PartialEq {
    type Properties: TryFromBTreeMap + Send + Sync + Clone + WithOptions;
    const SINK_NAME: &'static str;

    fn from_btreemap(btree_map: BTreeMap<String, String>) -> Result<Self::Properties>;
    fn new_operator(properties: Self::Properties) -> Result<Operator>;
    fn get_path(properties: Self::Properties) -> String;
    fn get_engine_type() -> EngineType;
    fn get_batching_strategy(properties: Self::Properties) -> BatchingStrategy;
}

#[derive(Clone, Debug)]
pub enum EngineType {
    Gcs,
    S3,
    Fs,
    Azblob,
    Webhdfs,
    Snowflake,
}

impl<S: OpendalSinkBackend> Sink for FileSink<S> {
    type LogSinker = BatchingLogSinker;

    const SINK_NAME: &'static str = S::SINK_NAME;

    async fn validate(&self) -> Result<()> {
        if matches!(self.engine_type, EngineType::Snowflake) {
            risingwave_common::license::Feature::SnowflakeSink
                .check_available()
                .map_err(|e| anyhow::anyhow!(e))?;
        }
        if !self.is_append_only {
            return Err(SinkError::Config(anyhow!(
                "File sink only supports append-only mode at present. \
                    Please change the query to append-only, and specify it \
                    explicitly after the `FORMAT ... ENCODE ...` statement. \
                    For example, `FORMAT xxx ENCODE xxx(force_append_only='true')`"
            )));
        }

        if self.format_desc.encode != SinkEncode::Parquet
            && self.format_desc.encode != SinkEncode::Json
        {
            return Err(SinkError::Config(anyhow!(
                "File sink only supports `PARQUET` and `JSON` encode at present."
            )));
        }

        match self.op.list(&self.path).await {
            Ok(_) => Ok(()),
            Err(e) => Err(anyhow!(e).into()),
        }
    }

    async fn new_log_sinker(
        &self,
        writer_param: crate::sink::SinkWriterParam,
    ) -> Result<Self::LogSinker> {
        let writer = OpenDalSinkWriter::new(
            self.op.clone(),
            &self.path,
            self.schema.clone(),
            writer_param.executor_id,
            &self.format_desc,
            self.engine_type.clone(),
            self.batching_strategy.clone(),
        )?;
        Ok(BatchingLogSinker::new(writer))
    }
}

impl<S: OpendalSinkBackend> TryFrom<SinkParam> for FileSink<S> {
    type Error = SinkError;

    fn try_from(param: SinkParam) -> std::result::Result<Self, Self::Error> {
        let schema = param.schema();
        let config = S::from_btreemap(param.properties)?;
        let path = S::get_path(config.clone());
        let op = S::new_operator(config.clone())?;
        let batching_strategy = S::get_batching_strategy(config);
        let engine_type = S::get_engine_type();
        let format_desc = match param.format_desc {
            Some(desc) => desc,
            None => {
                if let EngineType::Snowflake = engine_type {
                    SinkFormatDesc::plain_json_for_snowflake_only()
                } else {
                    return Err(SinkError::Config(anyhow!("missing FORMAT ... ENCODE ...")));
                }
            }
        };
        Ok(Self {
            op,
            path,
            schema,
            is_append_only: param.sink_type.is_append_only(),
            batching_strategy,
            format_desc,
            engine_type,
            _marker: PhantomData,
        })
    }
}

pub struct OpenDalSinkWriter {
    schema: SchemaRef,
    /// The RisingWave-side schema. Held so future extensions (e.g. column
    /// projection for Hive partition-column elision) can look up types.
    #[allow(dead_code)]
    rw_schema: Schema,
    operator: Operator,
    /// Active writers keyed by their rendered partition path.
    /// Empty when no chunk has been buffered since the last commit.
    sink_writers: HashMap<String, FileWriterEnum>,
    write_path: String,
    executor_id: ExecutorId,
    unique_writer_id: Uuid,
    encode_type: SinkEncode,
    row_encoder: JsonEncoder,
    engine_type: EngineType,
    pub(crate) batching_strategy: BatchingStrategy,
    partition_template: Option<PartitionTemplate>,
    /// Schema index of the column named by `event_time_field`, if any. When
    /// `Some`, partition rendering reads the chrono timestamp from this column
    /// instead of using the writer's flush clock.
    event_time_field_index: Option<usize>,
    current_bached_row_num: usize,
    created_time: SystemTime,
    file_seq: u64,
}

/// The `FileWriterEnum` enum represents different types of file writers used for various sink
/// implementations.
///
/// # Variants
///
/// - `ParquetFileWriter`: Represents a Parquet file writer using the `AsyncArrowWriter<W>`
///   for writing data to a Parquet file. It accepts an implementation of W: `AsyncWrite` + `Unpin` + `Send`
///   as the underlying writer. In this case, the `OpendalWriter` serves as the underlying writer.
/// - `FileWriter`: Represents a basic `OpenDAL` writer, for writing files in encodes other than parquet.
///
/// The choice of writer used during the actual writing process depends on the encode type of the sink.
enum FileWriterEnum {
    ParquetFileWriter(AsyncArrowWriter<Compat<FuturesAsyncWriter>>),
    FileWriter(OpendalWriter),
}

/// Public interface exposed to `BatchingLogSinker`, used to write chunk and commit files.
impl OpenDalSinkWriter {
    /// This method writes a chunk.
    pub async fn write_batch(&mut self, chunk: StreamChunk) -> Result<()> {
        if self.sink_writers.is_empty() {
            assert_eq!(self.current_bached_row_num, 0);
            self.created_time = SystemTime::now();
        }
        self.append_only(chunk).await?;
        Ok(())
    }

    /// This method closes every active writer, finishing any pending files, and
    /// returns whether at least one file was committed.
    pub async fn commit(&mut self) -> Result<bool> {
        if self.sink_writers.is_empty() {
            return Ok(false);
        }
        let writers = std::mem::take(&mut self.sink_writers);
        for (path, writer) in writers {
            match writer {
                FileWriterEnum::ParquetFileWriter(w) => {
                    let bytes_written = w.bytes_written();
                    if bytes_written > 0 {
                        w.close().await?;
                        tracing::debug!(
                            "writer {} (executor_id: {}, created_time: {}) finish write file at partition `{}`, bytes_written: {}",
                            self.unique_writer_id,
                            self.executor_id,
                            self.created_time
                                .duration_since(UNIX_EPOCH)
                                .expect("Time went backwards")
                                .as_secs(),
                            path,
                            bytes_written
                        );
                    }
                }
                FileWriterEnum::FileWriter(mut w) => {
                    w.close().await?;
                }
            };
        }
        self.current_bached_row_num = 0;
        Ok(true)
    }

    /// Returns whether there is pending data (i.e., any active writer that has not been committed yet).
    pub fn has_pending_data(&self) -> bool {
        !self.sink_writers.is_empty()
    }

    // Try commit if the batching condition is met.
    pub async fn try_commit(&mut self) -> Result<bool> {
        if self.can_commit() {
            return self.commit().await;
        }
        Ok(false)
    }
}

/// Private methods related to batching.
impl OpenDalSinkWriter {
    /// Method for judging whether batch condition is met.
    fn can_commit(&self) -> bool {
        self.duration_seconds_since_writer_created() >= self.batching_strategy.rollover_seconds
            || self.current_bached_row_num >= self.batching_strategy.max_row_count
    }

    /// Bucket each visible row by its rendered partition path. Used when
    /// `event_time_field` is set, when the template references columns, or both.
    /// `fallback_datetime` is the writer's flush clock, used when no event-time
    /// column is configured or when the row's event-time value is NULL.
    fn group_chunk_per_row(
        &self,
        chunk: &StreamChunk,
        fallback_datetime: DateTime<Utc>,
    ) -> HashMap<String, Vec<usize>> {
        // No event-time column: every row uses the writer's flush clock, so we
        // can defer to the template's own grouping helper.
        if self.event_time_field_index.is_none()
            && let Some(template) = &self.partition_template
        {
            return template.group_chunk_rows(chunk, fallback_datetime);
        }

        let data_chunk = chunk.data_chunk();
        let capacity = data_chunk.capacity();
        let visibility = data_chunk.visibility();
        let mut groups: HashMap<String, Vec<usize>> = HashMap::new();
        for idx in 0..capacity {
            if !visibility.is_set(idx) {
                continue;
            }
            let row = data_chunk.row_at_unchecked_vis(idx);
            let datetime = match self.event_time_field_index {
                Some(col_idx) => row_event_datetime(&row, col_idx).unwrap_or(fallback_datetime),
                None => fallback_datetime,
            };
            let path = match &self.partition_template {
                Some(t) if t.has_column_refs() => t.render_with_row(datetime, &row),
                Some(t) => t.render_time_only(datetime),
                None => self.time_only_partition_path(datetime),
            };
            groups.entry(path).or_default().push(idx);
        }
        groups
    }

    /// Render the partition path purely from the writer's creation time. Used
    /// when no `{column}` placeholders are present (or for the legacy
    /// `path_partition_prefix` enum).
    fn time_only_partition_path(&self, datetime: DateTime<Utc>) -> String {
        if let Some(template) = &self.partition_template {
            debug_assert!(!template.has_column_refs());
            return template.render_time_only(datetime);
        }
        let prefix = self
            .batching_strategy
            .path_partition_prefix
            .as_ref()
            .unwrap_or(&PathPartitionPrefix::None);
        match prefix {
            PathPartitionPrefix::None => String::new(),
            PathPartitionPrefix::Day => datetime.format("%Y-%m-%d/").to_string(),
            PathPartitionPrefix::Month => datetime.format("/%Y-%m/").to_string(),
            PathPartitionPrefix::Hour => datetime.format("/%Y-%m-%d %H:00/").to_string(),
        }
    }

    fn duration_seconds_since_writer_created(&self) -> usize {
        let now = SystemTime::now();
        now.duration_since(self.created_time)
            .expect("Time went backwards")
            .as_secs() as usize
    }

    // Method for writing chunk and update related batching condition.
    async fn append_only(&mut self, chunk: StreamChunk) -> Result<()> {
        let create_time = self
            .created_time
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards");
        let datetime = Utc
            .timestamp_opt(create_time.as_secs() as i64, 0)
            .single()
            .expect("Failed to convert timestamp to DateTime<Utc>");

        // Validate ops once; the rest of the writer assumes append-only chunks.
        for &op in chunk.ops() {
            assert_eq!(op, Op::Insert, "expect all `op(s)` to be `Op::Insert`");
        }

        // Fast path: no per-row partitioning. Applies only when neither column
        // placeholders nor `event_time_field` are in use — i.e. every row in
        // the chunk renders to the same partition path.
        let needs_per_row = self.event_time_field_index.is_some()
            || self
                .partition_template
                .as_ref()
                .is_some_and(|t| t.has_column_refs());
        if !needs_per_row {
            let partition_path = self.time_only_partition_path(datetime);
            self.write_chunk_to_partition(&partition_path, chunk)
                .await?;
            return Ok(());
        }

        // Per-row partitioning: bucket visible rows by their rendered path,
        // then flush each bucket as a sub-chunk so we get one writer per partition.
        let capacity = chunk.data_chunk().capacity();
        let groups = self.group_chunk_per_row(&chunk, datetime);

        for (path, indices) in groups {
            let mut bits = BitmapBuilder::zeroed(capacity);
            for idx in indices {
                bits.set(idx, true);
            }
            let sub = chunk.clone().data_chunk().with_visibility(bits.finish());
            // `compact_vis` is what `IcebergArrowConvert.to_record_batch` expects
            // when only some rows are visible. It also keeps the JSON path simple
            // by collapsing the chunk to its visible rows.
            let compacted = sub.compact_vis();
            // Reconstruct a StreamChunk with all-`Insert` ops matching the
            // compacted cardinality. Ops were already validated as inserts.
            let nrows = compacted.cardinality();
            let columns: Vec<_> = compacted.columns().to_vec();
            let sub_chunk = StreamChunk::new(vec![Op::Insert; nrows].into_boxed_slice(), columns);
            self.write_chunk_to_partition(&path, sub_chunk).await?;
        }
        Ok(())
    }

    async fn write_chunk_to_partition(
        &mut self,
        partition_path: &str,
        chunk: StreamChunk,
    ) -> Result<()> {
        if !self.sink_writers.contains_key(partition_path) {
            let writer = self.create_partition_writer(partition_path).await?;
            self.sink_writers.insert(partition_path.to_owned(), writer);
        }
        let writer = self
            .sink_writers
            .get_mut(partition_path)
            .expect("just inserted");
        match writer {
            FileWriterEnum::ParquetFileWriter(w) => {
                let batch =
                    IcebergArrowConvert.to_record_batch(self.schema.clone(), chunk.data_chunk())?;
                let batch_row_nums = batch.num_rows();
                w.write(&batch).await?;
                self.current_bached_row_num += batch_row_nums;
            }
            FileWriterEnum::FileWriter(w) => {
                let mut chunk_buf = BytesMut::new();
                let batch_row_nums = chunk.data_chunk().capacity();
                for (op, row) in chunk.rows() {
                    assert_eq!(op, Op::Insert, "expect all `op(s)` to be `Op::Insert`");
                    writeln!(
                        chunk_buf,
                        "{}",
                        Value::Object(self.row_encoder.encode(row)?)
                    )
                    .unwrap();
                }
                w.write(chunk_buf.freeze()).await?;
                self.current_bached_row_num += batch_row_nums;
            }
        }
        Ok(())
    }
}

/// Init methods.
impl OpenDalSinkWriter {
    pub fn new(
        operator: Operator,
        write_path: &str,
        rw_schema: Schema,
        executor_id: ExecutorId,
        format_desc: &SinkFormatDesc,
        engine_type: EngineType,
        batching_strategy: BatchingStrategy,
    ) -> Result<Self> {
        let arrow_schema = convert_rw_schema_to_arrow_schema(rw_schema.clone())?;
        let jsonb_handling_mode = JsonbHandlingMode::from_options(&format_desc.options)?;
        let row_encoder = JsonEncoder::new(
            rw_schema.clone(),
            None,
            crate::sink::encoder::DateHandlingMode::String,
            TimestampHandlingMode::String,
            TimestamptzHandlingMode::UtcString,
            TimeHandlingMode::String,
            jsonb_handling_mode,
        );
        let partition_template = match batching_strategy.path_partition_format.as_deref() {
            Some(fmt) if !fmt.is_empty() => Some(PartitionTemplate::parse(fmt, &rw_schema)?),
            _ => None,
        };
        let event_time_field_index =
            resolve_event_time_field_index(&batching_strategy, &rw_schema)?;
        Ok(Self {
            schema: Arc::new(arrow_schema),
            rw_schema,
            write_path: write_path.to_owned(),
            operator,
            sink_writers: HashMap::new(),
            executor_id,
            unique_writer_id: Uuid::now_v7(),
            encode_type: format_desc.encode.clone(),
            row_encoder,
            engine_type,
            batching_strategy,
            partition_template,
            event_time_field_index,
            current_bached_row_num: 0,
            created_time: SystemTime::now(),
            file_seq: 0,
        })
    }

    async fn create_object_writer(&mut self, partition_path: &str) -> Result<OpendalWriter> {
        // Todo: specify more file suffixes based on encode_type.
        let suffix = match self.encode_type {
            SinkEncode::Parquet => "parquet",
            SinkEncode::Json => "json",
            _ => unimplemented!(),
        };

        let create_time = self
            .created_time
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards");

        // With batching in place, the file writing process is decoupled from checkpoints.
        // The current object naming convention is:
        // 1. The subdirectory is `partition_path`, derived from `path_partition_format`
        //    (which supports both chrono `strftime` tokens and `{column}` placeholders
        //    for Hive-style partitioning) or the legacy `path_partition_prefix` enum.
        // 2. The file name is `{uuid}_{unix_ts}_{seq}.{suffix}` so concurrent writers
        //    and partitions never collide.
        // If the engine type is `Fs`, the base path is empty so the host filesystem
        // handles the path. For the Snowflake Sink the `write_path` may be empty.
        let object_name = {
            let base_path = match self.engine_type {
                EngineType::Fs => String::new(),
                EngineType::Snowflake if self.write_path.is_empty() => String::new(),
                _ => format!("{}/", self.write_path),
            };
            let current_file_seq = self.file_seq;
            self.file_seq = self.file_seq.checked_add(1).expect("file seq overflow");

            format!(
                "{}{}{}_{}_{}.{}",
                base_path,
                partition_path,
                self.unique_writer_id,
                create_time.as_secs(),
                current_file_seq,
                suffix,
            )
        };
        Ok(self
            .operator
            .writer_with(&object_name)
            .concurrent(8)
            .await?)
    }

    async fn create_partition_writer(&mut self, partition_path: &str) -> Result<FileWriterEnum> {
        let object_writer = self.create_object_writer(partition_path).await?;
        let writer = match self.encode_type {
            SinkEncode::Parquet => {
                let props = WriterProperties::builder().set_compression(Compression::SNAPPY);
                let parquet_writer: tokio_util::compat::Compat<opendal::FuturesAsyncWriter> =
                    object_writer.into_futures_async_write().compat_write();
                FileWriterEnum::ParquetFileWriter(AsyncArrowWriter::try_new(
                    parquet_writer,
                    self.schema.clone(),
                    Some(props.build()),
                )?)
            }
            _ => FileWriterEnum::FileWriter(object_writer),
        };
        Ok(writer)
    }
}

fn convert_rw_schema_to_arrow_schema(
    rw_schema: risingwave_common::catalog::Schema,
) -> anyhow::Result<arrow_schema_iceberg::Schema> {
    let mut schema_fields = HashMap::new();
    rw_schema.fields.iter().for_each(|field| {
        let res = schema_fields.insert(&field.name, &field.data_type);
        // This assert is to make sure there is no duplicate field name in the schema.
        assert!(res.is_none())
    });
    let mut arrow_fields = vec![];
    for rw_field in &rw_schema.fields {
        let arrow_field = IcebergArrowConvert
            .to_arrow_field(&rw_field.name.clone(), &rw_field.data_type.clone())?;

        arrow_fields.push(arrow_field);
    }

    Ok(arrow_schema_iceberg::Schema::new(arrow_fields))
}

/// `BatchingStrategy` represents the strategy for batching data before writing to files.
///
/// This struct contains settings that control how data is collected and
/// partitioned based on specified criteria:
///
/// - `max_row_count`: Optional maximum number of rows to accumulate before writing.
/// - `rollover_seconds`: Optional time interval (in seconds) to trigger a write,
///   regardless of the number of accumulated rows.
/// - `path_partition_prefix`: Specifies how files are organized into directories
///   based on creation time (e.g., by day, month, or hour). Kept for backwards
///   compatibility; prefer `path_partition_format` for arbitrary Hive-style
///   partition layouts.
/// - `path_partition_format`: An optional chrono `strftime`-style template applied
///   to the file's creation time (UTC). When set, this overrides
///   `path_partition_prefix` and lets users emit arbitrary Hive-style partition
///   directories. Example:
///   `path_partition_format = 'year=%Y/month=%m/day=%d/hour=%H/'`
///   produces `year=2025/month=06/day=25/hour=13/`. The user is responsible for
///   including a trailing `/` when a directory is desired.
/// - `event_time_field`: Optional name of a column whose timestamp value is used
///   as the chrono input when rendering `path_partition_format`. When unset
///   (default), the writer's flush clock is used and all rows in a batch share
///   one partition path. When set, each row's timestamp determines its partition,
///   so late-arriving data lands in the correct historical directory. The column
///   must be of type `TIMESTAMP` or `TIMESTAMPTZ`.

#[serde_as]
#[derive(Default, Deserialize, Debug, Clone, WithOptions)]
pub struct BatchingStrategy {
    #[serde(default = "default_max_row_count")]
    #[serde_as(as = "DisplayFromStr")]
    pub max_row_count: usize,
    #[serde(default = "default_rollover_seconds")]
    #[serde_as(as = "DisplayFromStr")]
    pub rollover_seconds: usize,
    #[serde(default)]
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub path_partition_prefix: Option<PathPartitionPrefix>,
    #[serde(default)]
    pub path_partition_format: Option<String>,
    #[serde(default)]
    pub event_time_field: Option<String>,
}

/// `PathPartitionPrefix` defines the granularity of file partitions based on creation time.
///
/// Each variant specifies how files are organized into directories:
/// - `None`: No partitioning.
/// - `Day`: Files are written in a directory for each day.
/// - `Month`: Files are written in a directory for each month.
/// - `Hour`: Files are written in a directory for each hour.
#[derive(Default, Debug, Clone, PartialEq, Display, Deserialize, EnumString)]
#[strum(serialize_all = "snake_case")]
pub enum PathPartitionPrefix {
    #[default]
    None = 0,
    #[serde(alias = "day")]
    Day = 1,
    #[serde(alias = "month")]
    Month = 2,
    #[serde(alias = "hour")]
    Hour = 3,
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;
    use risingwave_common::catalog::Field;
    use risingwave_common::row::OwnedRow;
    use risingwave_common::types::{DataType, ScalarImpl};

    use super::*;

    fn schema() -> Schema {
        Schema {
            fields: vec![
                Field::with_name(DataType::Varchar, "exchange"),
                Field::with_name(DataType::Varchar, "symbol"),
                Field::with_name(DataType::Int64, "amount"),
            ],
        }
    }

    fn ts(year: i32, month: u32, day: u32, hour: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(year, month, day, hour, 0, 0).unwrap()
    }

    #[test]
    fn template_parses_time_only_format() {
        let template =
            PartitionTemplate::parse("year=%Y/month=%m/day=%d/hour=%H/", &schema()).unwrap();
        assert!(!template.has_column_refs());
        assert_eq!(
            template.render_time_only(ts(2021, 6, 25, 13)),
            "year=2021/month=06/day=25/hour=13/"
        );
    }

    #[test]
    fn template_parses_column_refs() {
        let template = PartitionTemplate::parse(
            "exchange={exchange}/symbol={symbol}/year=%Y/month=%m/day=%d/hour=%H/",
            &schema(),
        )
        .unwrap();
        assert!(template.has_column_refs());
        let row = OwnedRow::new(vec![
            Some(ScalarImpl::Utf8("binance-futures".into())),
            Some(ScalarImpl::Utf8("BTC-USDT".into())),
            Some(ScalarImpl::Int64(42)),
        ]);
        assert_eq!(
            template.render_with_row(ts(2021, 6, 25, 13), &row),
            "exchange=binance-futures/symbol=BTC-USDT/year=2021/month=06/day=25/hour=13/"
        );
    }

    #[test]
    fn template_rejects_unknown_column() {
        let err =
            PartitionTemplate::parse("exchange={exchange}/missing={does_not_exist}/", &schema())
                .unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("does_not_exist"), "{msg}");
    }

    #[test]
    fn template_rejects_unterminated_placeholder() {
        let err = PartitionTemplate::parse("symbol={symbol", &schema()).unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("unterminated"), "{msg}");
    }

    #[test]
    fn template_handles_escaped_braces_and_null_column() {
        let template = PartitionTemplate::parse("{{literal}}={symbol}/", &schema()).unwrap();
        let row = OwnedRow::new(vec![
            Some(ScalarImpl::Utf8("bnb".into())),
            None,
            Some(ScalarImpl::Int64(0)),
        ]);
        assert_eq!(
            template.render_with_row(ts(2021, 6, 25, 13), &row),
            "{literal}=__HIVE_DEFAULT_PARTITION__/"
        );
    }

    #[test]
    fn template_sanitizes_slashes_in_column_values() {
        let template = PartitionTemplate::parse("symbol={symbol}/", &schema()).unwrap();
        let row = OwnedRow::new(vec![
            Some(ScalarImpl::Utf8("ex".into())),
            // A bogus value containing `/` must not break out of the partition dir.
            Some(ScalarImpl::Utf8("a/b".into())),
            Some(ScalarImpl::Int64(0)),
        ]);
        assert_eq!(
            template.render_with_row(ts(2021, 6, 25, 13), &row),
            "symbol=a%2Fb/"
        );
    }

    fn build_test_chunk(rows: &[(&str, &str, i64)]) -> StreamChunk {
        let owned: Vec<(Op, OwnedRow)> = rows
            .iter()
            .map(|(exch, sym, amt)| {
                (
                    Op::Insert,
                    OwnedRow::new(vec![
                        Some(ScalarImpl::Utf8((*exch).into())),
                        Some(ScalarImpl::Utf8((*sym).into())),
                        Some(ScalarImpl::Int64(*amt)),
                    ]),
                )
            })
            .collect();
        let ref_rows: Vec<(Op, &OwnedRow)> = owned.iter().map(|(op, r)| (*op, r)).collect();
        let data_types = [DataType::Varchar, DataType::Varchar, DataType::Int64];
        StreamChunk::from_rows(&ref_rows, &data_types)
    }

    #[test]
    fn group_chunk_rows_partitions_by_column_value() {
        let template = PartitionTemplate::parse(
            "exchange={exchange}/symbol={symbol}/year=%Y/month=%m/day=%d/hour=%H/",
            &schema(),
        )
        .unwrap();
        let chunk = build_test_chunk(&[
            ("binance-futures", "BTC-USDT", 1),
            ("binance-futures", "ETH-USDT", 2),
            ("binance-futures", "BTC-USDT", 3),
            ("okx-perps", "BTC-USDT", 4),
        ]);
        let groups = template.group_chunk_rows(&chunk, ts(2021, 6, 25, 13));
        assert_eq!(groups.len(), 3);
        let btc_binance = groups
            .get("exchange=binance-futures/symbol=BTC-USDT/year=2021/month=06/day=25/hour=13/")
            .expect("binance BTC bucket exists");
        assert_eq!(btc_binance, &vec![0, 2]);
        assert_eq!(
            groups
                .get("exchange=binance-futures/symbol=ETH-USDT/year=2021/month=06/day=25/hour=13/")
                .map(|v| v.len()),
            Some(1)
        );
        assert_eq!(
            groups
                .get("exchange=okx-perps/symbol=BTC-USDT/year=2021/month=06/day=25/hour=13/")
                .map(|v| v.len()),
            Some(1)
        );
    }

    #[test]
    fn group_chunk_rows_returns_single_bucket_for_time_only() {
        let template =
            PartitionTemplate::parse("year=%Y/month=%m/day=%d/hour=%H/", &schema()).unwrap();
        let chunk = build_test_chunk(&[
            ("binance-futures", "BTC-USDT", 1),
            ("okx-perps", "ETH-USDT", 2),
        ]);
        let groups = template.group_chunk_rows(&chunk, ts(2021, 6, 25, 13));
        assert_eq!(groups.len(), 1);
        let only = groups
            .get("year=2021/month=06/day=25/hour=13/")
            .expect("single time partition");
        assert_eq!(only, &vec![0, 1]);
    }

    fn schema_with_event_time() -> Schema {
        Schema {
            fields: vec![
                Field::with_name(DataType::Varchar, "symbol"),
                Field::with_name(DataType::Timestamptz, "time"),
                Field::with_name(DataType::Int64, "amount"),
            ],
        }
    }

    fn default_strategy() -> BatchingStrategy {
        BatchingStrategy {
            max_row_count: 1,
            rollover_seconds: 1,
            path_partition_prefix: None,
            path_partition_format: None,
            event_time_field: None,
        }
    }

    #[test]
    fn event_time_field_resolves_to_schema_index() {
        let strategy = BatchingStrategy {
            event_time_field: Some("time".to_owned()),
            ..default_strategy()
        };
        let idx = resolve_event_time_field_index(&strategy, &schema_with_event_time())
            .expect("resolve")
            .expect("some");
        assert_eq!(idx, 1);
    }

    #[test]
    fn event_time_field_rejects_unknown_column() {
        let strategy = BatchingStrategy {
            event_time_field: Some("missing".to_owned()),
            ..default_strategy()
        };
        let err = resolve_event_time_field_index(&strategy, &schema_with_event_time()).unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("missing"), "{msg}");
    }

    #[test]
    fn event_time_field_rejects_non_temporal_column() {
        let strategy = BatchingStrategy {
            event_time_field: Some("symbol".to_owned()),
            ..default_strategy()
        };
        let err = resolve_event_time_field_index(&strategy, &schema_with_event_time()).unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("TIMESTAMP"), "{msg}");
    }

    #[test]
    fn event_time_field_unset_returns_none() {
        let strategy = default_strategy();
        let resolved =
            resolve_event_time_field_index(&strategy, &schema_with_event_time()).expect("resolve");
        assert!(resolved.is_none());
    }

    #[test]
    fn event_time_field_blank_string_returns_none() {
        let strategy = BatchingStrategy {
            event_time_field: Some("   ".to_owned()),
            ..default_strategy()
        };
        let resolved =
            resolve_event_time_field_index(&strategy, &schema_with_event_time()).expect("resolve");
        assert!(resolved.is_none());
    }

    #[test]
    fn row_event_datetime_reads_timestamptz_column() {
        use risingwave_common::types::Timestamptz;

        let dt = Utc.with_ymd_and_hms(2021, 6, 25, 13, 0, 0).unwrap();
        let tz = Timestamptz::from_micros(dt.timestamp_micros());
        let row = OwnedRow::new(vec![
            Some(ScalarImpl::Utf8("BTC-USDT".into())),
            Some(ScalarImpl::Timestamptz(tz)),
            Some(ScalarImpl::Int64(0)),
        ]);
        assert_eq!(row_event_datetime(&row, 1), Some(dt));
    }

    #[test]
    fn row_event_datetime_handles_null() {
        let row = OwnedRow::new(vec![
            Some(ScalarImpl::Utf8("BTC-USDT".into())),
            None,
            Some(ScalarImpl::Int64(0)),
        ]);
        assert_eq!(row_event_datetime(&row, 1), None);
    }

    /// End-to-end test that wires up the real `OpenDalSinkWriter` with a local
    /// filesystem opendal backend, pushes a chunk through it, and asserts that
    /// the column-based partition directories are materialised on disk.
    #[tokio::test]
    async fn opendal_writer_emits_hive_partitions_on_local_fs() {
        use opendal::services::Fs;

        use crate::sink::SinkFormatDesc;
        use crate::sink::catalog::{SinkEncode, SinkFormat};

        let tmp = tempfile::tempdir().expect("tempdir");
        let root = tmp.path().to_str().unwrap();
        let operator = Operator::new(Fs::default().root(root))
            .expect("build opendal Fs builder")
            .finish();

        let format_desc = SinkFormatDesc {
            format: SinkFormat::AppendOnly,
            encode: SinkEncode::Json,
            options: BTreeMap::new(),
            secret_refs: BTreeMap::new(),
            key_encode: None,
            connection_id: None,
        };
        let strategy = BatchingStrategy {
            max_row_count: 1024,
            rollover_seconds: 60,
            path_partition_format: Some(
                "exchange={exchange}/symbol={symbol}/year=%Y/month=%m/day=%d/hour=%H/".to_owned(),
            ),
            ..default_strategy()
        };
        let mut writer = OpenDalSinkWriter::new(
            operator,
            "metrics",
            schema(),
            ExecutorId::new(1),
            &format_desc,
            EngineType::Fs,
            strategy,
        )
        .expect("construct OpenDalSinkWriter");

        let chunk = build_test_chunk(&[
            ("binance-futures", "BTC-USDT", 1),
            ("binance-futures", "ETH-USDT", 2),
            ("binance-futures", "BTC-USDT", 3),
            ("okx-perps", "BTC-USDT", 4),
        ]);
        writer.write_batch(chunk).await.expect("write_batch");
        assert!(writer.commit().await.expect("commit"));

        // Walk the tempdir and verify the three expected partition directories.
        // The year/month/day/hour values come from the writer's wall clock at
        // flush time, so we validate the structural pattern rather than fixed
        // dates.
        let mut dirs: Vec<String> = Vec::new();
        for entry in walkdir::WalkDir::new(tmp.path())
            .into_iter()
            .filter_map(|e| e.ok())
        {
            if entry.file_type().is_file() {
                let rel = entry
                    .path()
                    .strip_prefix(tmp.path())
                    .unwrap()
                    .to_string_lossy()
                    .into_owned();
                dirs.push(rel);
            }
        }
        dirs.sort();
        assert_eq!(dirs.len(), 3, "one file per partition; got {dirs:?}");

        let partition_re = regex::Regex::new(
            r"^exchange=(binance-futures|okx-perps)/symbol=(BTC-USDT|ETH-USDT)/year=\d{4}/month=\d{2}/day=\d{2}/hour=\d{2}/[0-9a-f-]+_\d+_\d+\.json$",
        )
        .expect("regex compiles");
        for path in &dirs {
            assert!(
                partition_re.is_match(path),
                "path `{path}` does not match expected Hive layout"
            );
        }

        let expected_partitions = [
            ("binance-futures", "BTC-USDT"),
            ("binance-futures", "ETH-USDT"),
            ("okx-perps", "BTC-USDT"),
        ];
        for (exch, sym) in expected_partitions {
            let prefix = format!("exchange={exch}/symbol={sym}/");
            assert!(
                dirs.iter().any(|p| p.starts_with(&prefix)),
                "no file under partition `{prefix}` was written. got files: {dirs:?}"
            );
        }
    }

    /// End-to-end test for `event_time_field`: rows from three distinct event
    /// hours land in three distinct `year=…/month=…/day=…/hour=…/` directories
    /// even though the writer's flush clock fires only once.
    #[tokio::test]
    async fn opendal_writer_emits_event_time_partitions_on_local_fs() {
        use opendal::services::Fs;
        use risingwave_common::types::Timestamptz;

        use crate::sink::SinkFormatDesc;
        use crate::sink::catalog::{SinkEncode, SinkFormat};

        let tmp = tempfile::tempdir().expect("tempdir");
        let root = tmp.path().to_str().unwrap();
        let operator = Operator::new(Fs::default().root(root))
            .expect("build opendal Fs builder")
            .finish();

        let schema = Schema {
            fields: vec![
                Field::with_name(DataType::Varchar, "symbol"),
                Field::with_name(DataType::Timestamptz, "event_time"),
                Field::with_name(DataType::Int64, "amount"),
            ],
        };

        let format_desc = SinkFormatDesc {
            format: SinkFormat::AppendOnly,
            encode: SinkEncode::Json,
            options: BTreeMap::new(),
            secret_refs: BTreeMap::new(),
            key_encode: None,
            connection_id: None,
        };
        let strategy = BatchingStrategy {
            max_row_count: 1024,
            rollover_seconds: 60,
            path_partition_format: Some(
                "symbol={symbol}/year=%Y/month=%m/day=%d/hour=%H/".to_owned(),
            ),
            event_time_field: Some("event_time".to_owned()),
            ..default_strategy()
        };
        let mut writer = OpenDalSinkWriter::new(
            operator,
            "events",
            schema.clone(),
            ExecutorId::new(1),
            &format_desc,
            EngineType::Fs,
            strategy,
        )
        .expect("construct OpenDalSinkWriter");

        let make_row = |sym: &str, year: i32, month: u32, day: u32, hour: u32| {
            let dt = Utc.with_ymd_and_hms(year, month, day, hour, 0, 0).unwrap();
            (
                Op::Insert,
                OwnedRow::new(vec![
                    Some(ScalarImpl::Utf8(sym.into())),
                    Some(ScalarImpl::Timestamptz(Timestamptz::from_micros(
                        dt.timestamp_micros(),
                    ))),
                    Some(ScalarImpl::Int64(0)),
                ]),
            )
        };
        let owned = vec![
            make_row("BTC-USDT", 2021, 6, 25, 13),
            make_row("BTC-USDT", 2021, 6, 25, 14),
            make_row("ETH-USDT", 2021, 6, 25, 13),
        ];
        let ref_rows: Vec<(Op, &OwnedRow)> = owned.iter().map(|(op, r)| (*op, r)).collect();
        let data_types = [DataType::Varchar, DataType::Timestamptz, DataType::Int64];
        let chunk = StreamChunk::from_rows(&ref_rows, &data_types);

        writer.write_batch(chunk).await.expect("write_batch");
        assert!(writer.commit().await.expect("commit"));

        let mut dirs: Vec<String> = Vec::new();
        for entry in walkdir::WalkDir::new(tmp.path())
            .into_iter()
            .filter_map(|e| e.ok())
        {
            if entry.file_type().is_file() {
                dirs.push(
                    entry
                        .path()
                        .strip_prefix(tmp.path())
                        .unwrap()
                        .to_string_lossy()
                        .into_owned(),
                );
            }
        }
        dirs.sort();

        // Three distinct (symbol, hour) partitions → three output files.
        assert_eq!(dirs.len(), 3, "got files: {dirs:?}");

        for expected in [
            "symbol=BTC-USDT/year=2021/month=06/day=25/hour=13/",
            "symbol=BTC-USDT/year=2021/month=06/day=25/hour=14/",
            "symbol=ETH-USDT/year=2021/month=06/day=25/hour=13/",
        ] {
            assert!(
                dirs.iter().any(|p| p.starts_with(expected)),
                "no file under `{expected}` was written. got files: {dirs:?}"
            );
        }
    }
}
