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

use std::sync::LazyLock;

use risingwave_common::array::stream_record::RecordType;
use risingwave_common::array::{ArrayBuilderImpl, Op, StreamChunk};
use risingwave_common::bitmap::BitmapBuilder;
use risingwave_common::log::LogSuppresser;
use risingwave_common::types::{Datum, DatumCow, ScalarRefImpl};
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_connector_codec::decoder::{AccessError, AccessResult};
use risingwave_pb::plan_common::additional_column::ColumnType as AdditionalColumnType;
use smallvec::SmallVec;
use thiserror_ext::AsReport;

use super::MessageMeta;
use crate::parser::utils::{
    extract_cdc_meta_column, extract_header_inner_from_meta, extract_headers_from_meta,
    extract_pulsar_message_id_data_from_meta, extract_subject_from_meta,
    extract_timestamp_from_meta,
};
use crate::source::{SourceColumnDesc, SourceColumnType, SourceCtrlOpts, SourceMeta};

/// Maximum number of rows in a transaction. If a transaction is larger than this, it will be force
/// committed to avoid potential OOM.
const MAX_TRANSACTION_SIZE: usize = 4096;

/// Represents an ongoing transaction.
struct Transaction {
    id: Box<str>,
    len: usize,
}

/// A builder for building a [`StreamChunk`] from [`SourceColumnDesc`].
///
/// Output chunk size is controlled by `source_ctrl_opts.chunk_size` and `source_ctrl_opts.split_txn`.
/// During building process, it's possible that multiple chunks are built even without any explicit
/// call to `finish_current_chunk`. This mainly happens when we find more than one records in one
/// `SourceMessage` when parsing it. User of this builder should call `consume_ready_chunks` to consume
/// the built chunks from time to time, to avoid the buffer from growing too large.
pub struct SourceStreamChunkBuilder {
    column_descs: Vec<SourceColumnDesc>,
    source_ctrl_opts: SourceCtrlOpts,
    builders: Vec<ArrayBuilderImpl>,
    op_builder: Vec<Op>,
    vis_builder: BitmapBuilder,
    ongoing_txn: Option<Transaction>,
    ready_chunks: SmallVec<[StreamChunk; 1]>,
}

impl SourceStreamChunkBuilder {
    pub fn new(column_descs: Vec<SourceColumnDesc>, source_ctrl_opts: SourceCtrlOpts) -> Self {
        let (builders, op_builder, vis_builder) =
            Self::create_builders(&column_descs, source_ctrl_opts.chunk_size);

        Self {
            column_descs,
            source_ctrl_opts,
            builders,
            op_builder,
            vis_builder,
            ongoing_txn: None,
            ready_chunks: SmallVec::new(),
        }
    }

    fn create_builders(
        column_descs: &[SourceColumnDesc],
        chunk_size: usize,
    ) -> (Vec<ArrayBuilderImpl>, Vec<Op>, BitmapBuilder) {
        let reserved_capacity = chunk_size + 1; // it's possible to have an additional `U-` at the end
        let builders = column_descs
            .iter()
            .map(|desc| desc.data_type.create_array_builder(reserved_capacity))
            .collect();
        let op_builder = Vec::with_capacity(reserved_capacity);
        let vis_builder = BitmapBuilder::with_capacity(reserved_capacity);
        (builders, op_builder, vis_builder)
    }

    /// Begin a (CDC) transaction with the given `txn_id`.
    pub fn begin_transaction(&mut self, txn_id: Box<str>) {
        if let Some(ref txn) = self.ongoing_txn {
            tracing::warn!(
                ongoing_txn_id = txn.id,
                new_txn_id = txn_id,
                "already in a transaction"
            );
        }
        tracing::debug!(txn_id, "begin upstream transaction");
        self.ongoing_txn = Some(Transaction { id: txn_id, len: 0 });
    }

    /// Commit the ongoing transaction with the given `txn_id`.
    pub fn commit_transaction(&mut self, txn_id: Box<str>) {
        if let Some(txn) = self.ongoing_txn.take() {
            if txn.id != txn_id {
                tracing::warn!(
                    expected_txn_id = txn.id,
                    actual_txn_id = txn_id,
                    "unexpected transaction id"
                );
            }
            tracing::debug!(txn_id, "commit upstream transaction");

            if self.current_chunk_len() >= self.source_ctrl_opts.chunk_size {
                // if `split_txn` is on, we should've finished the chunk already
                assert!(!self.source_ctrl_opts.split_txn);
                self.finish_current_chunk();
            }
        } else {
            tracing::warn!(txn_id, "no ongoing transaction to commit");
        }
    }

    /// Check if the builder is in an ongoing transaction.
    pub fn is_in_transaction(&self) -> bool {
        self.ongoing_txn.is_some()
    }

    /// Get a row writer for parser to write records to the builder.
    pub fn row_writer(&mut self) -> SourceStreamChunkRowWriter<'_> {
        SourceStreamChunkRowWriter {
            builder: self,
            visible: true, // write visible rows by default
            row_meta: None,
        }
    }

    /// Write a heartbeat record to the builder. The builder will decide whether to finish the
    /// current chunk or not. Currently it ensures that heartbeats are always in separate chunks.
    pub fn heartbeat(&mut self, meta: MessageMeta<'_>) {
        if self.current_chunk_len() > 0 {
            // If there are records in the chunk, finish it first.
            // If there's an ongoing transaction, `finish_current_chunk` will handle it properly.
            // Note this
            self.finish_current_chunk();
        }

        _ = self
            .row_writer()
            .invisible()
            .with_meta(meta)
            .do_insert(|_| Ok(Datum::None));
        self.finish_current_chunk(); // each heartbeat should be a separate chunk
    }

    /// Finish and build a [`StreamChunk`] from the current pending records in the builder,
    /// no matter whether the builder is in a transaction or not, `split_txn` or not. The
    /// built chunk will be appended to the `ready_chunks` and the builder will be reset.
    pub fn finish_current_chunk(&mut self) {
        if self.op_builder.is_empty() {
            return;
        }

        let (builders, op_builder, vis_builder) =
            Self::create_builders(&self.column_descs, self.source_ctrl_opts.chunk_size);
        let chunk = StreamChunk::with_visibility(
            std::mem::replace(&mut self.op_builder, op_builder),
            std::mem::replace(&mut self.builders, builders)
                .into_iter()
                .map(|builder| builder.finish().into())
                .collect(),
            std::mem::replace(&mut self.vis_builder, vis_builder).finish(),
        );
        self.ready_chunks.push(chunk);

        if let Some(ref mut txn) = self.ongoing_txn {
            tracing::warn!(
                txn_id = txn.id,
                len = txn.len,
                "splitting an ongoing transaction"
            );
            txn.len = 0;
        }
    }

    /// Consumes and returns the ready [`StreamChunk`]s.
    pub fn consume_ready_chunks(&mut self) -> impl ExactSizeIterator<Item = StreamChunk> + '_ {
        self.ready_chunks.drain(..)
    }

    fn current_chunk_len(&self) -> usize {
        self.op_builder.len()
    }

    /// Commit a newly-written record by appending `op` and `vis` to the corresponding builders.
    /// This is supposed to be called via the `row_writer` only.
    fn commit_record(&mut self, op: Op, vis: bool) {
        self.op_builder.push(op);
        self.vis_builder.append(vis);

        let curr_chunk_size = self.current_chunk_len();
        let max_chunk_size = self.source_ctrl_opts.chunk_size;

        if let Some(ref mut txn) = self.ongoing_txn {
            txn.len += 1;

            if txn.len >= MAX_TRANSACTION_SIZE
                || (self.source_ctrl_opts.split_txn && curr_chunk_size >= max_chunk_size)
            {
                self.finish_current_chunk();
            }
        } else if curr_chunk_size >= max_chunk_size {
            self.finish_current_chunk();
        }
    }
}

/// `SourceStreamChunkRowWriter` is responsible to write one or more records to the [`StreamChunk`],
/// where each contains either one row (Insert/Delete) or two rows (Update) that can be written atomically.
///
/// Callers are supposed to call one of the `insert`, `delete` or `update` methods to write a record,
/// providing a closure that produces one or two [`Datum`]s by corresponding [`SourceColumnDesc`].
/// Specifically,
/// - only columns with [`SourceColumnType::Normal`] need to be handled;
/// - errors for non-primary key columns will be ignored and filled with default value instead;
/// - other errors will be propagated.
pub struct SourceStreamChunkRowWriter<'a> {
    builder: &'a mut SourceStreamChunkBuilder,

    /// Whether the rows written by this writer should be visible in output `StreamChunk`.
    visible: bool,

    /// An optional meta data of the original message.
    ///
    /// When this is set by `with_meta`, it'll be used to fill the columns of types other than [`SourceColumnType::Normal`].
    row_meta: Option<MessageMeta<'a>>,
}

impl<'a> SourceStreamChunkRowWriter<'a> {
    /// Set the meta data of the original message for this row writer.
    ///
    /// This should always be called except for tests.
    pub fn with_meta(mut self, row_meta: MessageMeta<'a>) -> Self {
        self.row_meta = Some(row_meta);
        self
    }

    pub fn row_meta(&self) -> Option<MessageMeta<'a>> {
        self.row_meta
    }

    pub fn source_meta(&self) -> &'a SourceMeta {
        self.row_meta
            .map(|m| m.source_meta)
            .unwrap_or(&SourceMeta::Empty)
    }

    /// Convert the row writer to invisible row writer.
    pub fn invisible(mut self) -> Self {
        self.visible = false;
        self
    }
}

impl SourceStreamChunkRowWriter<'_> {
    fn do_action<'a, A: RowWriterAction>(
        &'a mut self,
        mut f: impl FnMut(&SourceColumnDesc) -> AccessResult<A::Output<'a>>,
    ) -> AccessResult<()> {
        let mut parse_field = |desc: &SourceColumnDesc| {
            match f(desc) {
                Ok(output) => Ok(output),

                // Throw error for failed access to primary key columns.
                Err(e) if desc.is_pk => Err(e),
                // Ignore error for other columns and fill in `NULL` instead.
                Err(error) => {
                    // TODO: figure out a way to fill in not-null default value if user specifies one
                    // TODO: decide whether the error should not be ignored (e.g., even not a valid Debezium message)
                    // TODO: not using tracing span to provide `split_id` and `offset` due to performance concern,
                    //       see #13105
                    static LOG_SUPPERSSER: LazyLock<LogSuppresser> =
                        LazyLock::new(LogSuppresser::default);
                    if let Ok(suppressed_count) = LOG_SUPPERSSER.check() {
                        tracing::warn!(
                            error = %error.as_report(),
                            split_id = self.row_meta.as_ref().map(|m| m.split_id),
                            offset = self.row_meta.as_ref().map(|m| m.offset),
                            column = desc.name,
                            suppressed_count,
                            "failed to parse non-pk column, padding with `NULL`"
                        );
                    }
                    Ok(A::output_for(Datum::None))
                }
            }
        };

        let mut wrapped_f = |desc: &SourceColumnDesc| {
            match (&desc.column_type, &desc.additional_column.column_type) {
                (&SourceColumnType::Offset | &SourceColumnType::RowId, _) => {
                    // SourceColumnType is for CDC source only.
                    Ok(A::output_for(
                        self.row_meta
                            .as_ref()
                            .and_then(|row_meta| row_meta.value_for_column(desc)),
                    ))
                }
                (&SourceColumnType::Meta, _)
                    if matches!(
                        &self.row_meta.map(|ele| ele.source_meta),
                        &Some(SourceMeta::Kafka(_) | SourceMeta::DebeziumCdc(_))
                    ) =>
                {
                    // SourceColumnType is for CDC source only.
                    Ok(A::output_for(
                        self.row_meta
                            .as_ref()
                            .and_then(|row_meta| row_meta.value_for_column(desc)),
                    ))
                }

                (
                    _, // for cdc tables
                    &Some(ref col @ AdditionalColumnType::DatabaseName(_))
                    | &Some(ref col @ AdditionalColumnType::TableName(_)),
                ) => {
                    match self.row_meta {
                        Some(row_meta) => {
                            if let SourceMeta::DebeziumCdc(cdc_meta) = row_meta.source_meta {
                                Ok(A::output_for(extract_cdc_meta_column(
                                    cdc_meta,
                                    col,
                                    desc.name.as_str(),
                                )?))
                            } else {
                                Err(AccessError::Uncategorized {
                                    message: "CDC metadata not found in the message".to_owned(),
                                })
                            }
                        }
                        None => parse_field(desc), // parse from payload
                    }
                }
                (_, &Some(AdditionalColumnType::Timestamp(_))) => match self.row_meta {
                    Some(row_meta) => Ok(A::output_for(extract_timestamp_from_meta(
                        row_meta.source_meta,
                    ))),
                    None => parse_field(desc), // parse from payload
                },
                (_, &Some(AdditionalColumnType::CollectionName(_))) => {
                    // collection name for `mongodb-cdc` should be parsed from the message payload
                    parse_field(desc)
                }
                (_, &Some(AdditionalColumnType::Subject(_))) => Ok(A::output_for(
                    self.row_meta
                        .as_ref()
                        .and_then(|ele| extract_subject_from_meta(ele.source_meta))
                        .unwrap_or(None),
                )),
                (_, &Some(AdditionalColumnType::Partition(_))) => {
                    // the meta info does not involve spec connector
                    Ok(A::output_for(
                        self.row_meta
                            .as_ref()
                            .map(|ele| ScalarRefImpl::Utf8(ele.split_id)),
                    ))
                }
                (_, &Some(AdditionalColumnType::Offset(_))) => {
                    // the meta info does not involve spec connector
                    Ok(A::output_for(
                        self.row_meta
                            .as_ref()
                            .map(|ele| ScalarRefImpl::Utf8(ele.offset)),
                    ))
                }
                (_, &Some(AdditionalColumnType::HeaderInner(ref header_inner))) => {
                    Ok(A::output_for(
                        self.row_meta
                            .as_ref()
                            .and_then(|ele| {
                                extract_header_inner_from_meta(
                                    ele.source_meta,
                                    header_inner.inner_field.as_ref(),
                                    header_inner.data_type.as_ref(),
                                )
                            })
                            .unwrap_or(Datum::None.into()),
                    ))
                }
                (_, &Some(AdditionalColumnType::Headers(_))) => Ok(A::output_for(
                    self.row_meta
                        .as_ref()
                        .and_then(|ele| extract_headers_from_meta(ele.source_meta))
                        .unwrap_or(None),
                )),
                (_, &Some(AdditionalColumnType::PulsarMessageIdData(_))) => {
                    // message_id_data is derived internally, so it's not included here
                    Ok(A::output_for(
                        self.row_meta
                            .as_ref()
                            .and_then(|ele| {
                                extract_pulsar_message_id_data_from_meta(ele.source_meta)
                            })
                            .unwrap_or(None),
                    ))
                }
                (_, &Some(AdditionalColumnType::Filename(_))) => {
                    // Filename is used as partition in FS connectors
                    Ok(A::output_for(
                        self.row_meta
                            .as_ref()
                            .map(|ele| ScalarRefImpl::Utf8(ele.split_id)),
                    ))
                }
                (_, &Some(AdditionalColumnType::Payload(_))) => {
                    // ingest the whole payload as a single column
                    // do special logic in `KvEvent::access_field`
                    parse_field(desc)
                }
                (_, _) => {
                    // For normal columns, call the user provided closure.
                    parse_field(desc)
                }
            }
        };

        // Columns that changes have been applied to. Used to rollback when an error occurs.
        let mut applied_columns = 0;

        let result = (self.builder.column_descs.iter())
            .zip_eq_fast(self.builder.builders.iter_mut())
            .try_for_each(|(desc, builder)| {
                wrapped_f(desc).map(|output| {
                    A::apply(builder, output);
                    applied_columns += 1;
                })
            });

        match result {
            Ok(_) => {
                // commit the action by appending `Op`s and visibility
                for op in A::RECORD_TYPE.ops() {
                    self.builder.commit_record(*op, self.visible);
                }

                Ok(())
            }
            Err(e) => {
                for i in 0..applied_columns {
                    A::rollback(&mut self.builder.builders[i]);
                }
                Err(e)
            }
        }
    }

    /// Write an `Insert` record to the [`StreamChunk`], with the given fallible closure that
    /// produces one [`Datum`] by corresponding [`SourceColumnDesc`].
    ///
    /// See the [struct-level documentation](SourceStreamChunkRowWriter) for more details.
    #[inline(always)]
    pub fn do_insert<'a, D>(
        &mut self,
        mut f: impl FnMut(&SourceColumnDesc) -> AccessResult<D>,
    ) -> AccessResult<()>
    where
        D: Into<DatumCow<'a>>,
    {
        self.do_action::<InsertAction>(|desc| f(desc).map(Into::into))
    }

    /// Write a `Delete` record to the [`StreamChunk`], with the given fallible closure that
    /// produces one [`Datum`] by corresponding [`SourceColumnDesc`].
    ///
    /// See the [struct-level documentation](SourceStreamChunkRowWriter) for more details.
    #[inline(always)]
    pub fn do_delete<'a, D>(
        &mut self,
        mut f: impl FnMut(&SourceColumnDesc) -> AccessResult<D>,
    ) -> AccessResult<()>
    where
        D: Into<DatumCow<'a>>,
    {
        self.do_action::<DeleteAction>(|desc| f(desc).map(Into::into))
    }

    /// Write a `Update` record to the [`StreamChunk`], with the given fallible closure that
    /// produces two [`Datum`]s as old and new value by corresponding [`SourceColumnDesc`].
    ///
    /// See the [struct-level documentation](SourceStreamChunkRowWriter) for more details.
    #[inline(always)]
    pub fn do_update<'a, D1, D2>(
        &mut self,
        mut f: impl FnMut(&SourceColumnDesc) -> AccessResult<(D1, D2)>,
    ) -> AccessResult<()>
    where
        D1: Into<DatumCow<'a>>,
        D2: Into<DatumCow<'a>>,
    {
        self.do_action::<UpdateAction>(|desc| f(desc).map(|(old, new)| (old.into(), new.into())))
    }
}

trait RowWriterAction {
    type Output<'a>;
    const RECORD_TYPE: RecordType;

    fn output_for<'a>(datum: impl Into<DatumCow<'a>>) -> Self::Output<'a>;

    fn apply(builder: &mut ArrayBuilderImpl, output: Self::Output<'_>);

    fn rollback(builder: &mut ArrayBuilderImpl);
}

struct InsertAction;

impl RowWriterAction for InsertAction {
    type Output<'a> = DatumCow<'a>;

    const RECORD_TYPE: RecordType = RecordType::Insert;

    #[inline(always)]
    fn output_for<'a>(datum: impl Into<DatumCow<'a>>) -> Self::Output<'a> {
        datum.into()
    }

    #[inline(always)]
    fn apply(builder: &mut ArrayBuilderImpl, output: DatumCow<'_>) {
        builder.append(output)
    }

    #[inline(always)]
    fn rollback(builder: &mut ArrayBuilderImpl) {
        builder.pop().unwrap()
    }
}

struct DeleteAction;

impl RowWriterAction for DeleteAction {
    type Output<'a> = DatumCow<'a>;

    const RECORD_TYPE: RecordType = RecordType::Delete;

    #[inline(always)]
    fn output_for<'a>(datum: impl Into<DatumCow<'a>>) -> Self::Output<'a> {
        datum.into()
    }

    #[inline(always)]
    fn apply(builder: &mut ArrayBuilderImpl, output: DatumCow<'_>) {
        builder.append(output)
    }

    #[inline(always)]
    fn rollback(builder: &mut ArrayBuilderImpl) {
        builder.pop().unwrap()
    }
}

struct UpdateAction;

impl RowWriterAction for UpdateAction {
    type Output<'a> = (DatumCow<'a>, DatumCow<'a>);

    const RECORD_TYPE: RecordType = RecordType::Update;

    #[inline(always)]
    fn output_for<'a>(datum: impl Into<DatumCow<'a>>) -> Self::Output<'a> {
        let datum = datum.into();
        (datum.clone(), datum)
    }

    #[inline(always)]
    fn apply(builder: &mut ArrayBuilderImpl, output: (DatumCow<'_>, DatumCow<'_>)) {
        builder.append(output.0);
        builder.append(output.1);
    }

    #[inline(always)]
    fn rollback(builder: &mut ArrayBuilderImpl) {
        builder.pop().unwrap();
        builder.pop().unwrap();
    }
}
