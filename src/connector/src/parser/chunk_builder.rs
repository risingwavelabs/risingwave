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

use std::sync::LazyLock;

use risingwave_common::array::{ArrayBuilderImpl, Op, StreamChunk};
use risingwave_common::bitmap::BitmapBuilder;
use risingwave_common::log::LogSuppresser;
use risingwave_common::types::{Datum, DatumCow, ScalarRefImpl};
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_connector_codec::decoder::{AccessError, AccessResult};
use risingwave_pb::plan_common::additional_column::ColumnType as AdditionalColumnType;
use thiserror_ext::AsReport;

use super::MessageMeta;
use crate::parser::utils::{
    extract_cdc_meta_column, extract_header_inner_from_meta, extract_headers_from_meta,
    extract_subject_from_meta, extract_timestamp_from_meta,
};
use crate::source::{SourceColumnDesc, SourceColumnType, SourceMeta};

/// A builder for building a [`StreamChunk`] from [`SourceColumnDesc`].
pub struct SourceStreamChunkBuilder {
    descs: Vec<SourceColumnDesc>,
    builders: Vec<ArrayBuilderImpl>,
    op_builder: Vec<Op>,
    vis_builder: BitmapBuilder,
}

impl SourceStreamChunkBuilder {
    pub fn with_capacity(descs: Vec<SourceColumnDesc>, cap: usize) -> Self {
        let builders = descs
            .iter()
            .map(|desc| desc.data_type.create_array_builder(cap))
            .collect();

        Self {
            descs,
            builders,
            op_builder: Vec::with_capacity(cap),
            vis_builder: BitmapBuilder::with_capacity(cap),
        }
    }

    pub fn row_writer(&mut self) -> SourceStreamChunkRowWriter<'_> {
        SourceStreamChunkRowWriter {
            builder: self,
            visible: true, // write visible rows by default
            row_meta: None,
        }
    }

    /// Consumes the builder and returns a [`StreamChunk`].
    pub fn finish(self) -> StreamChunk {
        StreamChunk::with_visibility(
            self.op_builder,
            self.builders
                .into_iter()
                .map(|builder| builder.finish().into())
                .collect(),
            self.vis_builder.finish(),
        )
    }

    /// Resets the builder and returns a [`StreamChunk`], while reserving `next_cap` capacity for
    /// the builders of the next [`StreamChunk`].
    #[must_use]
    pub fn take_and_reserve(&mut self, next_cap: usize) -> StreamChunk {
        let descs = std::mem::take(&mut self.descs); // we don't use `descs` in `finish`
        let builder = std::mem::replace(self, Self::with_capacity(descs, next_cap));
        builder.finish()
    }

    pub fn len(&self) -> usize {
        self.op_builder.len()
    }

    pub fn is_empty(&self) -> bool {
        self.op_builder.is_empty()
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

    /// Convert the row writer to invisible row writer.
    pub fn invisible(mut self) -> Self {
        self.visible = false;
        self
    }
}

impl SourceStreamChunkRowWriter<'_> {
    fn append_op(&mut self, op: Op) {
        self.builder.op_builder.push(op);
        self.builder.vis_builder.append(self.visible);
    }

    fn do_action<'a, A: OpAction>(
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
                        &self.row_meta.map(|ele| ele.meta),
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
                            if let SourceMeta::DebeziumCdc(cdc_meta) = row_meta.meta {
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
                    Some(row_meta) => Ok(A::output_for(extract_timestamp_from_meta(row_meta.meta))),
                    None => parse_field(desc), // parse from payload
                },
                (_, &Some(AdditionalColumnType::CollectionName(_))) => {
                    // collection name for `mongodb-cdc` should be parsed from the message payload
                    parse_field(desc)
                }
                (_, &Some(AdditionalColumnType::Subject(_))) => Ok(A::output_for(
                    self.row_meta
                        .as_ref()
                        .and_then(|ele| extract_subject_from_meta(ele.meta))
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
                                    ele.meta,
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
                        .and_then(|ele| extract_headers_from_meta(ele.meta))
                        .unwrap_or(None),
                )),
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

        let result = (self.builder.descs.iter())
            .zip_eq_fast(self.builder.builders.iter_mut())
            .try_for_each(|(desc, builder)| {
                wrapped_f(desc).map(|output| {
                    A::apply(builder, output);
                    applied_columns += 1;
                })
            });

        match result {
            Ok(_) => {
                A::finish(self);
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
        self.do_action::<OpActionInsert>(|desc| f(desc).map(Into::into))
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
        self.do_action::<OpActionDelete>(|desc| f(desc).map(Into::into))
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
        self.do_action::<OpActionUpdate>(|desc| f(desc).map(|(old, new)| (old.into(), new.into())))
    }
}

trait OpAction {
    type Output<'a>;

    fn output_for<'a>(datum: impl Into<DatumCow<'a>>) -> Self::Output<'a>;

    fn apply(builder: &mut ArrayBuilderImpl, output: Self::Output<'_>);

    fn rollback(builder: &mut ArrayBuilderImpl);

    fn finish(writer: &mut SourceStreamChunkRowWriter<'_>);
}

struct OpActionInsert;

impl OpAction for OpActionInsert {
    type Output<'a> = DatumCow<'a>;

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

    #[inline(always)]
    fn finish(writer: &mut SourceStreamChunkRowWriter<'_>) {
        writer.append_op(Op::Insert);
    }
}

struct OpActionDelete;

impl OpAction for OpActionDelete {
    type Output<'a> = DatumCow<'a>;

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

    #[inline(always)]
    fn finish(writer: &mut SourceStreamChunkRowWriter<'_>) {
        writer.append_op(Op::Delete);
    }
}

struct OpActionUpdate;

impl OpAction for OpActionUpdate {
    type Output<'a> = (DatumCow<'a>, DatumCow<'a>);

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

    #[inline(always)]
    fn finish(writer: &mut SourceStreamChunkRowWriter<'_>) {
        writer.append_op(Op::UpdateDelete);
        writer.append_op(Op::UpdateInsert);
    }
}
