// Copyright 2023 RisingWave Labs
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

use risingwave_common::error::{ErrorCode, RwError};

use super::{Access, AccessError, ChangeEvent};
use crate::parser::unified::ChangeEventOperation;
use crate::parser::{SourceStreamChunkRowWriter, WriteGuard};

pub fn apply_delete_on_stream_chunk_writer(
    row_op: impl ChangeEvent,
    writer: &mut SourceStreamChunkRowWriter<'_>,
) -> std::result::Result<WriteGuard, RwError> {
    writer.delete(|column| {
        let res = row_op.access_field(&column.name, &column.data_type);
        match res {
            Ok(datum) => Ok(datum),
            Err(e) => {
                tracing::error!(name=?column.name, data_type=?&column.data_type, err=?e, "delete column error");
                if column.is_pk {
                    // It should be an error when pk column is missing in the message
                    Err(e)?
                } else {
                    Ok(None)
                }
            }
        }
    })
}

pub fn apply_upsert_on_stream_chunk_writer(
    row_op: impl ChangeEvent,
    writer: &mut SourceStreamChunkRowWriter<'_>,
) -> std::result::Result<WriteGuard, RwError> {
    writer.insert(|column| {
        let res = match row_op.access_field(&column.name, &column.data_type) {
            Ok(o) => Ok(o),
            Err(AccessError::Undefined { name, .. }) if !column.is_pk && name == column.name => {
                // Fill in null value for non-pk column
                // TODO: figure out a way to fill in not-null default value if user specifies one
                Ok(None)
            }
            Err(e) => Err(e),
        };
        tracing::trace!(
            "inserted {:?} {:?} is_pk:{:?} {:?} ",
            &column.name,
            &column.data_type,
            &column.is_pk,
            res
        );
        Ok(res?)
    })
}

pub fn apply_row_operation_on_stream_chunk_writer_with_op(
    row_op: impl ChangeEvent,
    writer: &mut SourceStreamChunkRowWriter<'_>,
    op: ChangeEventOperation,
) -> std::result::Result<WriteGuard, RwError> {
    match op {
        ChangeEventOperation::Upsert => apply_upsert_on_stream_chunk_writer(row_op, writer),
        ChangeEventOperation::Delete => apply_delete_on_stream_chunk_writer(row_op, writer),
    }
}

pub fn apply_row_operation_on_stream_chunk_writer(
    row_op: impl ChangeEvent,
    writer: &mut SourceStreamChunkRowWriter<'_>,
) -> std::result::Result<WriteGuard, RwError> {
    let op = row_op.op()?;
    apply_row_operation_on_stream_chunk_writer_with_op(row_op, writer, op)
}

pub fn apply_row_accessor_on_stream_chunk_writer(
    accessor: impl Access,
    writer: &mut SourceStreamChunkRowWriter<'_>,
) -> std::result::Result<WriteGuard, RwError> {
    writer.insert(|column| {
        let res = match accessor.access(&[&column.name], Some(&column.data_type)) {
            Ok(o) => Ok(o),
            Err(AccessError::Undefined { name, .. }) if !column.is_pk && name == column.name => {
                // Fill in null value for non-pk column
                // TODO: figure out a way to fill in not-null default value if user specifies one
                Ok(None)
            }
            Err(e) => Err(e),
        };
        tracing::trace!(
            "inserted {:?} {:?} is_pk:{:?} {:?} ",
            &column.name,
            &column.data_type,
            &column.is_pk,
            res
        );
        Ok(res?)
    })
}

impl From<AccessError> for RwError {
    fn from(val: AccessError) -> Self {
        ErrorCode::InternalError(format!("AccessError: {:?}", val)).into()
    }
}
