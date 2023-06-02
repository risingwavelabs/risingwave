use risingwave_common::error::{ErrorCode, RwError};

use super::{AccessError, OperateRow};
use crate::parser::{SourceStreamChunkRowWriter, WriteGuard};

pub fn apply_row_operation_on_stream_chunk_writer(
    row_op: impl OperateRow,
    mut writer: SourceStreamChunkRowWriter<'_>,
) -> std::result::Result<WriteGuard, RwError> {
    match row_op.op()? {
        super::RowOperation::Update => writer.update(|column| {
            Ok((
                row_op.access_before(&column.name, &column.data_type)?,
                row_op.access_field(&column.name, &column.data_type)?,
            ))
        }),
        super::RowOperation::Insert => {
            writer.insert(|column| Ok(row_op.access_field(&column.name, &column.data_type)?))
        }
        super::RowOperation::Delete => {
            writer.delete(|column| Ok(row_op.access_field(&column.name, &column.data_type)?))
        }
    }
}

impl From<AccessError> for RwError {
    fn from(val: AccessError) -> Self {
        ErrorCode::InternalError(format!("AccessError: {:?}", val)).into()
    }
}
