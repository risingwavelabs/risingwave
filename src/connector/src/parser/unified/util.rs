use risingwave_common::error::{ErrorCode, RwError};

use super::{AccessError, ChangeEvent};
use crate::parser::{SourceStreamChunkRowWriter, WriteGuard};

pub fn apply_row_operation_on_stream_chunk_writer(
    row_op: impl ChangeEvent,
    writer: &mut SourceStreamChunkRowWriter<'_>,
) -> std::result::Result<WriteGuard, RwError> {
    match row_op.op()? {
        super::ChangeEventOperation::Upsert => {
            writer.insert(|column| Ok(row_op.access_field(&column.name, &column.data_type)?))
        }
        super::ChangeEventOperation::Delete => {
            writer.delete(|column| Ok(row_op.access_field(&column.name, &column.data_type)?))
        }
    }
}

impl From<AccessError> for RwError {
    fn from(val: AccessError) -> Self {
        ErrorCode::InternalError(format!("AccessError: {:?}", val)).into()
    }
}
