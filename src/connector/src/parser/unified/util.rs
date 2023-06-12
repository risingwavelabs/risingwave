use risingwave_common::error::{ErrorCode, RwError};

use super::{AccessError, ChangeEvent};
use crate::parser::{SourceStreamChunkRowWriter, WriteGuard};

pub fn apply_row_operation_on_stream_chunk_writer(
    row_op: impl ChangeEvent,
    writer: &mut SourceStreamChunkRowWriter<'_>,
) -> std::result::Result<WriteGuard, RwError> {
    match row_op.op()? {
        super::ChangeEventOperation::Upsert => writer.insert(|column| {
            let res = row_op.access_field(&column.name, &column.data_type);
            tracing::info!(
                "insert {:?} {:?} {:?}",
                &column.name,
                &column.data_type,
                res
            );
            Ok(res?)
        }),
        super::ChangeEventOperation::Delete => writer.delete(|column| {
            let res = row_op.access_field(&column.name, &column.data_type);
            tracing::info!("del {:?} {:?} {:?}", &column.name, &column.data_type, res);
            Ok(res?)
        }),
    }
}

impl From<AccessError> for RwError {
    fn from(val: AccessError) -> Self {
        ErrorCode::InternalError(format!("AccessError: {:?}", val)).into()
    }
}
