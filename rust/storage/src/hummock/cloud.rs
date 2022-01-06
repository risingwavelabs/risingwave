use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use risingwave_pb::hummock::TableMeta;

use crate::hummock::table::Table;
use crate::hummock::{HummockError, HummockResult, REMOTE_DIR};
use crate::object::ObjectStore;

/// Upload table to remote object storage and return the URL
pub async fn gen_remote_table(
    obj_client: Arc<dyn ObjectStore>,
    table_id: u64,
    data: Bytes,
    meta: TableMeta,
    remote_dir: Option<&str>,
) -> HummockResult<Table> {
    // encode table metadata
    let mut buf = BytesMut::new();
    Table::encode_meta(&meta, &mut buf);
    let meta_bytes = buf.freeze();

    // get remote dir
    let remote_dir = remote_dir.unwrap_or(REMOTE_DIR);

    // upload table metadata
    let meta_path = format!("{}{}.meta", remote_dir, table_id);
    obj_client
        .upload(&meta_path, meta_bytes)
        .await
        .map_err(HummockError::object_io_error)?;

    // upload table data
    let data_path = format!("{}{}.data", remote_dir, table_id);

    obj_client
        .upload(&data_path, data)
        .await
        .map_err(HummockError::object_io_error)?;

    // load table
    Table::load(table_id, obj_client, data_path, meta).await
}

#[cfg(test)]
mod tests {

    #[tokio::test]
    async fn test_upload() {}
}
