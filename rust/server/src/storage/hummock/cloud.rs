use std::sync::Arc;

use crate::storage::hummock::{HummockError, HummockResult, Table, REMOTE_DIR};
use crate::storage::object::{BlockLocation, ObjectStore};
use bytes::{Bytes, BytesMut};
use risingwave_pb::hummock::TableMeta;

pub async fn download_meta(client: &'_ dyn ObjectStore, meta_path: &str) -> HummockResult<Vec<u8>> {
    let size = client
        .metadata(meta_path)
        .await
        .map_err(|e| {
            HummockError::ObjectIoError(format!("Read metadata for metadata failed: {}", e))
        })?
        .total_size;
    client
        .read(meta_path, BlockLocation { offset: 0, size })
        .await
        .map_err(|e| HummockError::ObjectIoError(format!("Read metadata failed: {}", e)))
}

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

    let remote_dir = remote_dir.unwrap_or(REMOTE_DIR);

    // upload table metadata
    let meta_path = format!("{}{}.meta", remote_dir, table_id);
    obj_client
        .upload(&meta_path, meta_bytes)
        .await
        .map_err(|e| HummockError::ObjectIoError(e.to_string()))?;

    // upload table data
    let data_path = format!("{}{}.data", remote_dir, table_id);

    obj_client
        .upload(&data_path, data)
        .await
        .map_err(|e| HummockError::ObjectIoError(e.to_string()))?;

    // load table
    Table::load(table_id, obj_client, data_path, meta).await
}

#[cfg(test)]
mod tests {

    #[tokio::test]
    async fn test_upload() {}
}
