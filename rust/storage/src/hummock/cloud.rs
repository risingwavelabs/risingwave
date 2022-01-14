use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use risingwave_pb::hummock::SstableMeta;

use crate::hummock::sstable::SSTable;
use crate::hummock::{HummockError, HummockResult, REMOTE_DIR};
use crate::object::ObjectStore;

/// Upload table to remote object storage and return the URL
pub async fn gen_remote_sstable(
    obj_client: Arc<dyn ObjectStore>,
    sstable_id: u64,
    data: Bytes,
    meta: SstableMeta,
    remote_dir: Option<&str>,
) -> HummockResult<SSTable> {
    // encode sstable metadata
    let mut buf = BytesMut::new();
    SSTable::encode_meta(&meta, &mut buf);
    let meta_bytes = buf.freeze();

    // get remote dir
    let remote_dir = remote_dir.unwrap_or(REMOTE_DIR);

    // upload sstable metadata
    let meta_path = format!("{}{}.meta", remote_dir, sstable_id);
    obj_client
        .upload(&meta_path, meta_bytes)
        .await
        .map_err(HummockError::object_io_error)?;

    // upload sstable data
    let data_path = format!("{}{}.data", remote_dir, sstable_id);

    obj_client
        .upload(&data_path, data)
        .await
        .map_err(HummockError::object_io_error)?;

    // load sstable
    SSTable::load(sstable_id, obj_client, data_path, meta).await
}

#[cfg(test)]
mod tests {

    #[tokio::test]
    async fn test_upload() {}
}
