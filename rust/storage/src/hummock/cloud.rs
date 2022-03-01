// TODO(MrCroxx): This file needs to be refactored.

use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use risingwave_pb::hummock::SstableMeta;

use crate::hummock::sstable::SSTable;
use crate::hummock::{HummockError, HummockResult, HummockSSTableId};
use crate::object::ObjectStore;

const DEFAULT_META_BYTES_SIZE: usize = 4096;

pub async fn get_sst_meta(
    obj_client: Arc<dyn ObjectStore>,
    remote_dir: &str,
    sstable_id: u64,
) -> HummockResult<SstableMeta> {
    SSTable::decode_meta(
        &get_object_store_file(obj_client, &get_sst_meta_path(remote_dir, sstable_id)).await?,
    )
}

/// Upload table to remote object store.
pub async fn upload(
    client: &Arc<dyn ObjectStore>,
    sst_id: u64,
    meta: &SstableMeta,
    data: Bytes,
    path: &str,
) -> HummockResult<String> {
    let mut buf = BytesMut::with_capacity(DEFAULT_META_BYTES_SIZE);
    SSTable::encode_meta(meta, &mut buf);
    let buf_meta = buf.freeze();

    let data_path = get_sst_data_path(path, sst_id);
    client
        .upload(&data_path, data)
        .await
        .map_err(HummockError::object_io_error)?;

    let meta_path = get_sst_meta_path(path, sst_id);
    if let Err(e) = client.upload(&meta_path, buf_meta).await {
        client
            .delete(&data_path)
            .await
            .map_err(HummockError::object_io_error)?;
        return Err(HummockError::object_io_error(e));
    }
    Ok(data_path)
}

pub fn get_sst_meta_path(path: &str, sst_id: HummockSSTableId) -> String {
    format!("{}/{}.meta", path, sst_id)
}

pub fn get_sst_data_path(path: &str, sst_id: HummockSSTableId) -> String {
    format!("{}/{}.data", path, sst_id)
}

async fn get_object_store_file(
    obj_client: Arc<dyn ObjectStore>,
    file_path: &str,
) -> HummockResult<Bytes> {
    obj_client
        .read(file_path, None)
        .await
        .map_err(HummockError::object_io_error)
}
