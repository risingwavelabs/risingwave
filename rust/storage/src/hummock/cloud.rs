use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use moka::future::Cache;
use risingwave_pb::hummock::SstableMeta;

use super::Block;
use crate::hummock::sstable::SSTable;
use crate::hummock::{HummockError, HummockResult, HummockSSTableId};
use crate::object::ObjectStore;

pub async fn get_sst_meta(
    obj_client: Arc<dyn ObjectStore>,
    remote_dir: &str,
    sstable_id: u64,
) -> HummockResult<SstableMeta> {
    SSTable::decode_meta(
        &get_object_store_file(obj_client, &get_sst_meta_path(remote_dir, sstable_id)).await?,
    )
}

/// Upload table to remote object storage and return the URL
/// TODO: this function should not take block_cache as parameter -- why should we have a block cache
/// when uploading?
pub async fn gen_remote_sstable(
    obj_client: Arc<dyn ObjectStore>,
    sstable_id: u64,
    data: Bytes,
    meta: SstableMeta,
    remote_dir: &str,
    block_cache: Option<Arc<Cache<Vec<u8>, Arc<Block>>>>,
) -> HummockResult<SSTable> {
    // encode sstable metadata
    let mut buf = BytesMut::new();
    SSTable::encode_meta(&meta, &mut buf);
    let meta_bytes = buf.freeze();

    // upload sstable metadata
    let meta_path = get_sst_meta_path(remote_dir, sstable_id);
    obj_client
        .upload(&meta_path, meta_bytes)
        .await
        .map_err(HummockError::object_io_error)?;

    // upload sstable data
    let data_path = get_sst_data_path(remote_dir, sstable_id);

    obj_client
        .upload(&data_path, data)
        .await
        .map_err(HummockError::object_io_error)?;

    // load sstable
    SSTable::load(
        sstable_id,
        obj_client,
        data_path,
        meta,
        if let Some(block_cache) = block_cache {
            block_cache
        } else {
            #[cfg(test)]
            {
                Arc::new(Cache::new(2333))
            }
            #[cfg(not(test))]
            {
                panic!("must enable block cache in production mode")
            }
        },
    )
    .await
}

pub fn get_sst_meta_path(remote_dir: &str, sstable_id: HummockSSTableId) -> String {
    std::path::Path::new(remote_dir)
        .join(format!("{}.meta", sstable_id))
        .to_str()
        .unwrap()
        .to_owned()
}

pub fn get_sst_data_path(remote_dir: &str, sstable_id: HummockSSTableId) -> String {
    std::path::Path::new(remote_dir)
        .join(format!("{}.data", sstable_id))
        .to_str()
        .unwrap()
        .to_owned()
}

async fn get_object_store_file(
    obj_client: Arc<dyn ObjectStore>,
    file_path: &str,
) -> HummockResult<Vec<u8>> {
    obj_client
        .read(file_path, None)
        .await
        .map_err(HummockError::object_io_error)
}
