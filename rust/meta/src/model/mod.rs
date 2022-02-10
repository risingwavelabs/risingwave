mod catalog;
mod cluster;
mod stream;

use async_trait::async_trait;
pub use catalog::*;
pub use cluster::*;
use prost::Message;
use risingwave_common::error::Result;
pub use stream::*;

use crate::manager::{Epoch, SINGLE_VERSION_EPOCH};
use crate::storage::MetaStoreRef;

pub type TableRawId = i32;
pub type ActorId = u32;
pub type FragmentId = u32;

/// `MetadataModel` defines basic model operations in CRUD.
#[async_trait]
pub trait MetadataModel: Sized {
    /// Serialized prost message type.
    type ProstType: Message + Default;
    /// Serialized key type.
    type KeyType: Message;

    /// Column family for this model.
    fn cf_name() -> String;

    /// Serialize to protobuf.
    fn to_protobuf(&self) -> Self::ProstType;

    /// Deserialize from protobuf.
    fn from_protobuf(prost: Self::ProstType) -> Self;

    /// Current record key.
    fn key(&self) -> Result<Self::KeyType>;

    /// Version for current record, only one version by default.
    fn version(&self) -> Epoch {
        SINGLE_VERSION_EPOCH
    }

    /// `list` returns all records in this model.
    async fn list(store: &MetaStoreRef) -> Result<Vec<Self>> {
        let bytes_vec = store.list_cf(&Self::cf_name()).await?;
        Ok(bytes_vec
            .iter()
            .map(|bytes| Self::from_protobuf(Self::ProstType::decode(bytes.as_slice()).unwrap()))
            .collect::<Vec<_>>())
    }

    /// `insert` insert a new record in meta store, replaced it if the record already exist.
    async fn insert(&self, store: &MetaStoreRef) -> Result<()> {
        store
            .put_cf(
                &Self::cf_name(),
                &self.key()?.encode_to_vec(),
                &self.to_protobuf().encode_to_vec(),
                self.version(),
            )
            .await
    }

    /// `delete` drop records (in multi-version if has) from meta store with associated key.
    async fn delete(store: &MetaStoreRef, key: &Self::KeyType) -> Result<()> {
        store
            .delete_all_cf(&Self::cf_name(), &key.encode_to_vec())
            .await
    }

    /// `select` query a record with associated key and version.
    async fn select(store: &MetaStoreRef, key: &Self::KeyType, version: Epoch) -> Result<Self> {
        let byte_vec = store
            .get_cf(&Self::cf_name(), &key.encode_to_vec(), version)
            .await?;
        Ok(Self::from_protobuf(Self::ProstType::decode(
            byte_vec.as_slice(),
        )?))
    }
}
