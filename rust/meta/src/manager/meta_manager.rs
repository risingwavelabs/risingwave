use prost::Message;
use risingwave_common::error::{ErrorCode, Result};
use risingwave_pb::meta::{Catalog, EpochState};
use tokio::sync::RwLock;

use crate::catalog::{DatabaseMetaManager, SchemaMetaManager, TableMetaManager};
use crate::manager::{
    Config, Epoch, EpochGeneratorRef, IdGeneratorManagerRef, SINGLE_VERSION_EPOCH,
};
use crate::storage::MetaStoreRef;

pub struct MetaManager {
    pub meta_store_ref: MetaStoreRef,
    pub epoch_generator: EpochGeneratorRef,
    pub id_gen_manager_ref: IdGeneratorManagerRef,
    pub config: Config,
    pub catalog_lock: RwLock<()>,
    pub fragment_lock: RwLock<()>,

    // TODO: more data could cached for single node deployment mode.
    // Backend state fields.
    current_epoch: Epoch,
    stable_epoch: Epoch,
}

impl MetaManager {
    pub async fn new(
        meta_store_ref: MetaStoreRef,
        epoch_generator: EpochGeneratorRef,
        id_gen_manager_ref: IdGeneratorManagerRef,
        config: Config,
    ) -> Self {
        let mut manager = MetaManager {
            meta_store_ref,
            epoch_generator,
            id_gen_manager_ref,
            config,

            catalog_lock: RwLock::new(()),
            fragment_lock: RwLock::new(()),
            current_epoch: Epoch::from(0),
            stable_epoch: Epoch::from(0),
        };
        let key = manager.config.get_epoch_state_key();
        let res = manager
            .meta_store_ref
            .get(key.as_bytes(), SINGLE_VERSION_EPOCH)
            .await;
        match res {
            Ok(value) => {
                let proto = EpochState::decode(value.as_slice()).unwrap();
                manager.current_epoch = Epoch::from(proto.get_current_epoch());
                manager.stable_epoch = Epoch::from(proto.get_stable_epoch());
            }
            Err(err) => {
                if !matches!(err.inner(), ErrorCode::ItemNotFound(_)) {
                    panic!("{}", err)
                }
            }
        }

        manager
    }

    pub async fn get_catalog(&self) -> Result<Catalog> {
        let databases = self.list_databases().await?;
        let schemas = self.list_schemas().await?;
        let tables = self.list_tables().await?;

        Ok(Catalog {
            databases,
            schemas,
            tables,
        })
    }
}
