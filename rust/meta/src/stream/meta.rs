use std::sync::Arc;

use async_trait::async_trait;
use prost::Message;
use risingwave_common::error::{ErrorCode, Result};
use risingwave_pb::common::WorkerNode;
use risingwave_pb::meta::{FragmentLocation, TableFragments};
use risingwave_pb::plan::TableRefId;
use tokio::sync::RwLock;

use crate::manager::{Config, Epoch, MetaSrvEnv, SINGLE_VERSION_EPOCH};
use crate::storage::MetaStoreRef;

#[async_trait]
pub trait StreamMetaManager: Sync + Send + 'static {
    /// [`add_fragments_to_node`] adds fragments to its belonging node.
    async fn add_fragments_to_node(&self, location: &FragmentLocation) -> Result<()>;
    /// [`load_all_fragments`] loads all fragments for all nodes.
    async fn load_all_fragments(&self) -> Result<Vec<FragmentLocation>>;
    /// [`get_fragment_node`] returns which node the fragment belongs to.
    async fn get_fragment_node(&self, actor_id: u32) -> Result<WorkerNode>;
    /// [`add_table_fragments`] stores table related fragments.
    async fn add_table_fragments(
        &self,
        table_id: &TableRefId,
        fragments: &TableFragments,
    ) -> Result<()>;
    /// [`get_table_fragments`] returns table related fragments.
    async fn get_table_fragments(&self, table_id: &TableRefId) -> Result<TableFragments>;
    /// [`drop_table_fragments`] drops table fragments info, used when `Drop MV`.
    async fn drop_table_fragments(&self, table_id: &TableRefId) -> Result<()>;
}

pub type StreamMetaManagerRef = Arc<dyn StreamMetaManager>;

/// [`StoredStreamMetaManager`] manages stream meta data using `metastore`.
pub struct StoredStreamMetaManager {
    config: Arc<Config>,
    meta_store_ref: MetaStoreRef,
    // todo: remove the lock, refactor `node_fragments` storage.
    fragment_lock: RwLock<()>,
}

impl StoredStreamMetaManager {
    pub fn new(env: MetaSrvEnv) -> Self {
        Self {
            config: env.config(),
            meta_store_ref: env.meta_store_ref(),
            fragment_lock: RwLock::new(()),
        }
    }
}

#[async_trait]
impl StreamMetaManager for StoredStreamMetaManager {
    /// [`MetaManager`] manages streaming related meta data. fragments stored as follow category
    /// in meta store:
    ///
    /// cf(node_fragment): `node` -> `FragmentLocation`, defines all included fragments in the node.
    ///
    /// cf(fragment): `actor_id` -> `Node`, defines which node the fragment belongs.
    ///
    /// cf(table_fragment): `table_ref_id` -> `TableFragments`, defines table included fragments.
    async fn add_fragments_to_node(&self, location: &FragmentLocation) -> Result<()> {
        self.fragment_lock.write().await;
        let node = location.get_node().encode_to_vec();
        let fragments = location.get_fragments();
        let mut write_batch: Vec<(&str, Vec<u8>, Vec<u8>, Epoch)> = vec![];
        for f in fragments {
            write_batch.push((
                self.config.get_fragment_cf(),
                f.get_actor_id().to_be_bytes().to_vec(),
                node.clone(),
                SINGLE_VERSION_EPOCH,
            ));
        }

        let node_pb = self
            .meta_store_ref
            .get_cf(
                self.config.get_node_fragment_cf(),
                &node.clone(),
                SINGLE_VERSION_EPOCH,
            )
            .await;
        match node_pb {
            Ok(value) => {
                let mut old_location = FragmentLocation::decode(value.as_slice())?;
                old_location.fragments.extend(location.clone().fragments);
                write_batch.push((
                    self.config.get_node_fragment_cf(),
                    node,
                    old_location.encode_to_vec(),
                    SINGLE_VERSION_EPOCH,
                ));
            }
            Err(err) => {
                if !matches!(err.inner(), ErrorCode::ItemNotFound(_)) {
                    return Err(err);
                }
                write_batch.push((
                    self.config.get_node_fragment_cf(),
                    node,
                    location.encode_to_vec(),
                    SINGLE_VERSION_EPOCH,
                ));
            }
        }

        self.meta_store_ref.put_batch_cf(write_batch).await?;

        Ok(())
    }

    async fn load_all_fragments(&self) -> Result<Vec<FragmentLocation>> {
        self.fragment_lock.read().await;
        let locations_pb = self
            .meta_store_ref
            .list_cf(self.config.get_node_fragment_cf())
            .await?;

        Ok(locations_pb
            .iter()
            .map(|l| FragmentLocation::decode(l.as_slice()).unwrap())
            .collect::<Vec<_>>())
    }

    async fn get_fragment_node(&self, actor_id: u32) -> Result<WorkerNode> {
        self.fragment_lock.read().await;
        let node_pb = self
            .meta_store_ref
            .get_cf(
                self.config.get_fragment_cf(),
                actor_id.to_be_bytes().as_ref(),
                SINGLE_VERSION_EPOCH,
            )
            .await?;

        Ok(WorkerNode::decode(node_pb.as_slice())?)
    }

    async fn add_table_fragments(
        &self,
        table_id: &TableRefId,
        fragments: &TableFragments,
    ) -> Result<()> {
        self.fragment_lock.write().await;
        self.meta_store_ref
            .put_cf(
                self.config.get_table_fragment_cf(),
                &table_id.encode_to_vec(),
                &fragments.encode_to_vec(),
                SINGLE_VERSION_EPOCH,
            )
            .await
    }

    async fn get_table_fragments(&self, table_id: &TableRefId) -> Result<TableFragments> {
        self.fragment_lock.read().await;
        let fragments_pb = self
            .meta_store_ref
            .get_cf(
                self.config.get_table_fragment_cf(),
                &table_id.encode_to_vec(),
                SINGLE_VERSION_EPOCH,
            )
            .await?;

        Ok(TableFragments::decode(fragments_pb.as_slice())?)
    }

    async fn drop_table_fragments(&self, table_id: &TableRefId) -> Result<()> {
        self.fragment_lock.write().await;
        self.meta_store_ref
            .delete_cf(
                self.config.get_table_fragment_cf(),
                &table_id.encode_to_vec(),
                SINGLE_VERSION_EPOCH,
            )
            .await
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashSet;

    use risingwave_pb::common::{HostAddress, WorkerNode};
    use risingwave_pb::stream_plan::StreamFragment;

    use super::*;

    fn make_location(node: WorkerNode, actor_ids: Vec<u32>) -> FragmentLocation {
        FragmentLocation {
            node: Some(node),
            fragments: actor_ids
                .iter()
                .map(|&i| StreamFragment {
                    actor_id: i,
                    nodes: None,
                    dispatcher: None,
                    downstream_actor_id: vec![],
                })
                .collect::<Vec<_>>(),
        }
    }

    #[tokio::test]
    async fn test_node_fragment() -> Result<()> {
        let meta_manager = StoredStreamMetaManager::new(MetaSrvEnv::for_test().await);

        // Add fragments to node 1.
        assert_eq!(meta_manager.load_all_fragments().await?.len(), 0);
        let location = make_location(
            WorkerNode {
                id: 1,
                host: Some(HostAddress {
                    host: "127.0.0.1".to_string(),
                    port: 9527,
                }),
            },
            (0..5).collect(),
        );
        meta_manager.add_fragments_to_node(&location).await?;

        let locations = meta_manager.load_all_fragments().await?;
        assert_eq!(locations.len(), 1);
        let location = locations.get(0).unwrap();
        assert_eq!(location.get_node().get_id(), 1);
        assert_eq!(location.get_fragments().len(), 5);
        assert_eq!(
            location
                .fragments
                .iter()
                .map(|f| f.actor_id)
                .collect::<Vec<_>>(),
            (0..5).collect::<Vec<_>>()
        );

        // Add more fragments to same node 1.
        let location = make_location(
            WorkerNode {
                id: 1,
                host: Some(HostAddress {
                    host: "127.0.0.1".to_string(),
                    port: 9527,
                }),
            },
            (5..10).collect(),
        );
        meta_manager.add_fragments_to_node(&location).await?;

        // Check new fragments added result.
        let locations = meta_manager.load_all_fragments().await?;
        assert_eq!(locations.len(), 1);
        let location = locations.get(0).unwrap();
        assert_eq!(location.get_node().get_id(), 1);
        assert_eq!(location.get_fragments().len(), 10);
        assert_eq!(
            location
                .fragments
                .iter()
                .map(|f| f.actor_id)
                .collect::<Vec<_>>(),
            (0..10).collect::<Vec<_>>()
        );

        // Add fragments to another node 2.
        let location = make_location(
            WorkerNode {
                id: 2,
                host: Some(HostAddress {
                    host: "127.0.0.1".to_string(),
                    port: 9528,
                }),
            },
            (10..15).collect(),
        );
        meta_manager.add_fragments_to_node(&location).await?;
        let locations = meta_manager.load_all_fragments().await?;
        assert_eq!(locations.len(), 2);
        let location0 = locations.get(0).unwrap();
        let location1 = locations.get(1).unwrap();

        assert_eq!(
            location0
                .fragments
                .iter()
                .chain(location1.fragments.iter())
                .map(|f| f.actor_id)
                .collect::<HashSet<_>>(),
            HashSet::from_iter(0..15)
        );

        let node = meta_manager.get_fragment_node(0).await?;
        assert_eq!(node.id, 1);
        let node = meta_manager.get_fragment_node(10).await?;
        assert_eq!(node.id, 2);

        Ok(())
    }

    #[tokio::test]
    async fn test_table_fragment() -> Result<()> {
        let meta_manager = StoredStreamMetaManager::new(MetaSrvEnv::for_test().await);

        let table_ref_id = TableRefId {
            schema_ref_id: None,
            table_id: 0,
        };
        let actor_ids = (0..5).collect::<Vec<u32>>();

        meta_manager
            .add_table_fragments(
                &table_ref_id,
                &TableFragments {
                    table_ref_id: Some(table_ref_id.clone()),
                    actor_ids: actor_ids.clone(),
                },
            )
            .await?;

        let fragments = meta_manager.get_table_fragments(&table_ref_id).await?;
        assert_eq!(*fragments.get_actor_ids(), actor_ids);

        meta_manager.drop_table_fragments(&table_ref_id).await?;
        let res = meta_manager.get_table_fragments(&table_ref_id).await;
        assert!(res.is_err());

        Ok(())
    }
}
