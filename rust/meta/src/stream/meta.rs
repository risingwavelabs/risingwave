use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;
use itertools::Itertools;
use prost::Message;
use risingwave_common::error::{ErrorCode, Result};
use risingwave_pb::common::WorkerNode;
use risingwave_pb::meta::{ActorLocation, TableActors};
use risingwave_pb::plan::TableRefId;
use tokio::sync::RwLock;

use crate::manager::{Config, Epoch, MetaSrvEnv, SINGLE_VERSION_EPOCH};
use crate::storage::MetaStoreRef;

#[async_trait]
// TODO: refactor this trait, it's too ugly!!
pub trait StreamMetaManager: Sync + Send + 'static {
    /// [`add_actors_to_node`] adds actors to its belonging node.
    async fn add_actors_to_node(&self, location: &ActorLocation) -> Result<()>;
    /// [`load_all_actors`] loads all actors for all nodes.
    async fn load_all_actors(&self) -> Result<Vec<ActorLocation>>;
    /// [`get_actor_node`] returns which node the actor belongs to.
    async fn get_actor_node(&self, actor_id: u32) -> Result<WorkerNode>;
    /// [`add_table_actors`] stores table related actors.
    async fn add_table_actors(&self, table_id: &TableRefId, actors: &TableActors) -> Result<()>;
    /// [`get_table_actors`] returns table related actors.
    async fn get_table_actors(&self, table_id: &TableRefId) -> Result<TableActors>;
    /// [`drop_table_actors`] drops table actors info, used when `Drop MV`.
    async fn drop_table_actors(&self, table_id: &TableRefId) -> Result<()>;
    /// [`add_materialized_view_actors`] stores materialized view associated actor ids.
    async fn add_materialized_view_actors(
        &self,
        table_id: &TableRefId,
        actor_ids: &[u32],
    ) -> Result<()>;
    async fn get_materialized_view_actors(&self, table_id: &TableRefId) -> Result<Vec<u32>>;
}

pub type StreamMetaManagerRef = Arc<dyn StreamMetaManager>;

/// [`StoredStreamMetaManager`] manages stream meta data using `metastore`.
pub struct StoredStreamMetaManager {
    config: Arc<Config>,
    meta_store_ref: MetaStoreRef,
    // todo: remove the lock, refactor `node_actors` storage.
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
    /// [`MetaManager`] manages streaming related meta data. actors stored as follow category
    /// in meta store:
    ///
    /// cf(node_actor): `node` -> `ActorLocation`, defines all included actors in the node.
    ///
    /// cf(actor): `actor_id` -> `Node`, defines which node the actor belongs.
    ///
    /// cf(table_actor): `table_ref_id` -> `TableActors`, defines table included actors.
    async fn add_actors_to_node(&self, location: &ActorLocation) -> Result<()> {
        let _ = self.fragment_lock.write().await;

        let node = location.get_node()?.encode_to_vec();
        let actors = location.get_actors();
        let mut write_batch: Vec<(&str, Vec<u8>, Vec<u8>, Epoch)> = vec![];
        for f in actors {
            write_batch.push((
                self.config.get_actor_cf(),
                f.get_actor_id().to_be_bytes().to_vec(),
                node.clone(),
                SINGLE_VERSION_EPOCH,
            ));
        }

        let node_pb = self
            .meta_store_ref
            .get_cf(
                self.config.get_node_actor_cf(),
                &node.clone(),
                SINGLE_VERSION_EPOCH,
            )
            .await;
        match node_pb {
            Ok(value) => {
                let mut old_location = ActorLocation::decode(value.as_slice())?;
                old_location.actors.extend(location.clone().actors);
                write_batch.push((
                    self.config.get_node_actor_cf(),
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
                    self.config.get_node_actor_cf(),
                    node,
                    location.encode_to_vec(),
                    SINGLE_VERSION_EPOCH,
                ));
            }
        }

        self.meta_store_ref.put_batch_cf(write_batch).await?;

        Ok(())
    }

    async fn load_all_actors(&self) -> Result<Vec<ActorLocation>> {
        let _ = self.fragment_lock.read().await;

        let locations_pb = self
            .meta_store_ref
            .list_cf(self.config.get_node_actor_cf())
            .await?;

        Ok(locations_pb
            .iter()
            .map(|l| ActorLocation::decode(l.as_slice()).unwrap())
            .collect::<Vec<_>>())
    }

    async fn get_actor_node(&self, actor_id: u32) -> Result<WorkerNode> {
        let _ = self.fragment_lock.read().await;

        let node_pb = self
            .meta_store_ref
            .get_cf(
                self.config.get_actor_cf(),
                actor_id.to_be_bytes().as_ref(),
                SINGLE_VERSION_EPOCH,
            )
            .await?;

        Ok(WorkerNode::decode(node_pb.as_slice())?)
    }

    async fn add_table_actors(&self, table_id: &TableRefId, actors: &TableActors) -> Result<()> {
        let _ = self.fragment_lock.write().await;

        self.meta_store_ref
            .put_cf(
                self.config.get_table_actor_cf(),
                &table_id.encode_to_vec(),
                &actors.encode_to_vec(),
                SINGLE_VERSION_EPOCH,
            )
            .await
    }

    async fn get_table_actors(&self, table_id: &TableRefId) -> Result<TableActors> {
        let _ = self.fragment_lock.read().await;

        let actors_pb = self
            .meta_store_ref
            .get_cf(
                self.config.get_table_actor_cf(),
                &table_id.encode_to_vec(),
                SINGLE_VERSION_EPOCH,
            )
            .await?;

        Ok(TableActors::decode(actors_pb.as_slice())?)
    }

    // TODO: update `actor_cf`
    async fn drop_table_actors(&self, table_id: &TableRefId) -> Result<()> {
        let _ = self.fragment_lock.write().await;

        let table_actors = {
            let actors_pb = self
                .meta_store_ref
                .get_cf(
                    self.config.get_table_actor_cf(),
                    &table_id.encode_to_vec(),
                    SINGLE_VERSION_EPOCH,
                )
                .await?;

            TableActors::decode(actors_pb.as_slice())?
        };

        let mut node_to_actors: HashMap<_, HashSet<_>> = HashMap::new();

        for actor_id in table_actors.actor_ids {
            let node_pb = self
                .meta_store_ref
                .get_cf(
                    self.config.get_actor_cf(),
                    actor_id.to_be_bytes().as_ref(),
                    SINGLE_VERSION_EPOCH,
                )
                .await?;

            node_to_actors.entry(node_pb).or_default().insert(actor_id);
        }

        for (node_pb, actors) in node_to_actors {
            let actor_location_pb = self
                .meta_store_ref
                .get_cf(
                    self.config.get_node_actor_cf(),
                    &node_pb,
                    SINGLE_VERSION_EPOCH,
                )
                .await?;

            let mut actor_location = ActorLocation::decode(actor_location_pb.as_slice())?;
            actor_location
                .actors
                .retain(|a| !actors.contains(&a.actor_id));

            // Delete these actors in `node_actor` cf.
            self.meta_store_ref
                .put_cf(
                    self.config.get_node_actor_cf(),
                    &node_pb,
                    &actor_location.encode_to_vec(),
                    SINGLE_VERSION_EPOCH,
                )
                .await?;
        }

        // Delete this table in `table_actor` cf.
        self.meta_store_ref
            .delete_cf(
                self.config.get_table_actor_cf(),
                &table_id.encode_to_vec(),
                SINGLE_VERSION_EPOCH,
            )
            .await?;

        Ok(())
    }

    async fn add_materialized_view_actors(
        &self,
        table_id: &TableRefId,
        actor_ids: &[u32],
    ) -> Result<()> {
        self.meta_store_ref
            .put_cf(
                self.config.get_materialized_view_actors_cf(),
                &table_id.encode_to_vec(),
                &actor_ids
                    .iter()
                    .flat_map(|id| id.to_be_bytes())
                    .collect_vec(),
                SINGLE_VERSION_EPOCH,
            )
            .await?;
        Ok(())
    }

    async fn get_materialized_view_actors(&self, table_id: &TableRefId) -> Result<Vec<u32>> {
        let bytes = self
            .meta_store_ref
            .get_cf(
                self.config.get_materialized_view_actors_cf(),
                &table_id.encode_to_vec(),
                SINGLE_VERSION_EPOCH,
            )
            .await?;
        let ids = bytes
            .chunks(4)
            .map(|chunk| u32::from_be_bytes(chunk.try_into().unwrap()))
            .collect_vec();
        Ok(ids)
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashSet;

    use risingwave_pb::common::{HostAddress, WorkerNode, WorkerType};
    use risingwave_pb::stream_plan::StreamActor;

    use super::*;

    fn make_location(node: WorkerNode, actor_ids: Vec<u32>) -> ActorLocation {
        ActorLocation {
            node: Some(node),
            actors: actor_ids
                .iter()
                .map(|&i| StreamActor {
                    actor_id: i,
                    nodes: None,
                    dispatcher: None,
                    downstream_actor_id: vec![],
                })
                .collect::<Vec<_>>(),
        }
    }

    #[tokio::test]
    async fn test_node_actors() -> Result<()> {
        let meta_manager = StoredStreamMetaManager::new(MetaSrvEnv::for_test().await);

        // Add actors to node 1.
        assert_eq!(meta_manager.load_all_actors().await?.len(), 0);
        let location = make_location(
            WorkerNode {
                id: 1,
                r#type: WorkerType::ComputeNode as i32,
                host: Some(HostAddress {
                    host: "127.0.0.1".to_string(),
                    port: 9527,
                }),
            },
            (0..5).collect(),
        );
        meta_manager.add_actors_to_node(&location).await?;

        let locations = meta_manager.load_all_actors().await?;
        assert_eq!(locations.len(), 1);
        let location = locations.get(0).unwrap();
        assert_eq!(location.get_node().unwrap().get_id(), 1);
        assert_eq!(location.get_actors().len(), 5);
        assert_eq!(
            location
                .actors
                .iter()
                .map(|f| f.actor_id)
                .collect::<Vec<_>>(),
            (0..5).collect::<Vec<_>>()
        );

        // Add more actors to same node 1.
        let location = make_location(
            WorkerNode {
                id: 1,
                r#type: WorkerType::ComputeNode as i32,
                host: Some(HostAddress {
                    host: "127.0.0.1".to_string(),
                    port: 9527,
                }),
            },
            (5..10).collect(),
        );
        meta_manager.add_actors_to_node(&location).await?;

        // Check new actors added result.
        let locations = meta_manager.load_all_actors().await?;
        assert_eq!(locations.len(), 1);
        let location = locations.get(0).unwrap();
        assert_eq!(location.get_node().unwrap().get_id(), 1);
        assert_eq!(location.get_actors().len(), 10);
        assert_eq!(
            location
                .actors
                .iter()
                .map(|f| f.actor_id)
                .collect::<Vec<_>>(),
            (0..10).collect::<Vec<_>>()
        );

        // Add actors to another node 2.
        let location = make_location(
            WorkerNode {
                id: 2,
                r#type: WorkerType::ComputeNode as i32,
                host: Some(HostAddress {
                    host: "127.0.0.1".to_string(),
                    port: 9528,
                }),
            },
            (10..15).collect(),
        );
        meta_manager.add_actors_to_node(&location).await?;
        let locations = meta_manager.load_all_actors().await?;
        assert_eq!(locations.len(), 2);
        let location0 = locations.get(0).unwrap();
        let location1 = locations.get(1).unwrap();

        assert_eq!(
            location0
                .actors
                .iter()
                .chain(location1.actors.iter())
                .map(|f| f.actor_id)
                .collect::<HashSet<_>>(),
            HashSet::from_iter(0..15)
        );

        let node = meta_manager.get_actor_node(0).await?;
        assert_eq!(node.id, 1);
        let node = meta_manager.get_actor_node(10).await?;
        assert_eq!(node.id, 2);

        Ok(())
    }

    #[tokio::test]
    async fn test_table_actor() -> Result<()> {
        let meta_manager = StoredStreamMetaManager::new(MetaSrvEnv::for_test().await);

        let table_ref_id = TableRefId {
            schema_ref_id: None,
            table_id: 0,
        };
        let actor_ids = (0..5).collect::<Vec<u32>>();

        meta_manager
            .add_actors_to_node(&make_location(
                WorkerNode {
                    id: 114514,
                    r#type: WorkerType::ComputeNode as i32,
                    host: Some(HostAddress {
                        host: "127.0.0.1".to_string(),
                        port: 8888,
                    }),
                },
                actor_ids.clone(),
            ))
            .await?;

        meta_manager
            .add_table_actors(
                &table_ref_id,
                &TableActors {
                    table_ref_id: Some(table_ref_id.clone()),
                    actor_ids: actor_ids.clone(),
                },
            )
            .await?;

        let actors = meta_manager.get_table_actors(&table_ref_id).await?;
        assert_eq!(*actors.get_actor_ids(), actor_ids);

        meta_manager.drop_table_actors(&table_ref_id).await?;
        let res = meta_manager.get_table_actors(&table_ref_id).await;
        assert!(res.is_err());

        Ok(())
    }

    #[tokio::test]
    async fn test_materialized_view_actors() -> Result<()> {
        let meta_manager = StoredStreamMetaManager::new(MetaSrvEnv::for_test().await);
        let table_ref_id = TableRefId {
            schema_ref_id: None,
            table_id: 0,
        };
        let actor_ids = vec![1, 2, 3, 4, 5];
        meta_manager
            .add_materialized_view_actors(&table_ref_id, &actor_ids[..])
            .await?;
        let stored_actor_ids = meta_manager
            .get_materialized_view_actors(&table_ref_id)
            .await?;
        assert_eq!(actor_ids, stored_actor_ids);
        Ok(())
    }
}
