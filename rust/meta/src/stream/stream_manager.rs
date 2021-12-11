use async_trait::async_trait;
use prost::Message;
use risingwave_common::error::{ErrorCode, Result};
use risingwave_pb::meta::cluster::Node;
use risingwave_pb::meta::FragmentLocation;

use crate::manager::{Epoch, MetaManager, SINGLE_VERSION_EPOCH};

#[async_trait]
pub trait StreamMetaManager {
    /// [`add_fragments_to_node`] adds fragments to its belonging node.
    async fn add_fragments_to_node(&self, location: &FragmentLocation) -> Result<()>;
    /// [`load_all_fragments`] loads all fragments for all nodes.
    async fn load_all_fragments(&self) -> Result<Vec<FragmentLocation>>;
    /// [`get_fragment_node`] returns which node the fragment belongs to.
    async fn get_fragment_node(&self, fragment_id: u32) -> Result<Node>;
}

#[async_trait]
impl StreamMetaManager for MetaManager {
    /// [`MetaManager`] manages streaming related meta data. fragments stored as follow category
    /// in meta store:
    ///
    /// cf(node_fragment): `node` -> `FragmentLocation`, defines all included fragments in the node.
    ///
    /// cf(fragment): `fragment_id` -> `node`, defines which node the fragment belongs.
    async fn add_fragments_to_node(&self, location: &FragmentLocation) -> Result<()> {
        let node = location.get_node().encode_to_vec();
        let fragments = location.get_fragments();
        let mut write_batch: Vec<(&str, Vec<u8>, Vec<u8>, Epoch)> = vec![];
        for f in fragments.clone() {
            write_batch.push((
                self.config.get_fragment_cf(),
                f.get_fragment_id().to_be_bytes().to_vec(),
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
        let locations_pb = self
            .meta_store_ref
            .list_cf(self.config.get_node_fragment_cf())
            .await?;

        Ok(locations_pb
            .iter()
            .map(|l| FragmentLocation::decode(l.as_slice()).unwrap())
            .collect::<Vec<_>>())
    }

    async fn get_fragment_node(&self, fragment_id: u32) -> Result<Node> {
        let node_pb = self
            .meta_store_ref
            .get_cf(
                self.config.get_fragment_cf(),
                &fragment_id.to_be_bytes().to_vec(),
                SINGLE_VERSION_EPOCH,
            )
            .await?;

        Ok(Node::decode(node_pb.as_slice())?)
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashSet;
    use std::sync::Arc;

    use risingwave_pb::meta::cluster::Node;
    use risingwave_pb::stream_plan::StreamFragment;
    use risingwave_pb::task_service::HostAddress;

    use super::*;
    use crate::manager::{Config, IdGeneratorManager, MemEpochGenerator};
    use crate::storage::MemStore;

    fn make_location(node: Node, fragment_ids: Vec<u32>) -> FragmentLocation {
        FragmentLocation {
            node: Some(node),
            fragments: fragment_ids
                .iter()
                .map(|&i| StreamFragment {
                    fragment_id: i,
                    nodes: None,
                    dispatcher: None,
                    downstream_fragment_id: vec![],
                })
                .collect::<Vec<_>>(),
        }
    }

    #[tokio::test]
    async fn test_fragment_management() -> Result<()> {
        let meta_store_ref = Arc::new(MemStore::new());
        let meta_manager = MetaManager::new(
            meta_store_ref.clone(),
            Box::new(MemEpochGenerator::new()),
            IdGeneratorManager::new(meta_store_ref).await,
            Config::default(),
        )
        .await;

        // Add fragments to node 1.
        assert_eq!(meta_manager.load_all_fragments().await.unwrap().len(), 0);
        let location = make_location(
            Node {
                id: 1,
                host: Some(HostAddress {
                    host: "127.0.0.1".to_string(),
                    port: 9527,
                }),
            },
            (0..5).collect(),
        );
        meta_manager.add_fragments_to_node(&location).await?;

        let locations = meta_manager.load_all_fragments().await.unwrap();
        assert_eq!(locations.len(), 1);
        let location = locations.get(0).unwrap();
        assert_eq!(location.get_node().get_id(), 1);
        assert_eq!(location.get_fragments().len(), 5);
        assert_eq!(
            location
                .fragments
                .iter()
                .map(|f| f.fragment_id)
                .collect::<Vec<_>>(),
            (0..5).collect::<Vec<_>>()
        );

        // Add more fragments to same node 1.
        let location = make_location(
            Node {
                id: 1,
                host: Some(HostAddress {
                    host: "127.0.0.1".to_string(),
                    port: 9527,
                }),
            },
            (5..10).collect(),
        );
        assert!(meta_manager.add_fragments_to_node(&location).await.is_ok());

        // Check new fragments added result.
        let locations = meta_manager.load_all_fragments().await.unwrap();
        assert_eq!(locations.len(), 1);
        let location = locations.get(0).unwrap();
        assert_eq!(location.get_node().get_id(), 1);
        assert_eq!(location.get_fragments().len(), 10);
        assert_eq!(
            location
                .fragments
                .iter()
                .map(|f| f.fragment_id)
                .collect::<Vec<_>>(),
            (0..10).collect::<Vec<_>>()
        );

        // Add fragments to another node 2.
        let location = make_location(
            Node {
                id: 2,
                host: Some(HostAddress {
                    host: "127.0.0.1".to_string(),
                    port: 9528,
                }),
            },
            (10..15).collect(),
        );
        assert!(meta_manager.add_fragments_to_node(&location).await.is_ok());
        let locations = meta_manager.load_all_fragments().await.unwrap();
        assert_eq!(locations.len(), 2);
        let location0 = locations.get(0).unwrap();
        let location1 = locations.get(1).unwrap();

        assert_eq!(
            location0
                .fragments
                .iter()
                .chain(location1.fragments.iter())
                .map(|f| f.fragment_id)
                .collect::<HashSet<_>>(),
            HashSet::from_iter(0..15)
        );

        let node = meta_manager.get_fragment_node(0).await.unwrap();
        assert_eq!(node.id, 1);
        let node = meta_manager.get_fragment_node(10).await.unwrap();
        assert_eq!(node.id, 2);

        Ok(())
    }
}
