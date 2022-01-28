use std::collections::HashMap;

use itertools::Itertools;
use risingwave_pb::common::WorkerNode;
use risingwave_pb::meta::ActorLocation;
use risingwave_pb::stream_plan::{StreamActor, StreamNode};

/// [`BarrierActorInfo`] resolves the actor info read from meta store for [`BarrierManager`].
pub struct BarrierActorInfo {
    /// node_id => node
    pub node_map: HashMap<u32, WorkerNode>,

    /// node_id => actors
    pub actor_map: HashMap<u32, Vec<StreamActor>>,
}

impl BarrierActorInfo {
    // TODO: we may resolve this info as graph updating, instead of doing it every time we want to
    // send a barrier
    pub fn resolve(all_actors: impl IntoIterator<Item = ActorLocation>) -> Self {
        let all_actors = all_actors.into_iter().collect_vec();

        let node_map = all_actors
            .iter()
            .map(|location| location.node.as_ref().unwrap())
            .map(|node| (node.id, node.clone()))
            .collect::<HashMap<_, _>>();

        let actor_map = {
            let mut actor_map: HashMap<u32, Vec<_>> = HashMap::new();
            for location in all_actors {
                let node_id = location.node.unwrap().id;
                let actors = actor_map.entry(node_id).or_default();
                actors.extend(location.actors);
            }
            actor_map
        };

        Self {
            node_map,
            actor_map,
        }
    }

    // TODO: should only collect from reachable actors, for mv on mv
    pub fn actor_ids_to_collect(&self, node_id: &u32) -> impl Iterator<Item = u32> {
        let actors = self.actor_map.get(node_id).unwrap().clone();
        actors.into_iter().map(|a| a.actor_id)
    }

    pub fn actor_ids_to_send(&self, node_id: &u32) -> impl Iterator<Item = u32> {
        fn resolve_head_node<'a>(node: &'a StreamNode, head_nodes: &mut Vec<&'a StreamNode>) {
            if node.input.is_empty() {
                head_nodes.push(node);
            } else {
                for node in &node.input {
                    resolve_head_node(node, head_nodes);
                }
            }
        }

        let actors = self.actor_map.get(node_id).unwrap().clone();

        actors
            .into_iter()
            .filter(|actor| {
                let mut head_nodes = vec![];
                resolve_head_node(actor.get_nodes().unwrap(), &mut head_nodes);
                head_nodes.iter().any(|node| {
                    matches!(
                        node.get_node().unwrap(),
                        risingwave_pb::stream_plan::stream_node::Node::SourceNode(_)
                    )
                })
            })
            .map(|a| a.actor_id)
    }
}
