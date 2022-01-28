use std::collections::HashSet;

use futures::future::try_join_all;
use itertools::Itertools;
use log::debug;
use risingwave_common::array::RwError;
use risingwave_common::error::{Result, ToRwResult};
use risingwave_pb::data::barrier::Mutation;
use risingwave_pb::data::{NothingMutation, StopMutation};
use risingwave_pb::plan::TableRefId;
use risingwave_pb::stream_service::DropActorsRequest;
use uuid::Uuid;

use super::info::BarrierActorInfo;
use crate::manager::StreamClientsRef;
use crate::stream::StreamMetaManagerRef;

/// [`Command`] is the action of [`BarrierManager`]. For different commands, we'll build different
/// barriers to send, and may do different stuffs after the barrier is collected.
#[derive(Debug, Clone)]
pub enum Command {
    /// `Plain` command generates a barrier with the mutation it carries.
    /// After the barrier is collected, it does nothing.
    Plain(Mutation),

    /// `DropMaterializedView` command generates a `Stop` barrier by the given [`TableRefId`].
    /// After the barrier is collected, it notifies the local stream manager of compute nodes to
    /// drop actors, and then delete the info from meta store.
    DropMaterializedView(TableRefId),
}

impl Command {
    pub fn checkpoint() -> Self {
        Self::Plain(Mutation::Nothing(NothingMutation {}))
    }
}

/// [`CommandContext`] is used for generating barrier and doing post stuffs according to the given
/// [`Command`].
pub struct CommandContext<'a> {
    stream_meta_manager: StreamMetaManagerRef,

    clients: StreamClientsRef,

    /// Resolved info in this barrier loop.
    // TODO: this could be stale when we are calling `post_collect`, check if it matters
    info: &'a BarrierActorInfo,

    command: Command,
}

impl<'a> CommandContext<'a> {
    pub fn new(
        stream_meta_manager: StreamMetaManagerRef,
        clients: StreamClientsRef,
        info: &'a BarrierActorInfo,
        command: Command,
    ) -> Self {
        Self {
            stream_meta_manager,
            clients,
            info,
            command,
        }
    }
}

impl CommandContext<'_> {
    /// Generate a mutation for the given command.
    pub async fn to_mutation(&self) -> Result<Mutation> {
        let mutation = match &self.command {
            Command::Plain(mutation) => mutation.clone(),

            Command::DropMaterializedView(table_id) => {
                let table_actors = self.stream_meta_manager.get_table_actors(table_id).await?;
                Mutation::Stop(StopMutation {
                    actors: table_actors.actor_ids,
                })
            }
        };

        Ok(mutation)
    }

    /// Do some stuffs after barriers are collected, for the given command.
    pub async fn post_collect(&self) -> Result<()> {
        match &self.command {
            Command::Plain(_) => {}

            Command::DropMaterializedView(table_id) => {
                let table_actors = self.stream_meta_manager.get_table_actors(table_id).await?;
                let actor_ids: HashSet<_> = table_actors.actor_ids.into_iter().collect();

                // Tell compute nodes to drop actors.
                let futures = self.info.node_map.iter().map(|(node_id, node)| {
                    let node_actor_ids = self
                        .info
                        .actor_map
                        .get(node_id)
                        .unwrap()
                        .iter()
                        .map(|a| a.actor_id)
                        .filter(|id| actor_ids.contains(id))
                        .collect_vec();
                    let request_id = Uuid::new_v4().to_string();

                    async move {
                        let mut client = self.clients.get(node).await?;

                        debug!("[{}]drop actors: {:?}", request_id, node_actor_ids);
                        let request = DropActorsRequest {
                            request_id,
                            table_ref_id: Some(table_id.clone()),
                            actor_ids: node_actor_ids,
                        };
                        client.drop_actors(request).await.to_rw_result()?;

                        Ok::<_, RwError>(())
                    }
                });

                try_join_all(futures).await?;

                // Drop actor info in meta store.
                self.stream_meta_manager.drop_table_actors(table_id).await?;
            }
        }

        Ok(())
    }
}
