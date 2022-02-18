use std::collections::HashMap;

use futures::future::try_join_all;
use log::debug;
use risingwave_common::array::RwError;
use risingwave_common::catalog::TableId;
use risingwave_common::error::{Result, ToRwResult};
use risingwave_pb::common::ActorInfo;
use risingwave_pb::data::barrier::Mutation;
use risingwave_pb::data::{Actors, AddMutation, NothingMutation, StopMutation};
use risingwave_pb::meta::table_fragments::State;
use risingwave_pb::plan::TableRefId;
use risingwave_pb::stream_service::DropActorsRequest;
use uuid::Uuid;

use super::info::BarrierActorInfo;
use crate::manager::StreamClientsRef;
use crate::model::{ActorId, TableFragments};
use crate::storage::MetaStore;
use crate::stream::FragmentManagerRef;

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

    CreateMaterializedView {
        table_fragments: TableFragments,
        dispatches: HashMap<ActorId, Vec<ActorInfo>>,
    },
}

impl Command {
    pub fn checkpoint() -> Self {
        Self::Plain(Mutation::Nothing(NothingMutation {}))
    }
}

/// [`CommandContext`] is used for generating barrier and doing post stuffs according to the given
/// [`Command`].
pub struct CommandContext<'a, S> {
    fragment_manager: FragmentManagerRef<S>,

    clients: StreamClientsRef,

    /// Resolved info in this barrier loop.
    // TODO: this could be stale when we are calling `post_collect`, check if it matters
    info: &'a BarrierActorInfo,

    command: Command,
}

impl<'a, S> CommandContext<'a, S> {
    pub fn new(
        fragment_manager: FragmentManagerRef<S>,
        clients: StreamClientsRef,
        info: &'a BarrierActorInfo,
        command: Command,
    ) -> Self {
        Self {
            fragment_manager,
            clients,
            info,
            command,
        }
    }
}

impl<S> CommandContext<'_, S>
where
    S: MetaStore,
{
    /// Generate a mutation for the given command.
    pub fn to_mutation(&self) -> Result<Mutation> {
        let mutation = match &self.command {
            Command::Plain(mutation) => mutation.clone(),

            Command::DropMaterializedView(table_id) => {
                let table_actors = self
                    .fragment_manager
                    .get_table_actor_ids(&TableId::from(&Some(table_id.clone())))?;
                Mutation::Stop(StopMutation {
                    actors: table_actors,
                })
            }

            Command::CreateMaterializedView { dispatches, .. } => {
                let actors = dispatches
                    .iter()
                    .map(|(&up_actor_id, down_actor_infos)| {
                        (
                            up_actor_id,
                            Actors {
                                info: down_actor_infos.to_vec(),
                            },
                        )
                    })
                    .collect::<HashMap<_, _>>();
                Mutation::Add(AddMutation { actors })
            }
        };

        Ok(mutation)
    }

    /// Do some stuffs after barriers are collected, for the given command.
    pub async fn post_collect(&self) -> Result<()> {
        match &self.command {
            Command::Plain(_) => {}

            Command::DropMaterializedView(table_ref_id) => {
                // Tell compute nodes to drop actors.
                let table_id = TableId::from(&Some(table_ref_id.clone()));
                let node_actors = self.fragment_manager.get_table_node_actors(&table_id)?;
                let futures = node_actors.iter().map(|(node_id, actors)| {
                    let node = self.info.node_map.get(node_id).unwrap();
                    let request_id = Uuid::new_v4().to_string();

                    async move {
                        let mut client = self.clients.get(node).await?;

                        debug!("[{}]drop actors: {:?}", request_id, actors.clone());
                        let request = DropActorsRequest {
                            request_id,
                            table_ref_id: Some(table_ref_id.to_owned()),
                            actor_ids: actors.to_owned(),
                        };
                        client.drop_actors(request).await.to_rw_result()?;

                        Ok::<_, RwError>(())
                    }
                });

                try_join_all(futures).await?;

                // Drop fragment info in meta store.
                self.fragment_manager
                    .drop_table_fragments(&table_id)
                    .await?;
            }

            Command::CreateMaterializedView {
                table_fragments, ..
            } => {
                let mut table_fragments = table_fragments.clone();
                table_fragments.update_state(State::Created);
                self.fragment_manager
                    .update_table_fragments(table_fragments)
                    .await?;
            }
        }

        Ok(())
    }
}
