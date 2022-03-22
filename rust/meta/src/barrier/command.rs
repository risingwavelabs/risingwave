// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
use std::collections::{HashMap, HashSet};

use futures::future::try_join_all;
use log::debug;
use risingwave_common::catalog::TableId;
use risingwave_common::error::{Result, RwError, ToRwResult};
use risingwave_pb::common::ActorInfo;
use risingwave_pb::data::barrier::Mutation;
use risingwave_pb::data::{Actors, AddMutation, NothingMutation, StopMutation};
use risingwave_pb::meta::table_fragments::ActorState;
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
    /// `Plain` command generates a barrier with nothing mutation.
    ///
    /// Barriers from all actors marked as `Created` state will be collected.
    /// After the barrier is collected, it does nothing.
    Plain,

    /// `DropMaterializedView` command generates a `Stop` barrier by the given [`TableRefId`]. The
    /// catalog has ensured that this materialized view is safe to be dropped by reference counts
    /// before.
    ///
    /// Barriers from the actors to be dropped will STILL be collected.
    /// After the barrier is collected, it notifies the local stream manager of compute nodes to
    /// drop actors, and then delete the table fragments info from meta store.
    DropMaterializedView(TableRefId),

    /// `CreateMaterializedView` command generates a `Add` barrier by given info.
    ///
    /// Barriers from the actors to be created, which is marked as `Creating` at first, will NOT be
    /// collected.
    /// After the barrier is collected, these newly created actors will be marked as `Created`. And
    /// it adds the table fragments info to meta store.
    CreateMaterializedView {
        table_fragments: TableFragments,
        table_sink_map: HashMap<TableId, Vec<ActorId>>,
        dispatches: HashMap<ActorId, Vec<ActorInfo>>,
    },
}

impl Command {
    pub fn checkpoint() -> Self {
        Self::Plain
    }

    pub fn creating_table_id(&self) -> Option<TableId> {
        match self {
            Command::CreateMaterializedView {
                table_fragments, ..
            } => Some(table_fragments.table_id()),
            _ => None,
        }
    }
}

/// [`CommandContext`] is used for generating barrier and doing post stuffs according to the given
/// [`Command`].
pub struct CommandContext<'a, S> {
    fragment_manager: FragmentManagerRef<S>,

    clients: StreamClientsRef,

    /// Resolved info in this barrier loop.
    // TODO: this could be stale when we are calling `post_collect`, check if it matters
    pub info: &'a BarrierActorInfo,

    pub prev_epoch: u64,
    pub curr_epoch: u64,

    command: Command,
}

impl<'a, S> CommandContext<'a, S> {
    pub fn new(
        fragment_manager: FragmentManagerRef<S>,
        clients: StreamClientsRef,
        info: &'a BarrierActorInfo,
        prev_epoch: u64,
        curr_epoch: u64,
        command: Command,
    ) -> Self {
        Self {
            fragment_manager,
            clients,
            info,
            prev_epoch,
            curr_epoch,
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
            Command::Plain => Mutation::Nothing(NothingMutation {}),

            Command::DropMaterializedView(table_id) => {
                let actors = self
                    .fragment_manager
                    .get_table_actor_ids(&TableId::from(&Some(table_id.clone())))?;
                Mutation::Stop(StopMutation { actors })
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
                    .collect();
                Mutation::Add(AddMutation { actors })
            }
        };

        Ok(mutation)
    }

    pub fn actors_to_finish(&self) -> Result<HashSet<ActorId>> {
        let actors = match &self.command {
            Command::CreateMaterializedView { dispatches, .. } => dispatches
                .iter()
                .flat_map(|(_, down_actor_infos)| down_actor_infos.iter().map(|info| info.actor_id))
                .collect(),

            _ => Default::default(),
        };

        Ok(actors)
    }

    /// Do some stuffs after barriers are collected, for the given command.
    pub async fn post_collect(&self) -> Result<()> {
        match &self.command {
            Command::Plain => {}

            Command::DropMaterializedView(table_ref_id) => {
                // Tell compute nodes to drop actors.
                let table_id = TableId::from(&Some(table_ref_id.clone()));
                let node_actors = self.fragment_manager.table_node_actors(&table_id)?;
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
                table_fragments,
                dispatches,
                table_sink_map,
            } => {
                // TODO: all related updates should be done in one transaction.
                let mut table_fragments = table_fragments.clone();
                table_fragments.update_actors_state(ActorState::Running);
                self.fragment_manager
                    .update_table_fragments(table_fragments)
                    .await?;

                for (table_id, actors) in table_sink_map {
                    let downstream_actors = dispatches
                        .iter()
                        .filter(|(upstream_actor_id, _)| actors.contains(upstream_actor_id))
                        .map(|(upstream_actor_id, downstream_actor_infos)| {
                            (
                                *upstream_actor_id,
                                downstream_actor_infos
                                    .iter()
                                    .map(|info| info.actor_id)
                                    .collect(),
                            )
                        })
                        .collect::<HashMap<ActorId, Vec<ActorId>>>();
                    self.fragment_manager
                        .update_table_fragments_downstream(table_id, &downstream_actors)
                        .await?;
                }
            }
        }

        Ok(())
    }
}
