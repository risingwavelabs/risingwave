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

use std::collections::{HashMap, HashSet};

use futures::future::try_join_all;
use risingwave_common::catalog::TableId;
use risingwave_common::error::{Result, RwError};
use risingwave_common::util::epoch::Epoch;
use risingwave_connector::SplitImpl;
use risingwave_pb::common::ActorInfo;
use risingwave_pb::data::barrier::Mutation;
use risingwave_pb::data::{
    AddMutation, DispatcherMutation, NothingMutation, SourceChangeSplit, StopMutation,
};
use risingwave_pb::stream_service::DropActorsRequest;
use risingwave_rpc_client::StreamClientPoolRef;
use uuid::Uuid;

use super::info::BarrierActorInfo;
use crate::model::{ActorId, DispatcherId, TableFragments};
use crate::storage::MetaStore;
use crate::stream::FragmentManagerRef;

/// [`Command`] is the action of [`crate::barrier::GlobalBarrierManager`]. For different commands,
/// we'll build different barriers to send, and may do different stuffs after the barrier is
/// collected.
#[derive(Debug, Clone)]
pub enum Command {
    /// `Plain` command generates a barrier with the mutation it carries.
    ///
    /// Barriers from all actors marked as `Created` state will be collected.
    /// After the barrier is collected, it does nothing.
    Plain(Mutation),

    /// `DropMaterializedView` command generates a `Stop` barrier by the given [`TableId`]. The
    /// catalog has ensured that this materialized view is safe to be dropped by reference counts
    /// before.
    ///
    /// Barriers from the actors to be dropped will STILL be collected.
    /// After the barrier is collected, it notifies the local stream manager of compute nodes to
    /// drop actors, and then delete the table fragments info from meta store.
    DropMaterializedView(TableId),

    /// `CreateMaterializedView` command generates a `Add` barrier by given info.
    ///
    /// Barriers from the actors to be created, which is marked as `Creating` at first, will NOT be
    /// collected.
    /// After the barrier is collected, these newly created actors will be marked as `Created`. And
    /// it adds the table fragments info to meta store.
    CreateMaterializedView {
        table_fragments: TableFragments,
        table_sink_map: HashMap<TableId, Vec<ActorId>>,
        dispatches: HashMap<(ActorId, DispatcherId), Vec<ActorInfo>>,
        source_state: HashMap<ActorId, Vec<SplitImpl>>,
    },
}

impl Command {
    pub fn checkpoint() -> Self {
        Self::Plain(Mutation::Nothing(NothingMutation {}))
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
pub struct CommandContext<'a, S: MetaStore> {
    fragment_manager: FragmentManagerRef<S>,

    client_pool: StreamClientPoolRef,

    /// Resolved info in this barrier loop.
    // TODO: this could be stale when we are calling `post_collect`, check if it matters
    pub info: &'a BarrierActorInfo,

    pub prev_epoch: &'a Epoch,
    pub curr_epoch: &'a Epoch,

    command: Command,
}

impl<'a, S: MetaStore> CommandContext<'a, S> {
    pub fn new(
        fragment_manager: FragmentManagerRef<S>,
        client_pool: StreamClientPoolRef,
        info: &'a BarrierActorInfo,
        prev_epoch: &'a Epoch,
        curr_epoch: &'a Epoch,
        command: Command,
    ) -> Self {
        Self {
            fragment_manager,
            client_pool,
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
    pub async fn to_mutation(&self) -> Result<Mutation> {
        let mutation = match &self.command {
            Command::Plain(mutation) => mutation.clone(),

            Command::DropMaterializedView(table_id) => {
                let actors = self.fragment_manager.get_table_actor_ids(table_id).await?;
                Mutation::Stop(StopMutation { actors })
            }

            Command::CreateMaterializedView {
                dispatches,
                source_state,
                ..
            } => {
                let mutations = dispatches
                    .iter()
                    .map(
                        |(&(up_actor_id, dispatcher_id), down_actor_infos)| DispatcherMutation {
                            actor_id: up_actor_id,
                            dispatcher_id,
                            info: down_actor_infos.to_vec(),
                        },
                    )
                    .collect();

                let splits = source_state
                    .iter()
                    .filter(|(_, splits)| !splits.is_empty())
                    .map(|(actor_id, splits)| {
                        let split_type = splits.iter().next().unwrap().get_type();
                        SourceChangeSplit {
                            actor_id: *actor_id,
                            split_type,
                            source_splits: splits
                                .iter()
                                .map(|split| split.to_json_bytes().to_vec())
                                .collect(),
                        }
                    })
                    .collect();

                Mutation::Add(AddMutation { mutations, splits })
            }
        };

        Ok(mutation)
    }

    /// For `CreateMaterializedView`, returns the actors of the `Chain` nodes. For other commands,
    /// returns an empty set.
    pub fn actors_to_track(&self) -> HashSet<ActorId> {
        match &self.command {
            Command::CreateMaterializedView { dispatches, .. } => dispatches
                .iter()
                .flat_map(|(_, down_actor_infos)| down_actor_infos.iter().map(|info| info.actor_id))
                .collect(),

            _ => Default::default(),
        }
    }

    /// Do some stuffs after barriers are collected, for the given command.
    pub async fn post_collect(&self) -> Result<()> {
        match &self.command {
            Command::Plain(_) => {}

            Command::DropMaterializedView(table_id) => {
                // Tell compute nodes to drop actors.
                let node_actors = self.fragment_manager.table_node_actors(table_id).await?;
                let futures = node_actors.iter().map(|(node_id, actors)| {
                    let node = self.info.node_map.get(node_id).unwrap();
                    let request_id = Uuid::new_v4().to_string();

                    async move {
                        let mut client = self.client_pool.get(node).await?;
                        let request = DropActorsRequest {
                            request_id,
                            actor_ids: actors.to_owned(),
                        };
                        client.drop_actors(request).await?;

                        Ok::<_, RwError>(())
                    }
                });

                try_join_all(futures).await?;

                // Drop fragment info in meta store.
                self.fragment_manager.drop_table_fragments(table_id).await?;
            }

            Command::CreateMaterializedView {
                table_fragments,
                dispatches,
                table_sink_map,
                source_state: _,
            } => {
                let mut dependent_table_actors = Vec::with_capacity(table_sink_map.len());
                for (table_id, actors) in table_sink_map {
                    let downstream_actors = dispatches
                        .iter()
                        .filter(|((upstream_actor_id, _), _)| actors.contains(upstream_actor_id))
                        .map(|((upstream_actor_id, _), downstream_actor_infos)| {
                            (
                                *upstream_actor_id,
                                downstream_actor_infos
                                    .iter()
                                    .map(|info| info.actor_id)
                                    .collect(),
                            )
                        })
                        .collect::<HashMap<ActorId, Vec<ActorId>>>();
                    dependent_table_actors.push((*table_id, downstream_actors));
                }
                self.fragment_manager
                    .finish_create_table_fragments(
                        &table_fragments.table_id(),
                        &dependent_table_actors,
                    )
                    .await?;
            }
        }

        Ok(())
    }
}
