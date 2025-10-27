// Copyright 2025 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use risingwave_pb::meta::table_fragments::actor_status::PbActorState;
use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};

use crate::{ActorId, ConnectorSplits, ExprContext, FragmentId, VnodeBitmap, WorkerId};

#[derive(Clone, Debug, PartialEq, Eq, EnumIter, DeriveActiveEnum, Serialize, Deserialize)]
#[sea_orm(rs_type = "String", db_type = "string(None)")]
pub enum ActorStatus {
    #[sea_orm(string_value = "INACTIVE")]
    Inactive,
    #[sea_orm(string_value = "RUNNING")]
    Running,
}

impl From<PbActorState> for ActorStatus {
    fn from(val: PbActorState) -> Self {
        match val {
            PbActorState::Unspecified => unreachable!(),
            PbActorState::Inactive => ActorStatus::Inactive,
            PbActorState::Running => ActorStatus::Running,
        }
    }
}

impl From<ActorStatus> for PbActorState {
    fn from(val: ActorStatus) -> Self {
        match val {
            ActorStatus::Inactive => PbActorState::Inactive,
            ActorStatus::Running => PbActorState::Running,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActorModel {
    pub actor_id: ActorId,
    pub fragment_id: FragmentId,
    pub status: ActorStatus,
    pub splits: ConnectorSplits,
    pub worker_id: WorkerId,
    pub vnode_bitmap: Option<VnodeBitmap>,
    pub expr_context: ExprContext,
}
