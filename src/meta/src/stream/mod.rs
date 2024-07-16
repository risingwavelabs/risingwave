// Copyright 2024 RisingWave Labs
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

mod scale;
mod sink;
mod source_manager;
mod stream_graph;
mod stream_manager;
#[cfg(test)]
mod test_fragmenter;
mod test_scale;

use std::collections::HashMap;

use risingwave_common::catalog::TableId;
use risingwave_pb::stream_plan::StreamActor;
use risingwave_pb::stream_service::build_actor_info::SubscriptionIds;
use risingwave_pb::stream_service::BuildActorInfo;
pub use scale::*;
pub use sink::*;
pub use source_manager::*;
pub use stream_graph::*;
pub use stream_manager::*;

pub(crate) fn to_build_actor_info(
    actor: StreamActor,
    subscriptions: &HashMap<TableId, HashMap<u32, u64>>,
    subscription_depend_table_id: TableId,
) -> BuildActorInfo {
    BuildActorInfo {
        actor: Some(actor),
        related_subscriptions: subscriptions
            .get(&subscription_depend_table_id)
            .into_iter()
            .map(|subscriptions| {
                (
                    subscription_depend_table_id.table_id,
                    SubscriptionIds {
                        subscription_ids: subscriptions.keys().cloned().collect(),
                    },
                )
            })
            .collect(),
    }
}
