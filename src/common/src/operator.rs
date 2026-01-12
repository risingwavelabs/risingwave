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

use risingwave_common::id::{ActorId, FragmentId};
use risingwave_pb::id::{ExecutorId, GlobalOperatorId, LocalOperatorId};

/// Generate a globally unique operator id.
pub fn unique_operator_id(
    fragment_id: FragmentId,
    operator_id: impl Into<LocalOperatorId>,
) -> GlobalOperatorId {
    (((fragment_id.as_raw_id() as u64) << 32) + operator_id.into().as_raw_id() as u64).into()
}

/// Generate a globally unique executor id.
pub fn unique_executor_id(
    actor_id: ActorId,
    operator_id: impl Into<LocalOperatorId>,
) -> ExecutorId {
    (((actor_id.as_raw_id() as u64) << 32) + operator_id.into().as_raw_id() as u64).into()
}

/// Decompose a unique executor id into actor id and operator id.
pub fn unique_executor_id_into_parts(unique_executor_id: ExecutorId) -> (ActorId, LocalOperatorId) {
    let actor_id = (unique_executor_id.as_raw_id() >> 32) as u32;
    let operator_id = (unique_executor_id.as_raw_id() & 0xFFFFFFFF) as u32;
    (actor_id.into(), operator_id.into())
}

pub fn unique_operator_id_into_parts(
    unique_operator_id: GlobalOperatorId,
) -> (FragmentId, LocalOperatorId) {
    let fragment_id = (unique_operator_id.as_raw_id() >> 32) as u32;
    let operator_id = (unique_operator_id.as_raw_id() & 0xFFFFFFFF) as u32;
    (fragment_id.into(), operator_id.into())
}

pub fn unique_executor_id_from_unique_operator_id(
    actor_id: ActorId,
    unique_operator_id: GlobalOperatorId,
) -> ExecutorId {
    let (_, operator_id) = unique_operator_id_into_parts(unique_operator_id);
    unique_executor_id(actor_id, operator_id)
}
