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

//! # Stream Task Management Architecture
//!
//! This module contains the core stream task management system that handles streaming
//! computation actors and barrier coordination in RisingWave.
//!
//! ## Architecture Overview
//!
//! The stream task management system consists of the following layered components:
//!
//! ### External Interface Layer
//! - Meta Service: Central coordination service
//! - [`LocalStreamManager`]: Public API handler for StreamService/ExchangeService
//!
//! ### Core Control Layer
//! - [`LocalBarrierWorker`]: Central event coordinator and barrier processor
//!   - Owns [`ControlStreamHandle`]: Bidirectional communication with Meta Service
//!   - Manages [`ManagedBarrierState`]: Multi-database barrier state coordinator
//!     + [`DatabaseManagedBarrierState`]: Per-database barrier state manager
//!       - Uses [`StreamActorManager`]: Actor factory and lifecycle manager
//!       - Manages [`PartialGraphManagedBarrierState`]: Per-partial-graph barrier coordination
//!
//! ### Actor Execution Layer
//! - Stream Actors: Individual computation units
//! - [`LocalBarrierManager`]: Actor-to-system event bridge
//!
//! ## Key Event Types
//!
//! - [`risingwave_pb::stream_service::streaming_control_stream_request::Request`]: Barrier injection events sent from Meta Service to [`ControlStreamHandle`]
//! - [`LocalActorOperation`]: Meta control events sent from [`LocalStreamManager`] to [`barrier_worker::LocalBarrierWorker`]
//! - [`LocalBarrierEvent`]: Events sent from actors via [`LocalBarrierManager`] to [`barrier_worker::managed_state::DatabaseManagedBarrierState`]
//!
//! ## Data Flow and Event Processing
//!
//! ### 1. Barrier Flow
//! This is the primary coordination mechanism for checkpoints and barriers:
//!
//! 1. Meta Service sends [`risingwave_pb::stream_service::InjectBarrierRequest`] via `streaming_control_stream`
//! 2. [`barrier_worker::ControlStreamHandle`] (owned by [`barrier_worker::LocalBarrierWorker`]) receives the request
//! 3. [`barrier_worker::LocalBarrierWorker`] processes the request and calls [`barrier_worker::LocalBarrierWorker::send_barrier()`]
//!    - [`barrier_worker::managed_state::DatabaseManagedBarrierState::transform_to_issued`] creates new actors if needed
//!    - [`barrier_worker::managed_state::PartialGraphManagedBarrierState::transform_to_issued`] transitions to `Issued` state
//!    - [`barrier_worker::managed_state::InflightActorState::issue_barrier`] sends barriers to individual actors
//! 4. Stream Actors receive barriers and process them
//! 5. Stream Actors finish processing barriers and send [`LocalBarrierEvent::ReportActorCollected`] via [`LocalBarrierManager::collect`]
//! 6. [`barrier_worker::managed_state::DatabaseManagedBarrierState::poll_next_event`] processes the collection via [`DatabaseManagedBarrierState::collect`]
//! 6. [`barrier_worker::LocalBarrierWorker::complete_barrier`] initiates state store sync if needed
//! 7. [`barrier_worker::LocalBarrierWorker::on_epoch_completed`] sends [`risingwave_pb::stream_service::BarrierCompleteResponse`] to Meta Service
//!
//! ### 2. Actor Lifecycle Management
//! How actors are created, managed, and destroyed:
//!
//! 1. Meta Service sends [`risingwave_pb::stream_service::InjectBarrierRequest`] with `actors_to_build`
//! 2. [`barrier_worker::managed_state::DatabaseManagedBarrierState::transform_to_issued`] processes new actors
//! 3. [`StreamActorManager::spawn_actor`] creates and starts actor tasks
//! 4. [`barrier_worker::managed_state::InflightActorState::start`] tracks actor in system
//!
//! ### 3. Error Handling Flow
//! How errors propagate through the system:
//!
//! 1. Stream Actors encounter errors and call [`LocalBarrierManager::notify_failure`]
//! 2. [`LocalBarrierManager`] sends error via `actor_failure_rx`
//! 3. [`barrier_worker::managed_state::DatabaseManagedBarrierState::poll_next_event`] sends `ActorError` event
//! 4. [`barrier_worker::LocalBarrierWorker::on_database_failure`] suspends database and reports to Meta Service
//! 5. Meta Service responds with [`risingwave_pb::stream_service::streaming_control_stream_request::ResetDatabaseRequest`]
//! 6. [`barrier_worker::LocalBarrierWorker::reset_database`] starts database reset process
//! 7. [`barrier_worker::managed_state::SuspendedDatabaseState::reset`] cleans up actors and state

#[expect(unused_imports, reason = "used for doc-link")]
use barrier_worker::managed_state::{
    DatabaseManagedBarrierState, ManagedBarrierState, PartialGraphManagedBarrierState,
};

use crate::executor::exchange::permit::{Receiver, Sender};
mod actor_manager;
mod barrier_manager;
mod barrier_worker;
mod env;
mod stream_manager;

pub use actor_manager::*;
pub use barrier_manager::*;
pub use barrier_worker::*;
pub use env::*;
pub use risingwave_common::id::FragmentId;
pub use stream_manager::*;

pub type ConsumableChannelPair = (Option<Sender>, Option<Receiver>);
pub type ActorId = u32;
pub type DispatcherId = u64;
/// (`upstream_actor_id`, `downstream_actor_id`)
pub type UpDownActorIds = (ActorId, ActorId);
pub type UpDownFragmentIds = (FragmentId, FragmentId);

#[derive(Hash, Eq, PartialEq, Copy, Clone, Debug)]
pub(crate) struct PartialGraphId(u32);

#[cfg(test)]
pub(crate) const TEST_DATABASE_ID: risingwave_common::catalog::DatabaseId =
    risingwave_common::catalog::DatabaseId::new(u32::MAX);

#[cfg(test)]
pub(crate) const TEST_PARTIAL_GRAPH_ID: PartialGraphId = PartialGraphId(u32::MAX);

impl PartialGraphId {
    fn new(id: u32) -> Self {
        Self(id)
    }
}

impl From<PartialGraphId> for u32 {
    fn from(val: PartialGraphId) -> u32 {
        val.0
    }
}
