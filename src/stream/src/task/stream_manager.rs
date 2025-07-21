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

use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::Instant;

use futures::FutureExt;
use futures::stream::BoxStream;
use risingwave_common::catalog::DatabaseId;
use risingwave_pb::stream_service::streaming_control_stream_request::InitRequest;
use risingwave_pb::stream_service::{
    StreamingControlStreamRequest, StreamingControlStreamResponse,
};
use tokio::sync::mpsc::{UnboundedSender, unbounded_channel};
use tokio::task::JoinHandle;
use tonic::Status;

use crate::error::StreamResult;
use crate::executor::ActorContextRef;
use crate::executor::exchange::permit::Receiver;
use crate::executor::monitor::StreamingMetrics;
use crate::task::barrier_worker::{
    ControlStreamHandle, EventSender, LocalActorOperation, LocalBarrierWorker,
};
use crate::task::{StreamEnvironment, UpDownActorIds};

#[cfg(test)]
pub static LOCAL_TEST_ADDR: std::sync::LazyLock<risingwave_common::util::addr::HostAddr> =
    std::sync::LazyLock::new(|| "127.0.0.1:2333".parse().unwrap());

pub type ActorHandle = JoinHandle<()>;

pub type AtomicU64Ref = Arc<AtomicU64>;

pub mod await_tree_key {
    /// Await-tree key type for actors.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub struct Actor(pub crate::task::ActorId);

    /// Await-tree key type for barriers.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub struct BarrierAwait {
        pub prev_epoch: u64,
    }
}

/// `LocalStreamManager` directly handles public API for streaming, e.g., `StreamService`, `ExchangeService`.
///
/// Interacts with meta, and sends [`LocalActorOperation`] events to [`LocalBarrierWorker`].
/// Note: barriers are handled by [`ControlStreamHandle`]. The control stream is established in [`Self::handle_new_control_stream`],
/// but the handle lives in [`LocalBarrierWorker`].
///
/// See [`crate::task`] for architecture overview.
#[derive(Clone)]
pub struct LocalStreamManager {
    await_tree_reg: Option<await_tree::Registry>,

    pub env: StreamEnvironment,

    actor_op_tx: EventSender<LocalActorOperation>,
}

/// Report expression evaluation errors to the actor context.
///
/// The struct can be cheaply cloned.
#[derive(Clone)]
pub struct ActorEvalErrorReport {
    pub actor_context: ActorContextRef,
    pub identity: Arc<str>,
}

impl risingwave_expr::expr::EvalErrorReport for ActorEvalErrorReport {
    fn report(&self, err: risingwave_expr::ExprError) {
        self.actor_context.on_compute_error(err, &self.identity);
    }
}

impl LocalStreamManager {
    pub fn new(
        env: StreamEnvironment,
        streaming_metrics: Arc<StreamingMetrics>,
        await_tree_config: Option<await_tree::Config>,
        watermark_epoch: AtomicU64Ref,
    ) -> Self {
        if !env.config().unsafe_enable_strict_consistency {
            // If strict consistency is disabled, should disable storage sanity check.
            // Since this is a special config, we have to check it here.
            risingwave_storage::hummock::utils::disable_sanity_check();
        }

        let await_tree_reg = await_tree_config.clone().map(await_tree::Registry::new);

        let (actor_op_tx, actor_op_rx) = unbounded_channel();

        let _join_handle = LocalBarrierWorker::spawn(
            env.clone(),
            streaming_metrics,
            await_tree_reg.clone(),
            watermark_epoch,
            actor_op_rx,
        );
        Self {
            await_tree_reg,
            env,
            actor_op_tx: EventSender(actor_op_tx),
        }
    }

    /// Get the registry of await-trees.
    pub fn await_tree_reg(&self) -> Option<&await_tree::Registry> {
        self.await_tree_reg.as_ref()
    }

    /// Receive a new control stream request from meta. Notify the barrier worker to reset the CN and use the new control stream
    /// to receive control message from meta
    pub fn handle_new_control_stream(
        &self,
        sender: UnboundedSender<Result<StreamingControlStreamResponse, Status>>,
        request_stream: BoxStream<'static, Result<StreamingControlStreamRequest, Status>>,
        init_request: InitRequest,
    ) {
        self.actor_op_tx
            .send_event(LocalActorOperation::NewControlStream {
                handle: ControlStreamHandle::new(sender, request_stream),
                init_request,
            })
    }

    pub async fn take_receiver(
        &self,
        database_id: DatabaseId,
        term_id: String,
        ids: UpDownActorIds,
    ) -> StreamResult<Receiver> {
        self.actor_op_tx
            .send_and_await(|result_sender| LocalActorOperation::TakeReceiver {
                database_id,
                term_id,
                ids,
                result_sender,
            })
            .await?
    }

    pub async fn inspect_barrier_state(&self) -> StreamResult<String> {
        info!("start inspecting barrier state");
        let start = Instant::now();
        self.actor_op_tx
            .send_and_await(|result_sender| LocalActorOperation::InspectState { result_sender })
            .inspect(|result| {
                info!(
                    ok = result.is_ok(),
                    time = ?start.elapsed(),
                    "finish inspecting barrier state"
                );
            })
            .await
    }

    pub async fn shutdown(&self) -> StreamResult<()> {
        self.actor_op_tx
            .send_and_await(|result_sender| LocalActorOperation::Shutdown { result_sender })
            .await
    }
}

#[cfg(test)]
pub mod test_utils {
    use risingwave_pb::common::{ActorInfo, HostAddress};

    use super::*;

    pub fn helper_make_local_actor(actor_id: u32) -> ActorInfo {
        ActorInfo {
            actor_id,
            host: Some(HostAddress {
                host: LOCAL_TEST_ADDR.host.clone(),
                port: LOCAL_TEST_ADDR.port as i32,
            }),
        }
    }
}
