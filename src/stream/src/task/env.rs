// Copyright 2022 RisingWave Labs
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

use std::fmt::{Debug, Formatter};
use std::num::NonZeroU64;
use std::sync::Arc;
use std::time::Duration;

use hytra::TrAdder;
use risingwave_common::config::StreamingConfig;
pub(crate) use risingwave_common::id::WorkerId as WorkerNodeId;
use risingwave_common::system_param::local_manager::LocalSystemParamsManagerRef;
use risingwave_common::util::addr::HostAddr;
use risingwave_connector::source::monitor::SourceMetrics;
use risingwave_dml::dml_manager::DmlManagerRef;
use risingwave_rpc_client::{ComputeClientPoolRef, MetaClient};
use risingwave_storage::StateStoreImpl;
use tokio::sync::Semaphore;
use tokio::time::{Instant, Interval, MissedTickBehavior};

/// A cancellation-safe FIFO pacer for remote input subscriptions.
///
/// Waiters queue on the semaphore instead of reserving future time slots, so dropping actors does
/// not leave rate-limit debt for the next recovery attempt.
struct RemoteInputSubscriptionRateLimiter {
    tokens: Arc<Semaphore>,
    rate: NonZeroU64,
}

impl RemoteInputSubscriptionRateLimiter {
    const MAX_BURST: usize = 1;

    fn new(rate: u64) -> Option<Arc<Self>> {
        NonZeroU64::new(rate).map(|rate| {
            let rate_limiter = Arc::new(Self {
                tokens: Arc::new(Semaphore::new(Self::MAX_BURST)),
                rate,
            });
            Self::spawn_token_producer(&rate_limiter.tokens, rate);
            rate_limiter
        })
    }

    async fn wait(&self) {
        self.tokens
            .acquire()
            .await
            .expect("remote input subscription rate limiter is never closed")
            .forget();
    }

    fn spawn_token_producer(tokens: &Arc<Semaphore>, rate: NonZeroU64) {
        let tokens = Arc::downgrade(tokens);
        let mut interval = Self::token_interval(rate);
        tokio::spawn(async move {
            loop {
                interval.tick().await;
                let Some(tokens) = tokens.upgrade() else {
                    return;
                };
                Self::replenish_token(&tokens);
            }
        });
    }

    fn replenish_token(tokens: &Semaphore) {
        if tokens.available_permits() < Self::MAX_BURST {
            tokens.add_permits(1);
        }
    }

    fn token_interval(rate: NonZeroU64) -> Interval {
        let nanos_per_token = (Duration::from_secs(1).as_nanos() as u64 / rate.get()).max(1);
        let period = Duration::from_nanos(nanos_per_token);
        let mut interval = tokio::time::interval_at(Instant::now() + period, period);
        // Do not catch up missed ticks after the runtime stalls, which would create a burst.
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
        interval
    }
}

impl Debug for RemoteInputSubscriptionRateLimiter {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("RemoteInputSubscriptionRateLimiter")
            .field(&self.rate)
            .finish()
    }
}

/// The global environment for task execution.
/// The instance will be shared by every task.
#[derive(Clone, Debug)]
pub struct StreamEnvironment {
    /// Endpoint the stream manager listens on.
    server_addr: HostAddr,

    /// Streaming related configurations.
    ///
    /// This is the global config for the whole compute node. Actor may have its config overridden.
    /// In executor, use `actor_context.config` instead.
    global_config: Arc<StreamingConfig>,

    /// Current worker node id.
    worker_id: WorkerNodeId,

    /// State store for table scanning.
    state_store: StateStoreImpl,

    /// Manages dml information.
    dml_manager: DmlManagerRef,

    /// Read the latest system parameters.
    system_params_manager: LocalSystemParamsManagerRef,

    /// Metrics for source.
    source_metrics: Arc<SourceMetrics>,

    /// Total memory usage in stream.
    total_mem_val: Arc<TrAdder<i64>>,

    /// Meta client. Use `None` for test only
    meta_client: Option<MetaClient>,

    /// Compute client pool for streaming gRPC exchange.
    client_pool: ComputeClientPoolRef,

    /// Smooths remote input subscription bursts without limiting in-flight requests.
    remote_input_subscription_rate_limiter: Option<Arc<RemoteInputSubscriptionRateLimiter>>,

    /// Semaphore to limit the number of kv log store readers concurrently reading historical data.
    /// `None` means unlimited.
    kv_log_store_historical_read_semaphore: Option<Arc<Semaphore>>,
}

impl StreamEnvironment {
    #[expect(clippy::too_many_arguments)]
    pub fn new(
        server_addr: HostAddr,
        global_config: Arc<StreamingConfig>,
        worker_id: WorkerNodeId,
        state_store: StateStoreImpl,
        dml_manager: DmlManagerRef,
        system_params_manager: LocalSystemParamsManagerRef,
        source_metrics: Arc<SourceMetrics>,
        meta_client: MetaClient,
        client_pool: ComputeClientPoolRef,
    ) -> Self {
        let kv_log_store_historical_read_semaphore = {
            let max = global_config
                .developer
                .max_concurrent_kv_log_store_historical_read;
            if max > 0 {
                Some(Arc::new(Semaphore::new(max)))
            } else {
                None
            }
        };
        let remote_input_subscription_rate_limiter = RemoteInputSubscriptionRateLimiter::new(
            global_config.developer.remote_input_subscription_rate_limit,
        );
        StreamEnvironment {
            server_addr,
            global_config,
            worker_id,
            state_store,
            dml_manager,
            system_params_manager,
            source_metrics,
            total_mem_val: Arc::new(TrAdder::new()),
            meta_client: Some(meta_client),
            client_pool,
            remote_input_subscription_rate_limiter,
            kv_log_store_historical_read_semaphore,
        }
    }

    // Create an instance for testing purpose.
    pub fn for_test() -> Self {
        use risingwave_common::system_param::local_manager::LocalSystemParamsManager;
        use risingwave_dml::dml_manager::DmlManager;
        use risingwave_rpc_client::ComputeClientPool;
        use risingwave_storage::monitor::MonitoredStorageMetrics;
        StreamEnvironment {
            server_addr: "127.0.0.1:2333".parse().unwrap(),
            global_config: Arc::new(StreamingConfig::default()),
            worker_id: WorkerNodeId::default(),
            state_store: StateStoreImpl::shared_in_memory_store(Arc::new(
                MonitoredStorageMetrics::unused(),
            )),
            dml_manager: Arc::new(DmlManager::for_test()),
            system_params_manager: Arc::new(LocalSystemParamsManager::for_test()),
            source_metrics: Arc::new(SourceMetrics::default()),
            total_mem_val: Arc::new(TrAdder::new()),
            meta_client: None,
            client_pool: Arc::new(ComputeClientPool::for_test()),
            remote_input_subscription_rate_limiter: None,
            kv_log_store_historical_read_semaphore: None,
        }
    }

    pub fn server_address(&self) -> &HostAddr {
        &self.server_addr
    }

    pub fn global_config(&self) -> &Arc<StreamingConfig> {
        &self.global_config
    }

    pub fn worker_id(&self) -> WorkerNodeId {
        self.worker_id
    }

    pub fn state_store(&self) -> StateStoreImpl {
        self.state_store.clone()
    }

    pub fn dml_manager_ref(&self) -> DmlManagerRef {
        self.dml_manager.clone()
    }

    pub fn system_params_manager_ref(&self) -> LocalSystemParamsManagerRef {
        self.system_params_manager.clone()
    }

    pub fn source_metrics(&self) -> Arc<SourceMetrics> {
        self.source_metrics.clone()
    }

    pub fn total_mem_usage(&self) -> Arc<TrAdder<i64>> {
        self.total_mem_val.clone()
    }

    pub fn meta_client(&self) -> Option<MetaClient> {
        self.meta_client.clone()
    }

    pub fn client_pool(&self) -> ComputeClientPoolRef {
        self.client_pool.clone()
    }

    pub async fn wait_remote_input_subscription(&self) {
        if let Some(rate_limiter) = &self.remote_input_subscription_rate_limiter {
            rate_limiter.wait().await;
        }
    }

    pub fn kv_log_store_historical_read_semaphore(&self) -> Option<Arc<Semaphore>> {
        self.kv_log_store_historical_read_semaphore.clone()
    }
}

#[cfg(test)]
mod tests {
    use tokio::sync::{mpsc, oneshot};
    use tokio::task::JoinHandle;

    use super::*;

    fn rate_limiter_without_producer() -> Arc<RemoteInputSubscriptionRateLimiter> {
        Arc::new(RemoteInputSubscriptionRateLimiter {
            tokens: Arc::new(Semaphore::new(0)),
            rate: NonZeroU64::new(1).unwrap(),
        })
    }

    async fn enqueue_waiter(
        rate_limiter: Arc<RemoteInputSubscriptionRateLimiter>,
        id: usize,
        completed_tx: mpsc::UnboundedSender<usize>,
    ) -> JoinHandle<()> {
        let (ready_tx, ready_rx) = oneshot::channel();
        let handle = tokio::spawn(async move {
            ready_tx.send(()).unwrap();
            rate_limiter.wait().await;
            completed_tx.send(id).unwrap();
        });
        ready_rx.await.unwrap();
        handle
    }

    async fn next_completed(completed_rx: &mut mpsc::UnboundedReceiver<usize>) -> usize {
        tokio::time::timeout(Duration::from_secs(1), completed_rx.recv())
            .await
            .expect("waiter should receive a token")
            .expect("completion channel should stay open")
    }

    #[tokio::test]
    async fn test_remote_input_subscription_rate_limiter_is_fifo() {
        let rate_limiter = rate_limiter_without_producer();
        let (completed_tx, mut completed_rx) = mpsc::unbounded_channel();
        let mut waiters = Vec::new();
        for id in 0..3 {
            waiters.push(enqueue_waiter(rate_limiter.clone(), id, completed_tx.clone()).await);
        }

        for id in 0..3 {
            rate_limiter.tokens.add_permits(1);
            assert_eq!(next_completed(&mut completed_rx).await, id);
        }
        for waiter in waiters {
            waiter.await.unwrap();
        }
    }

    #[tokio::test]
    async fn test_cancelled_waiters_leave_no_rate_limit_debt() {
        let rate_limiter = rate_limiter_without_producer();
        let (completed_tx, mut completed_rx) = mpsc::unbounded_channel();
        let mut stale_waiters = Vec::new();
        for id in 0..16 {
            stale_waiters
                .push(enqueue_waiter(rate_limiter.clone(), id, completed_tx.clone()).await);
        }
        for waiter in stale_waiters {
            waiter.abort();
            assert!(waiter.await.unwrap_err().is_cancelled());
        }

        let fresh_waiter = enqueue_waiter(rate_limiter.clone(), 16, completed_tx).await;
        rate_limiter.tokens.add_permits(1);
        assert_eq!(next_completed(&mut completed_rx).await, 16);
        fresh_waiter.await.unwrap();
    }

    #[tokio::test]
    async fn test_token_producer_has_bounded_burst_and_delays_missed_ticks() {
        let tokens = Semaphore::new(0);
        for _ in 0..10 {
            RemoteInputSubscriptionRateLimiter::replenish_token(&tokens);
        }
        assert_eq!(tokens.available_permits(), 1);

        let interval =
            RemoteInputSubscriptionRateLimiter::token_interval(NonZeroU64::new(256).unwrap());
        assert_eq!(interval.missed_tick_behavior(), MissedTickBehavior::Delay);
    }
}
