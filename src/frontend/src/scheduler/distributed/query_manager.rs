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

use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::pin::Pin;
use std::sync::{Arc, RwLock, Weak};
use std::task::{Context, Poll};

use futures::Stream;
use pgwire::pg_server::BoxedError;
use risingwave_batch::worker_manager::worker_node_manager::{
    WorkerNodeManagerRef, WorkerNodeSelector,
};
use risingwave_common::array::DataChunk;
use risingwave_common::session_config::QueryMode;
use risingwave_pb::batch_plan::TaskOutputId;
use risingwave_pb::common::HostAddress;
use risingwave_rpc_client::ComputeClientPoolRef;
use tokio::sync::OwnedSemaphorePermit;

use super::QueryExecution;
use super::stats::DistributedQueryMetrics;
use crate::catalog::catalog_service::CatalogReader;
use crate::scheduler::plan_fragmenter::{Query, QueryId};
use crate::scheduler::{ExecutionContextRef, ReadSnapshot, SchedulerResult};
use crate::session::SessionImpl;

pub struct DistributedQueryStream {
    chunk_rx: tokio::sync::mpsc::Receiver<SchedulerResult<DataChunk>>,
    // Used for cleaning up `QueryExecution` after all data are polled.
    query_id: QueryId,
    query_execution_info: QueryExecutionInfoRef,
    // Avoid a Session -> cursor -> stream -> Session reference cycle.
    session: Weak<SessionImpl>,
}

impl DistributedQueryStream {
    pub fn query_id(&self) -> &QueryId {
        &self.query_id
    }
}

impl Stream for DistributedQueryStream {
    // TODO(error-handling): use a concrete error type.
    type Item = Result<DataChunk, BoxedError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.chunk_rx.poll_recv(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(chunk) => match chunk {
                Some(chunk_result) => match chunk_result {
                    Ok(chunk) => Poll::Ready(Some(Ok(chunk))),
                    Err(err) => Poll::Ready(Some(Err(Box::new(err)))),
                },
                None => Poll::Ready(None),
            },
        }
    }
}

impl Drop for DistributedQueryStream {
    fn drop(&mut self) {
        // Clear `QueryExecution`. Avoid holding it after execution ends.
        self.query_execution_info
            .write()
            .unwrap()
            .delete_query(&self.query_id);
        if let Some(session) = self.session.upgrade() {
            session.unregister_distributed_query(&self.query_id);
        }
    }
}

pub struct QueryResultFetcher {
    task_output_id: TaskOutputId,
    task_host: HostAddress,

    chunk_rx: tokio::sync::mpsc::Receiver<SchedulerResult<DataChunk>>,

    // `query_id` and `query_execution_info` are used for cleaning up `QueryExecution` after
    // execution.
    query_id: QueryId,
    query_execution_info: QueryExecutionInfoRef,
}

/// [`QueryExecutionInfo`] stores necessary information of query executions. Currently, a
/// `QueryExecution` will be removed right after it ends execution. We might add additional fields
/// in the future.
#[derive(Clone, Default)]
pub struct QueryExecutionInfo {
    query_execution_map: HashMap<QueryId, Arc<QueryExecution>>,
}

impl QueryExecutionInfo {
    #[cfg(test)]
    pub fn new_from_map(query_execution_map: HashMap<QueryId, Arc<QueryExecution>>) -> Self {
        Self {
            query_execution_map,
        }
    }
}

pub type QueryExecutionInfoRef = Arc<RwLock<QueryExecutionInfo>>;

/// Owns cleanup of a registered distributed query until its result stream takes ownership.
///
/// Dropping an armed guard removes the execution registration and session ownership record and
/// requests execution cancellation, including when the scheduling future is dropped at an await
/// point. Dropping it requires a Tokio runtime.
struct DistributedQueryRegistrationGuard {
    query_id: QueryId,
    query_execution: Arc<QueryExecution>,
    query_execution_info: QueryExecutionInfoRef,
    session: Weak<SessionImpl>,
    armed: bool,
}

impl DistributedQueryRegistrationGuard {
    /// Arms cleanup for a query already inserted into the execution registry.
    fn new(
        query_id: QueryId,
        query_execution: Arc<QueryExecution>,
        query_execution_info: QueryExecutionInfoRef,
        session: Weak<SessionImpl>,
    ) -> Self {
        Self {
            query_id,
            query_execution,
            query_execution_info,
            session,
            armed: true,
        }
    }

    /// Relinquishes cleanup after the result stream has assumed ownership.
    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for DistributedQueryRegistrationGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        self.query_execution_info
            .write()
            .unwrap()
            .delete_query(&self.query_id);
        if let Some(session) = self.session.upgrade() {
            session.unregister_distributed_query(&self.query_id);
        }
        let query_execution = self.query_execution.clone();
        tokio::spawn(async move {
            query_execution
                .abort("query scheduling was cancelled".to_owned())
                .await;
        });
    }
}

impl QueryExecutionInfo {
    pub fn add_query(&mut self, query_id: QueryId, query_execution: Arc<QueryExecution>) {
        self.query_execution_map.insert(query_id, query_execution);
    }

    pub fn delete_query(&mut self, query_id: &QueryId) {
        self.query_execution_map.remove(query_id);
    }
}

/// Manages execution of distributed batch queries.
#[derive(Clone)]
pub struct QueryManager {
    worker_node_manager: WorkerNodeManagerRef,
    compute_client_pool: ComputeClientPoolRef,
    catalog_reader: CatalogReader,
    query_execution_info: QueryExecutionInfoRef,
    pub query_metrics: Arc<DistributedQueryMetrics>,
    /// Limit per session.
    distributed_query_limit: Option<u64>,
    /// Limits the number of concurrent distributed queries.
    distributed_query_semaphore: Option<Arc<tokio::sync::Semaphore>>,
    /// Total permitted distributed query number.
    pub total_distributed_query_limit: Option<u64>,
}

impl QueryManager {
    pub fn new(
        worker_node_manager: WorkerNodeManagerRef,
        compute_client_pool: ComputeClientPoolRef,
        catalog_reader: CatalogReader,
        query_metrics: Arc<DistributedQueryMetrics>,
        distributed_query_limit: Option<u64>,
        total_distributed_query_limit: Option<u64>,
    ) -> Self {
        let distributed_query_semaphore = total_distributed_query_limit
            .map(|limit| Arc::new(tokio::sync::Semaphore::new(limit as usize)));
        Self {
            worker_node_manager,
            compute_client_pool,
            catalog_reader,
            query_execution_info: Arc::new(RwLock::new(QueryExecutionInfo::default())),
            query_metrics,
            distributed_query_limit,
            distributed_query_semaphore,
            total_distributed_query_limit,
        }
    }

    async fn get_permit(&self) -> SchedulerResult<Option<OwnedSemaphorePermit>> {
        match self.distributed_query_semaphore {
            Some(ref semaphore) => {
                let permit = semaphore.clone().acquire_owned().await;
                match permit {
                    Ok(permit) => Ok(Some(permit)),
                    Err(_) => {
                        self.query_metrics.rejected_query_counter.inc();
                        Err(crate::scheduler::SchedulerError::QueryReachLimit(
                            QueryMode::Distributed,
                            self.total_distributed_query_limit
                                .expect("should have distributed query limit"),
                        ))
                    }
                }
            }
            None => Ok(None),
        }
    }

    /// Uses the supplied snapshot, or pins the current transaction's snapshot when none is
    /// supplied.
    pub async fn schedule(
        &self,
        context: ExecutionContextRef,
        mut query: Query,
        is_cursor_query: bool,
        snapshot: Option<ReadSnapshot>,
    ) -> SchedulerResult<DistributedQueryStream> {
        // TODO: if there's no table scan, we don't need to acquire snapshot.
        let pinned_snapshot = snapshot.unwrap_or_else(|| context.session().pinned_snapshot());
        pinned_snapshot.fill_batch_query_epoch(&mut query)?;

        if let Some(query_limit) = self.distributed_query_limit
            && self.query_metrics.running_query_num.get() as u64 == query_limit
        {
            self.query_metrics.rejected_query_counter.inc();
            return Err(crate::scheduler::SchedulerError::QueryReachLimit(
                QueryMode::Distributed,
                query_limit,
            ));
        }
        let query_id = query.query_id.clone();
        let permit = self.get_permit().await?;
        let query_execution = Arc::new(QueryExecution::new(query, permit));

        self.add_query(query_id.clone(), query_execution.clone());
        context
            .session()
            .register_distributed_query(query_id.clone(), is_cursor_query);
        let session = Arc::downgrade(context.session());
        let mut registration = DistributedQueryRegistrationGuard::new(
            query_id,
            query_execution.clone(),
            self.query_execution_info.clone(),
            session.clone(),
        );

        let worker_node_manager_reader = WorkerNodeSelector::new(
            self.worker_node_manager.clone(),
            pinned_snapshot.support_barrier_read(),
        );

        // Starts the execution of the query.
        let query_result_fetcher = query_execution
            .start(
                context.clone(),
                worker_node_manager_reader,
                self.compute_client_pool.clone(),
                self.catalog_reader.clone(),
                self.query_execution_info.clone(),
                self.query_metrics.clone(),
            )
            .await?;
        let stream = query_result_fetcher.stream_from_channel(session);
        registration.disarm();
        Ok(stream)
    }

    /// Requests cancellation of the query IDs selected by their owners, without applying session
    /// or cursor classification. Queries no longer present in the registry are ignored.
    pub(crate) fn cancel_queries_by_ids(&self, query_ids: &[QueryId], reason: impl Into<String>) {
        let query_executions = {
            let registry = self.query_execution_info.read().unwrap();
            query_ids
                .iter()
                .filter_map(|query_id| registry.query_execution_map.get(query_id).cloned())
                .collect::<Vec<_>>()
        };
        let reason = reason.into();
        for query_execution in query_executions {
            let reason = reason.clone();
            tokio::spawn(async move { query_execution.abort(reason).await });
        }
    }

    pub fn add_query(&self, query_id: QueryId, query_execution: Arc<QueryExecution>) {
        let mut query_execution_info = self.query_execution_info.write().unwrap();
        query_execution_info.add_query(query_id, query_execution);
    }
}

impl QueryResultFetcher {
    pub fn new(
        task_output_id: TaskOutputId,
        task_host: HostAddress,
        chunk_rx: tokio::sync::mpsc::Receiver<SchedulerResult<DataChunk>>,
        query_id: QueryId,
        query_execution_info: QueryExecutionInfoRef,
    ) -> Self {
        Self {
            task_output_id,
            task_host,
            chunk_rx,
            query_id,
            query_execution_info,
        }
    }

    fn stream_from_channel(self, session: Weak<SessionImpl>) -> DistributedQueryStream {
        DistributedQueryStream {
            chunk_rx: self.chunk_rx,
            query_id: self.query_id,
            query_execution_info: self.query_execution_info,
            session,
        }
    }
}

impl Debug for QueryResultFetcher {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueryResultFetcher")
            .field("task_output_id", &self.task_output_id)
            .field("task_host", &self.task_host)
            .finish()
    }
}

#[cfg(test)]
mod cursor_lifecycle_tests {
    use std::time::Duration;

    use tokio::sync::mpsc;

    use super::*;
    use crate::scheduler::ExecutionContext;
    use crate::scheduler::distributed::query::QueryMessage;
    use crate::scheduler::distributed::query::tests::{
        create_query, running_query_execution_with_query_message_receiver,
    };

    impl QueryManager {
        /// Creates a result stream backed by a test-controlled channel and this query registry.
        pub(crate) fn query_stream_for_test(
            &self,
            query_id: QueryId,
            chunk_rx: mpsc::Receiver<SchedulerResult<DataChunk>>,
            session: Weak<SessionImpl>,
        ) -> DistributedQueryStream {
            DistributedQueryStream {
                chunk_rx,
                query_id,
                query_execution_info: self.query_execution_info.clone(),
                session,
            }
        }

        /// Returns whether the query is still present in the execution registry.
        pub(crate) fn contains_query_for_test(&self, query_id: &QueryId) -> bool {
            self.query_execution_info
                .read()
                .unwrap()
                .query_execution_map
                .contains_key(query_id)
        }
    }

    async fn assert_schedule_drops_armed_registration_guard(drop_schedule: bool) {
        for is_cursor_query in [false, true] {
            let session = Arc::new(SessionImpl::mock());
            session
                .set_config("visibility_mode", "all".to_owned())
                .unwrap();
            let _txn = session.txn_begin_implicit();
            let manager = session.env().query_manager().clone();
            let query = create_query().await;
            let query_id = query.query_id().clone();
            let context = Arc::new(ExecutionContext::new(session.clone(), None));
            let mut scheduling = Box::pin(manager.schedule(context, query, is_cursor_query, None));

            // With no semaphore configured, get_permit().await completes immediately.
            // The registration checks below confirm this poll has passed that await;
            // the next await, query_execution.start(...).await, is where it suspends.
            assert!(futures::poll!(scheduling.as_mut()).is_pending());
            assert!(manager.contains_query_for_test(&query_id));
            assert_eq!(session.all_distributed_query_ids(), vec![query_id.clone()]);
            assert_eq!(
                session.ordinary_distributed_query_ids(),
                if is_cursor_query {
                    vec![]
                } else {
                    vec![query_id.clone()]
                }
            );

            if drop_schedule {
                drop(scheduling);
            } else {
                // The mock query manager has no workers to execute the query.
                assert!(scheduling.await.is_err());
            }

            assert!(!manager.contains_query_for_test(&query_id));
            assert!(session.all_distributed_query_ids().is_empty());
        }
    }

    /// Verifies scheduling uses the supplied snapshot without calling `session.pinned_snapshot()`.
    /// The mock session has no transaction, so that fallback would panic. Checking each table
    /// scan's epoch additionally verifies that the supplied snapshot is actually applied.
    #[tokio::test]
    async fn test_schedule_uses_supplied_snapshot_when_provided() {
        use risingwave_common::util::epoch::Epoch;
        use risingwave_pb::batch_plan::plan_node::NodeBody;
        use risingwave_pb::common::batch_query_epoch;

        use crate::scheduler::plan_fragmenter::ExecutionPlanNode;

        fn check_scan_epochs(node: &ExecutionPlanNode, epoch: u64) -> usize {
            let own_scan = if let NodeBody::RowSeqScan(scan) = &node.node {
                assert_eq!(
                    scan.query_epoch.as_ref().unwrap().epoch,
                    Some(batch_query_epoch::Epoch::Backup(epoch))
                );
                1
            } else {
                0
            };
            own_scan
                + node
                    .children
                    .iter()
                    .map(|child| check_scan_epochs(child, epoch))
                    .sum::<usize>()
        }

        let session = Arc::new(SessionImpl::mock());
        // Deliberately do not begin a transaction. Calling session.pinned_snapshot() would panic,
        // so reaching suspended startup also verifies that the fallback was not evaluated.
        let manager = session.env().query_manager().clone();
        let query = create_query().await;
        let query_id = query.query_id().clone();
        let epoch = Epoch(123 << 16);
        let context = Arc::new(ExecutionContext::new(session.clone(), None));
        let mut scheduling =
            Box::pin(manager.schedule(context, query, true, Some(ReadSnapshot::Other(epoch))));
        assert!(futures::poll!(scheduling.as_mut()).is_pending());
        let execution = manager
            .query_execution_info
            .read()
            .unwrap()
            .query_execution_map
            .get(&query_id)
            .unwrap()
            .clone();
        let scan_count: usize = execution
            .query()
            .stage_graph
            .stages
            .values()
            .map(|stage| check_scan_epochs(&stage.root, epoch.0))
            .sum();
        assert!(scan_count > 0);
        assert_eq!(session.all_distributed_query_ids(), vec![query_id.clone()]);
        assert!(session.ordinary_distributed_query_ids().is_empty());

        drop(scheduling);
        assert!(!manager.contains_query_for_test(&query_id));
        assert!(session.all_distributed_query_ids().is_empty());
    }

    async fn registration_guard_with_control_receiver() -> (
        DistributedQueryRegistrationGuard,
        mpsc::Receiver<QueryMessage>,
    ) {
        let query = create_query().await;
        let query_id = query.query_id().clone();
        let (query_execution, control_rx) =
            running_query_execution_with_query_message_receiver(query);
        let registry = Arc::new(RwLock::new(QueryExecutionInfo::new_from_map(
            HashMap::from([(query_id.clone(), query_execution.clone())]),
        )));
        let registration = DistributedQueryRegistrationGuard::new(
            query_id,
            query_execution,
            registry,
            Weak::new(),
        );
        (registration, control_rx)
    }

    /// Verifies that dropping a schedule drops its armed registration guard, removing both
    /// global and session registrations for ordinary and cursor-owned queries.
    #[tokio::test]
    async fn test_dropped_schedule_drops_armed_registration_guard() {
        assert_schedule_drops_armed_registration_guard(true).await;
    }

    /// Verifies that a failed schedule drops its armed registration guard, removing both
    /// global and session registrations for ordinary and cursor-owned queries.
    #[tokio::test]
    async fn test_failed_schedule_drops_armed_registration_guard() {
        assert_schedule_drops_armed_registration_guard(false).await;
    }

    /// Verifies that dropping an armed registration guard requests cancellation of the unfinished
    /// query by sending a cancellation message.
    #[tokio::test]
    async fn test_dropping_armed_registration_guard_requests_cancellation() {
        let (registration, mut control_rx) = registration_guard_with_control_receiver().await;

        drop(registration);

        let message = tokio::time::timeout(Duration::from_secs(1), control_rx.recv())
            .await
            .expect("dropping an armed guard must request query cancellation")
            .expect("query cancellation message must arrive");
        assert!(matches!(
            message,
            QueryMessage::CancelQuery(reason) if reason == "query scheduling was cancelled"
        ));
    }

    /// Verifies that dropping a disarmed registration guard does not send a cancellation message.
    #[tokio::test]
    async fn test_dropping_disarmed_registration_guard_does_not_request_cancellation() {
        let (mut registration, mut control_rx) = registration_guard_with_control_receiver().await;
        let query_execution = registration.query_execution.clone();

        registration.disarm();
        drop(registration);

        assert!(
            tokio::time::timeout(Duration::from_secs(1), control_rx.recv())
                .await
                .is_err(),
            "dropping a disarmed guard must not request query cancellation"
        );
        // Keep the execution alive through the assertion so channel closure cannot end the wait.
        drop(query_execution);
    }
}
