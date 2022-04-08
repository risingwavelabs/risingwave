use std::collections::HashMap;
use std::mem::swap;
use std::sync::Arc;

use arc_swap::ArcSwap;
use risingwave_common::error::ErrorCode::InternalError;
use risingwave_common::error::Result;
use risingwave_pb::common::HostAddress;
use risingwave_pb::plan::plan_node::NodeBody;
use risingwave_pb::plan::{
    ExchangeNode, ExchangeSource, MergeSortExchangeNode, PlanFragment, PlanNode as PlanNodeProst,
    TaskId as TaskIdProst, TaskOutputId,
};
use risingwave_rpc_client::ComputeClient;
use tokio::spawn;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use uuid::Uuid;

use crate::optimizer::plan_node::PlanNodeType;
use crate::scheduler::execution::stage::StageState::Pending;
use crate::scheduler::execution::QueryMessage;
use crate::scheduler::plan_fragmenter::{ExecutionPlanNode, QueryStageRef, StageId};
use crate::scheduler::schedule::{TaskId, WorkerNodeManagerRef};

// Root stage always has only one task.
pub const ROOT_TASK_ID: u32 = 0;
// Root task has only one output.
pub const ROOT_TASK_OUTPUT_ID: u32 = 0;

enum StageState {
    Pending,
    Started {
        sender: Sender<StageMessage>,
        handle: JoinHandle<Result<()>>,
    },
    Running {
        sender: Sender<StageMessage>,
        handle: JoinHandle<Result<()>>,
    },
    Completed,
    Failed,
}

enum StageMessage {
    Stop,
}

#[derive(Debug)]
pub enum StageEvent {
    Scheduled(StageId),
    Failed(StageId),
    Completed(StageId),
}

#[derive(Clone)]
pub struct TaskStatus {
    task_id: TaskId,

    // None before task is scheduled.
    location: Option<HostAddress>,
}

struct TaskStatusHolder {
    inner: ArcSwap<TaskStatus>,
}

pub struct StageExecution {
    epoch: u64,
    stage: QueryStageRef,
    worker_node_manager: WorkerNodeManagerRef,
    tasks: Arc<HashMap<TaskId, TaskStatusHolder>>,
    state: Arc<RwLock<StageState>>,
    msg_sender: Sender<QueryMessage>,

    /// Children stage executions.
    ///
    /// We use `Vec` here since children's size is usually small.
    children: Vec<Arc<StageExecution>>,
}

struct StageRunner {
    epoch: u64,
    state: Arc<RwLock<StageState>>,
    stage: QueryStageRef,
    worker_node_manager: WorkerNodeManagerRef,
    tasks: Arc<HashMap<TaskId, TaskStatusHolder>>,
    _receiver: Receiver<StageMessage>,
    // Send message to `QueryRunner` to notify stage state change.
    msg_sender: Sender<QueryMessage>,
    children: Vec<Arc<StageExecution>>,
}

impl TaskStatusHolder {
    fn new(task_id: TaskId) -> Self {
        let task_status = TaskStatus {
            task_id,
            location: None,
        };

        Self {
            inner: ArcSwap::new(Arc::new(task_status)),
        }
    }

    fn get_status(&self) -> Arc<TaskStatus> {
        self.inner.load_full()
    }
}

impl StageExecution {
    pub fn new(
        epoch: u64,
        stage: QueryStageRef,
        worker_node_manager: WorkerNodeManagerRef,
        msg_sender: Sender<QueryMessage>,
        children: Vec<Arc<StageExecution>>,
    ) -> Self {
        let tasks = (0..stage.parallelism)
            .into_iter()
            .map(|task_id| (task_id, TaskStatusHolder::new(task_id)))
            .collect();
        Self {
            epoch,
            stage,
            worker_node_manager,
            tasks: Arc::new(tasks),
            state: Arc::new(RwLock::new(Pending)),
            msg_sender,
            children,
        }
    }

    /// Starts execution of this stage, returns error if already started.
    pub async fn start(&self) -> Result<()> {
        let mut s = self.state.write().await;
        match &*s {
            &StageState::Pending => {
                let (sender, receiver) = channel(100);
                let runner = StageRunner {
                    epoch: self.epoch,
                    stage: self.stage.clone(),
                    worker_node_manager: self.worker_node_manager.clone(),
                    tasks: self.tasks.clone(),
                    _receiver: receiver,
                    msg_sender: self.msg_sender.clone(),
                    children: self.children.clone(),
                    state: self.state.clone(),
                };
                let handle = spawn(runner.run());

                *s = StageState::Started { sender, handle };
                Ok(())
            }
            _ => Err(InternalError("Staged already started!".to_string()).into()),
        }
    }

    pub async fn stop(&self) -> Result<()> {
        todo!()
    }

    pub async fn is_scheduled(&self) -> bool {
        let s = self.state.read().await;
        matches!(*s, StageState::Running { .. })
    }

    pub fn get_task_status_unchecked(&self, task_id: TaskId) -> Arc<TaskStatus> {
        self.tasks[&task_id].get_status()
    }

    /// Returns all exchange sources for `output_id`. Each `ExchangeSource` is identified by
    /// producer `TaskId` and `output_id`, since each task may produce output to several channels.
    ///
    /// When this method is called, all tasks should have been scheduled, and their `worker_node`
    /// should have been set.
    fn all_exchange_sources_for(&self, output_id: u32) -> Vec<ExchangeSource> {
        self.tasks
            .iter()
            .map(|(task_id, status_holder)| {
                let task_output_id = TaskOutputId {
                    task_id: Some(TaskIdProst {
                        query_id: self.stage.query_id.id.clone(),
                        stage_id: self.stage.id,
                        task_id: *task_id,
                    }),
                    output_id,
                };

                ExchangeSource {
                    task_output_id: Some(task_output_id),
                    host: Some(status_holder.inner.load_full().location.clone().unwrap()),
                }
            })
            .collect()
    }
}

impl StageRunner {
    async fn run(self) -> Result<()> {
        for id in 0..self.stage.parallelism {
            let task_id = TaskIdProst {
                query_id: self.stage.query_id.id.clone(),
                stage_id: self.stage.id,
                task_id: id,
            };
            self.schedule_task(task_id, self.create_plan_fragment(id))
                .await?;
        }

        {
            // Changing state
            let mut s = self.state.write().await;
            let mut tmp_s = StageState::Failed;
            swap(&mut *s, &mut tmp_s);
            match tmp_s {
                StageState::Started { sender, handle } => {
                    *s = StageState::Running { sender, handle };
                }
                _ => unreachable!(),
            }
        }

        // All tasks scheduled, send `StageScheduled` event to `QueryRunner`.
        self.msg_sender
            .send(QueryMessage::Stage(StageEvent::Scheduled(self.stage.id)))
            .await
            .map_err(|e| {
                InternalError(format!(
                    "Failed to send stage scheduled event: {:?}, reason: {:?}",
                    self.stage.id, e
                ))
            })?;

        Ok(())
    }

    async fn schedule_task(&self, task_id: TaskIdProst, plan_fragment: PlanFragment) -> Result<()> {
        let worker_node = self.worker_node_manager.next_random();
        let compute_client = ComputeClient::new(worker_node.host.as_ref().unwrap().into()).await?;

        let t_id = task_id.task_id;
        compute_client
            .create_task2(task_id, plan_fragment, self.epoch)
            .await?;

        self.tasks[&t_id].inner.store(Arc::new(TaskStatus {
            task_id: t_id,
            location: Some(worker_node.host.unwrap()),
        }));

        Ok(())
    }

    fn create_plan_fragment(&self, task_id: TaskId) -> PlanFragment {
        let plan_node_prost = self.convert_plan_node(&self.stage.root, task_id);
        let exchange_info = self.stage.exchange_info.clone();

        PlanFragment {
            root: Some(plan_node_prost),
            exchange_info: Some(exchange_info),
        }
    }

    fn convert_plan_node(
        &self,
        execution_plan_node: &ExecutionPlanNode,
        task_id: TaskId,
    ) -> PlanNodeProst {
        match execution_plan_node.plan_node_type {
            PlanNodeType::BatchExchange => {
                // Find the stage this exchange node should fetch from and get all exchange sources.
                let exchange_sources = self
                    .children
                    .iter()
                    .find(|child_stage| {
                        child_stage.stage.id == execution_plan_node.stage_id.unwrap()
                    })
                    .map(|child_stage| child_stage.all_exchange_sources_for(task_id))
                    .unwrap();

                match &execution_plan_node.node {
                    NodeBody::Exchange(_exchange_node) => {
                        PlanNodeProst {
                            children: vec![],
                            // TODO: Generate meaningful identify
                            identity: Uuid::new_v4().to_string(),
                            node_body: Some(NodeBody::Exchange(ExchangeNode {
                                sources: exchange_sources,
                                input_schema: execution_plan_node.schema.clone(),
                            })),
                        }
                    }
                    NodeBody::MergeSortExchange(sort_merge_exchange_node) => {
                        PlanNodeProst {
                            children: vec![],
                            // TODO: Generate meaningful identify
                            identity: Uuid::new_v4().to_string(),
                            node_body: Some(NodeBody::MergeSortExchange(MergeSortExchangeNode {
                                exchange_node: Some(ExchangeNode {
                                    sources: exchange_sources,
                                    input_schema: execution_plan_node.schema.clone(),
                                }),
                                column_orders: sort_merge_exchange_node.column_orders.clone(),
                            })),
                        }
                    }
                    _ => unreachable!(),
                }
            }
            _ => {
                let children = execution_plan_node
                    .children
                    .iter()
                    .map(|e| self.convert_plan_node(&*e, task_id))
                    .collect();

                PlanNodeProst {
                    children,
                    // TODO: Generate meaningful identify
                    identity: Uuid::new_v4().to_string(),
                    node_body: Some(execution_plan_node.node.clone()),
                }
            }
        }
    }
}

impl TaskStatus {
    pub fn task_host_unchecked(&self) -> HostAddress {
        self.location.clone().unwrap()
    }
}
