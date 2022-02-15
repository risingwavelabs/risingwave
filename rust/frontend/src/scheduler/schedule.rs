use std::collections::{HashMap, HashSet};
use std::sync::mpsc::channel;
use std::sync::Arc;

use rand::distributions::{Distribution as RandDistribution, Uniform};
use risingwave_pb::common::WorkerNode;

use crate::optimizer::plan_node::PlanNodeType;
use crate::optimizer::property::Distribution;
use crate::optimizer::PlanRef;
use crate::scheduler::plan_fragmenter::{Query, QueryStageRef, StageId};

type ScheduledStageSender = std::sync::mpsc::Sender<ScheduledStage>;
type ScheduledStageReceiver = std::sync::mpsc::Receiver<ScheduledStage>;
pub(crate) type TaskId = u64;

pub(crate) struct BatchScheduler {
    worker_manager: WorkerNodeManager,
    scheduled_stage_sender: ScheduledStageSender,
    scheduled_stage_receiver: ScheduledStageReceiver,
    scheduled_stages_map: HashMap<StageId, ScheduledStageRef>,
}

pub(crate) struct ScheduledStage {
    pub assignments: HashMap<TaskId, WorkerNode>,
    pub augmented_stage: AugmentedStageRef,
}

impl ScheduledStage {
    pub fn from_augmented_stage(
        augmented_stage: AugmentedStageRef,
        assignments: HashMap<TaskId, WorkerNode>,
    ) -> Self {
        Self {
            assignments,
            augmented_stage,
        }
    }

    pub fn id(&self) -> StageId {
        self.augmented_stage.query_stage.id
    }
}
pub(crate) type ScheduledStageRef = Arc<ScheduledStage>;

pub(crate) struct AugmentedStage {
    pub query_stage: QueryStageRef,
    pub exchange_source: HashMap<StageId, ScheduledStageRef>,
    pub parallelism: u32,
    pub workers: Vec<WorkerNode>,
}

impl AugmentedStage {
    /// Construct augment stage from query stage.
    pub fn new_with_query_stage(
        query_stage: QueryStageRef,
        exchange_source: &HashMap<StageId, ScheduledStageRef>,
        workers: Vec<WorkerNode>,
        parallelism: u32,
    ) -> Self {
        Self {
            query_stage,
            exchange_source: exchange_source.clone(),
            parallelism,
            workers,
        }
    }
}

pub(crate) type AugmentedStageRef = Arc<AugmentedStage>;

impl BatchScheduler {
    /// Used in tests.
    pub fn mock(workers: Vec<WorkerNode>) -> Self {
        let (sender, receiver) = channel();
        Self {
            worker_manager: WorkerNodeManager {
                worker_nodes: workers,
            },
            scheduled_stage_sender: sender,
            scheduled_stage_receiver: receiver,
            scheduled_stages_map: HashMap::new(),
        }
    }

    pub fn get_scheduled_stage_unchecked(&self, stage_id: &StageId) -> &ScheduledStageRef {
        self.scheduled_stages_map.get(stage_id).unwrap()
    }
}

pub(crate) struct WorkerNodeManager {
    worker_nodes: Vec<WorkerNode>,
}

impl WorkerNodeManager {
    pub fn all_nodes(&self) -> &[WorkerNode] {
        &self.worker_nodes
    }

    /// Get a random worker node.
    pub fn next_random(&self) -> &WorkerNode {
        let mut rng = rand::thread_rng();
        let die = Uniform::from(0..self.worker_nodes.len());
        self.worker_nodes.get(die.sample(&mut rng)).unwrap()
    }
}

/// Where the query execution handler to pull results.
pub(crate) struct QueryResultLocation {
    task_id: TaskId,
    worker_node: WorkerNode,
}

impl BatchScheduler {
    /// Given a `Query` (Already split by )
    pub fn schedule(&mut self, query: &Query) -> QueryResultLocation {
        // First schedule all leaf stages.
        for leaf_stage_id in &query.leaf_stages() {
            let stage = query.stage_graph.get_stage_unchecked(leaf_stage_id);
            let child_stages = query.stage_graph.get_child_stages_unchecked(leaf_stage_id);
            self.schedule_stage(stage, child_stages);
        }

        loop {
            let scheduled_stage = self.scheduled_stage_receiver.recv().unwrap();
            let cur_stage_id = scheduled_stage.id();
            self.scheduled_stages_map
                .insert(cur_stage_id, Arc::new(scheduled_stage));

            let parent_ids = query.get_parents(&cur_stage_id);
            for parent_id in parent_ids {
                let stage = query.stage_graph.get_stage_unchecked(parent_id);
                let child_stages = query.stage_graph.get_child_stages_unchecked(parent_id);
                if self.all_child_scheduled(child_stages) {
                    self.schedule_stage(stage, child_stages);
                }
            }

            if cur_stage_id == query.stage_graph.id {
                // All child stages have been scheduled.
                let root_stage = self.scheduled_stages_map.get(&cur_stage_id).unwrap();
                let (task_id, worker_node) = root_stage.assignments.iter().next().unwrap();
                return QueryResultLocation {
                    task_id: *task_id,
                    worker_node: worker_node.clone(),
                };
            }
        }
    }

    /// Get scheduled stages from `stage_ids`.
    fn get_scheduled_stages(
        &self,
        stage_ids: &HashSet<StageId>,
    ) -> HashMap<StageId, ScheduledStageRef> {
        let mut ret = HashMap::new();
        for stage_id in stage_ids {
            ret.insert(
                *stage_id,
                self.scheduled_stages_map.get(stage_id).unwrap().clone(),
            );
        }
        ret
    }

    /// Schedule each query stage (`QueryStageRef` -> `AugmentedStageRef` -> `ScheduledStageRef`)
    /// and write results into channel.
    ///
    /// Calculate available workers, parallelism for each stage.
    fn schedule_stage(
        &mut self,
        query_stage_ref: QueryStageRef,
        child_scheduled_stage: &HashSet<StageId>,
    ) {
        let distribution_schema = query_stage_ref.distribution.clone();
        let mut next_stage_parallelism = 1;
        if distribution_schema != Distribution::Single {
            next_stage_parallelism = self.worker_manager.all_nodes().len();
        }

        let all_nodes = self.worker_manager.all_nodes();
        let scheduled_children = self.get_scheduled_stages(child_scheduled_stage);

        // Determine how many worker nodes for current stage.
        let mut cur_stage_worker_nodes = vec![];
        if scheduled_children.is_empty() {
            // If current plan has scan node, use all workers (the data may be in any of them).
            if Self::include_table_scan(query_stage_ref.root.clone()) {
                cur_stage_worker_nodes = all_nodes.to_vec();
            } else {
                // Otherwise just choose a random worker.
                cur_stage_worker_nodes.push(self.worker_manager.next_random().clone());
            }
        } else {
            let mut use_num_nodes = all_nodes.len();
            for stage in scheduled_children.values() {
                // If distribution is single, one worker.
                if stage.augmented_stage.query_stage.distribution == Distribution::Single {
                    use_num_nodes = 1;
                    break;
                }
            }

            if use_num_nodes == all_nodes.len() {
                cur_stage_worker_nodes = all_nodes.to_vec();
            } else {
                cur_stage_worker_nodes.push(self.worker_manager.next_random().clone());
            }
        }

        self.do_stage_execution(Arc::new(AugmentedStage::new_with_query_stage(
            query_stage_ref,
            &scheduled_children,
            cur_stage_worker_nodes,
            next_stage_parallelism as u32,
        )));
    }

    /// Check whether plan node has a table scan node. If true, the parallelism should be
    /// all the compute nodes.
    fn include_table_scan(plan_node: PlanRef) -> bool {
        if plan_node.node_type() == PlanNodeType::BatchSeqScan {
            return true;
        }

        for child in plan_node.inputs() {
            if Self::include_table_scan(child) {
                return true;
            }
        }

        false
    }

    /// Check whether all child stages are scheduled.
    fn all_child_scheduled(&self, child_stages: &HashSet<StageId>) -> bool {
        for child_stage_id in child_stages {
            if !self.scheduled_stages_map.contains_key(child_stage_id) {
                return false;
            }
        }
        true
    }

    /// Wrap scheduled stages into task and send to compute node for execution.
    /// TODO(Bowen): Introduce Compute Client to do task distribution.
    fn do_stage_execution(&mut self, augmented_stage: AugmentedStageRef) {
        let mut scheduled_tasks = HashMap::new();

        for task_id in 0..augmented_stage.parallelism {
            scheduled_tasks.insert(
                task_id as TaskId,
                augmented_stage.workers[task_id as usize].clone(),
            );
        }

        let scheduled_stage =
            ScheduledStage::from_augmented_stage(augmented_stage, scheduled_tasks);
        self.scheduled_stage_sender.send(scheduled_stage).unwrap();
    }
}
