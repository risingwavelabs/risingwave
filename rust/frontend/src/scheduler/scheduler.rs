use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use itertools::Itertools;
use rand::distributions::{Distribution as RandDistribution, Uniform};
use std::sync::mpsc::channel;
use tonic::Status;

use crate::optimizer::plan_node::{BatchSeqScan, PlanNodeType};
use crate::optimizer::property::Distribution;
use crate::optimizer::PlanRef;
use crate::scheduler::plan_fragmenter::{AugmentedStage, Query, QueryStage, QueryStageRef, ScheduledStage, ScheduledStageRef, StageId, AugmentedQueryStageRef, TaskId};
use risingwave_pb::common::WorkerNode;

type ScheduledStageSender =
    std::sync::mpsc::Sender<std::result::Result<ScheduledStage, Status>>;
type ScheduledStageReceiver =
    std::sync::mpsc::Receiver<std::result::Result<ScheduledStage, Status>>;

pub(crate) struct Scheduler {
    worker_manager: WorkerNodeManager,
    scheduled_stage_sender: ScheduledStageSender,
    scheduled_stage_receiver: ScheduledStageReceiver,
    scheduled_stages_map: HashMap<StageId, ScheduledStageRef>,
}

impl Scheduler {
    /// Used in tests.
    pub fn mock(workers: Vec<WorkerNode>) -> Self {
        let (sender, receiver) = channel();
        Self {
            worker_manager: WorkerNodeManager {worker_nodes: workers},
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

    pub fn next_random(&self) -> &WorkerNode {
        let mut rng = rand::thread_rng();
        let die = Uniform::from(0..self.worker_nodes.len());
        self.worker_nodes.get(die.sample(&mut rng)).unwrap()
    }
}

pub(crate) struct QueryResultLocation;

impl Scheduler {
    pub fn augment(&mut self, query: &Query) -> QueryResultLocation {
        // First augment all leaf stages.
        for leaf_stage_id in &query.leaf_stages() {
            let stage = query.stage_graph.get_stage_unchecked(leaf_stage_id);
            let child_stages = query.stage_graph.get_child_stages_unchecked(leaf_stage_id);
            self.augment_stage(stage, child_stages);
        }

        loop {
            let scheduled_stage = self.scheduled_stage_receiver.recv().unwrap().unwrap();
            let cur_stage_id = scheduled_stage.id;
            self.scheduled_stages_map
                .insert(scheduled_stage.id, Arc::new(scheduled_stage));

            let parent_ids = query.get_parents(&cur_stage_id);
            for parent_id in parent_ids {
                let stage = query.stage_graph.get_stage_unchecked(parent_id);
                let child_stages = query.stage_graph.get_child_stages_unchecked(parent_id);
                if self.all_child_scheduled(*parent_id, child_stages) {
                    self.augment_stage(stage, child_stages);
                }
            }

            if cur_stage_id == query.stage_graph.id {
                // All child stages have been scheduled.
                let root_stage = self.scheduled_stages_map.get(&cur_stage_id).unwrap();
                let (task_id, worker_node) = root_stage.assignments.iter().next().unwrap();
                return QueryResultLocation {};
            }
        }
    }

    fn get_scheduled_stage(
        &self,
        child_stage_ids: &HashSet<StageId>,
    ) -> HashMap<StageId, ScheduledStageRef> {
        let mut ret = HashMap::new();
        for stage_id in child_stage_ids {
            ret.insert(
                *stage_id,
                self.scheduled_stages_map.get(stage_id).unwrap().clone(),
            );
        }
        ret
    }

    /// schedule each stage and wirte results into `scheduled_stage_sender`.
    fn augment_stage(
        &mut self,
        query_stage_ref: QueryStageRef,
        child_scheduled_stage: &HashSet<StageId>,
    ) {
        // Augment stage info (exchange source) and send result (scheduled stage) into channel.
        let distribution_schema = query_stage_ref.distribution.clone();
        let mut next_stage_parallelism = 1;
        if distribution_schema != Distribution::Single {
            next_stage_parallelism = self.worker_manager.all_nodes().len();
        }

        let all_nodes = self.worker_manager.all_nodes();
        let child_scheduled_stage = self.get_scheduled_stage(child_scheduled_stage);
        let mut cur_stage_worker_nodes = vec![];
        if child_scheduled_stage.len() == 0 {
            if Self::include_table_scan(query_stage_ref.root.clone()) {
                cur_stage_worker_nodes = all_nodes.to_vec();
            } else {
                cur_stage_worker_nodes.push(self.worker_manager.next_random().clone());
            }
        } else {
            let mut use_num_nodes = all_nodes.len();
            for (stage_id, stage) in &child_scheduled_stage {
                if stage.query_stage.query_stage.distribution == Distribution::Single {
                    use_num_nodes = 1;
                    break;
                }
            }

            if use_num_nodes == all_nodes.len() {
                cur_stage_worker_nodes = all_nodes.into();
            } else {
                cur_stage_worker_nodes.push(self.worker_manager.next_random().clone());
            }
        }

        self.do_stage_execution(Arc::new(AugmentedStage::new_with_query_stage(
            query_stage_ref,
            &child_scheduled_stage,
            all_nodes,
            next_stage_parallelism as u32,
        )));
    }

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
    fn all_child_scheduled(&self, stage_id: StageId, child_stages: &HashSet<StageId>) -> bool {
        for child_stage_id in child_stages {
            if !self.scheduled_stages_map.contains_key(child_stage_id) {
                return false;
            }
        }
        true
    }

    /// Wrap scheduled stages into task and send to compute node for execution.
    fn do_stage_execution(&mut self, augmented_stage: AugmentedQueryStageRef) {
        let mut scheduled_tasks = HashMap::new();

        for task_id in 0..augmented_stage.parallelism {
            scheduled_tasks.insert(task_id as TaskId, augmented_stage.workers[task_id as usize].clone());
        }



        let scheduled_stage = ScheduledStage::from_augmented_stage(augmented_stage, scheduled_tasks);
        self.scheduled_stage_sender.send(Ok(scheduled_stage));
    }
}
