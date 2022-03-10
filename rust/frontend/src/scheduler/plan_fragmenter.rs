use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use risingwave_common::error::Result;
use uuid::Uuid;

use crate::optimizer::plan_node::{BatchExchange, PlanNodeType, PlanTreeNode};
use crate::optimizer::property::Distribution;
use crate::optimizer::PlanRef;

pub(crate) type StageId = u64;

/// `BatchPlanFragmenter` splits a query plan into fragments.
struct BatchPlanFragmenter {
    stage_graph_builder: StageGraphBuilder,
    next_stage_id: u64,
}

impl BatchPlanFragmenter {
    pub fn new() -> Self {
        Self {
            stage_graph_builder: StageGraphBuilder::new(),
            next_stage_id: 0,
        }
    }
}

/// Contains the connection info of each stage.
pub(crate) struct Query {
    /// Query id should always be unique.
    query_id: Uuid,
    pub(crate) stage_graph: StageGraph,
}

impl Query {
    pub fn leaf_stages(&self) -> Vec<StageId> {
        let mut ret_leaf_stages = Vec::new();
        for stage_id in self.stage_graph.stages.keys() {
            if self
                .stage_graph
                .get_child_stages_unchecked(stage_id)
                .is_empty()
            {
                ret_leaf_stages.push(*stage_id);
            }
        }
        ret_leaf_stages
    }

    pub fn get_parents(&self, stage_id: &StageId) -> &HashSet<StageId> {
        self.stage_graph.parent_edges.get(stage_id).unwrap()
    }
}

/// Fragment part of `Query`.
#[derive(Debug)]
pub(crate) struct QueryStage {
    pub id: StageId,
    pub root: PlanRef,
    pub distribution: Distribution,
}
pub(crate) type QueryStageRef = Arc<QueryStage>;

/// Maintains how each stage are connected.
pub(crate) struct StageGraph {
    pub(crate) id: StageId,
    stages: HashMap<StageId, QueryStageRef>,
    /// Traverse from top to down. Used in split plan into stages.
    child_edges: HashMap<StageId, HashSet<StageId>>,
    /// Traverse from down to top. Used in schedule each stage.
    parent_edges: HashMap<StageId, HashSet<StageId>>,
    /// Indicates which stage the exchange executor is running on.
    /// Look up child stage for exchange source so that parent stage knows where to pull data.
    exchange_id_to_stage: HashMap<u64, StageId>,
}

impl StageGraph {
    pub fn get_stage_unchecked(&self, stage_id: &StageId) -> QueryStageRef {
        self.stages.get(stage_id).unwrap().clone()
    }

    pub fn get_child_stages_unchecked(&self, stage_id: &StageId) -> &HashSet<StageId> {
        self.child_edges.get(stage_id).unwrap()
    }
}

struct StageGraphBuilder {
    stages: HashMap<StageId, QueryStageRef>,
    child_edges: HashMap<StageId, HashSet<StageId>>,
    parent_edges: HashMap<StageId, HashSet<StageId>>,
    exchange_id_to_stage: HashMap<u64, StageId>,
}

impl StageGraphBuilder {
    pub fn new() -> Self {
        Self {
            stages: HashMap::new(),
            child_edges: HashMap::new(),
            parent_edges: HashMap::new(),
            exchange_id_to_stage: HashMap::new(),
        }
    }

    pub fn build(mut self, stage_id: StageId) -> StageGraph {
        for stage_id in self.stages.keys() {
            if self.child_edges.get(stage_id).is_none() {
                self.child_edges.insert(*stage_id, HashSet::new());
            }

            if self.parent_edges.get(stage_id).is_none() {
                self.parent_edges.insert(*stage_id, HashSet::new());
            }
        }

        StageGraph {
            id: stage_id,
            stages: self.stages,
            child_edges: self.child_edges,
            parent_edges: self.parent_edges,
            exchange_id_to_stage: self.exchange_id_to_stage,
        }
    }

    /// Link parent stage and child stage. Maintain the mappings of parent -> child and child ->
    /// parent.
    ///
    /// # Arguments
    ///
    /// * `exchange_id` - The operator id of exchange executor.
    pub fn link_to_child(&mut self, parent_id: StageId, exchange_id: u64, child_id: StageId) {
        let child_ids = self.child_edges.get_mut(&parent_id);
        // If the parent id does not exist, create a new set containing the child ids. Otherwise
        // just insert.
        match child_ids {
            Some(childs) => {
                childs.insert(child_id);
            }

            None => {
                let mut childs = HashSet::new();
                childs.insert(child_id);
                self.child_edges.insert(parent_id, childs);
            }
        };

        let parent_ids = self.parent_edges.get_mut(&child_id);
        // If the child id does not exist, create a new set containing the parent ids. Otherwise
        // just insert.
        match parent_ids {
            Some(parent_ids) => {
                parent_ids.insert(parent_id);
            }

            None => {
                let mut parents = HashSet::new();
                parents.insert(parent_id);
                self.parent_edges.insert(child_id, parents);
            }
        };
        self.exchange_id_to_stage.insert(exchange_id, child_id);
    }

    pub fn add_node(&mut self, stage: QueryStageRef) {
        self.stages.insert(stage.id, stage);
    }
}

impl BatchPlanFragmenter {
    /// Split the plan node into each stages, based on exchange node.
    pub fn split(mut self, batch_node: PlanRef) -> Result<Query> {
        let root_stage_graph =
            self.new_query_stage(batch_node.clone(), batch_node.distribution().clone());
        self.build_stage(&root_stage_graph, batch_node.clone());
        let stage_graph = self.stage_graph_builder.build(root_stage_graph.id);
        Ok(Query {
            stage_graph,
            query_id: Uuid::new_v4(),
        })
    }

    fn new_query_stage(&mut self, node: PlanRef, distribution: Distribution) -> QueryStageRef {
        let next_stage_id = self.next_stage_id;
        self.next_stage_id += 1;
        let stage = Arc::new(QueryStage {
            id: next_stage_id,
            root: node.clone(),
            distribution,
        });
        self.stage_graph_builder.add_node(stage.clone());
        stage
    }

    /// Based on current stage, use stage graph builder to recursively build the DAG plan (splits
    /// the plan by exchange node.). Children under pipeline-breaker separately forms a stage
    /// (aka plan fragment).
    fn build_stage(&mut self, cur_stage: &QueryStage, node: PlanRef) {
        // NOTE: The breaker's children will not be logically removed after plan slicing,
        // but their serialized plan will ignore the children. Therefore, the compute-node
        // will eventually only receive the sliced part.
        if node.node_type() == PlanNodeType::BatchExchange {
            let exchange_node = node.downcast_ref::<BatchExchange>().unwrap();
            for child_node in exchange_node.inputs() {
                // If plan node is a exchange node, for each inputs (child), new a query stage and
                // link with current stage.
                let child_query_stage =
                    self.new_query_stage(child_node.clone(), child_node.distribution().clone());
                // TODO(Bowen): replace mock exchange id 0 to real operator id (#67).
                self.stage_graph_builder
                    .link_to_child(cur_stage.id, 0, child_query_stage.id);
                self.build_stage(&child_query_stage, child_node);
            }
        } else {
            for child_node in node.inputs() {
                // All child nodes still belongs to current stage if no exchange.
                self.build_stage(cur_stage, child_node);
            }
        }
    }
}

#[cfg(test)]
mod tests {

    use std::sync::Arc;

    use risingwave_common::catalog::{Schema, TableId};
    use risingwave_pb::common::{ParallelUnit, WorkerNode, WorkerType};
    use risingwave_pb::plan::JoinType;

    use crate::optimizer::plan_node::{
        BatchExchange, BatchHashJoin, BatchSeqScan, EqJoinPredicate, LogicalJoin, LogicalScan,
        PlanNodeType,
    };
    use crate::optimizer::property::{Distribution, Order};
    use crate::optimizer::PlanRef;
    use crate::scheduler::plan_fragmenter::BatchPlanFragmenter;
    use crate::scheduler::schedule::{BatchScheduler, WorkerNodeManager};
    use crate::utils::Condition;

    #[tokio::test]
    async fn test_fragmenter() {
        // Construct a Hash Join with Exchange node.
        // Logical plan:
        //
        //    HashJoin
        //     /    \
        //   Scan  Scan
        //
        let batch_plan_node: PlanRef = BatchSeqScan::new(LogicalScan::new(
            "".to_string(),
            TableId::default(),
            vec![],
            Schema::default(),
        ))
        .into();
        let batch_exchange_node1: PlanRef = BatchExchange::new(
            batch_plan_node.clone(),
            Order::default(),
            Distribution::AnyShard,
        )
        .into();
        let batch_exchange_node2: PlanRef = BatchExchange::new(
            batch_plan_node.clone(),
            Order::default(),
            Distribution::AnyShard,
        )
        .into();
        let hash_join_node: PlanRef = BatchHashJoin::new(
            LogicalJoin::new(
                batch_exchange_node1.clone(),
                batch_exchange_node2.clone(),
                JoinType::Inner,
                Condition::true_cond(),
            ),
            EqJoinPredicate::create(0, 0, Condition::true_cond()),
        )
        .into();
        let batch_exchange_node3: PlanRef = BatchExchange::new(
            hash_join_node.clone(),
            Order::default(),
            Distribution::Single,
        )
        .into();

        // Break the plan node into fragments.
        let fragmenter = BatchPlanFragmenter::new();
        let query = fragmenter.split(batch_exchange_node3).unwrap();

        assert_eq!(query.stage_graph.id, 0);
        assert_eq!(query.stage_graph.stages.len(), 4);

        // Check the mappings of child edges.
        assert_eq!(query.stage_graph.child_edges.get(&0).unwrap().len(), 1);
        assert_eq!(query.stage_graph.child_edges.get(&1).unwrap().len(), 2);
        assert_eq!(query.stage_graph.child_edges.get(&2).unwrap().len(), 0);
        assert_eq!(query.stage_graph.child_edges.get(&3).unwrap().len(), 0);

        // Check the mappings of parent edges.
        assert_eq!(query.stage_graph.parent_edges.get(&0).unwrap().len(), 0);
        assert_eq!(query.stage_graph.parent_edges.get(&1).unwrap().len(), 1);
        assert_eq!(query.stage_graph.parent_edges.get(&2).unwrap().len(), 1);
        assert_eq!(query.stage_graph.parent_edges.get(&3).unwrap().len(), 1);

        // Check plan node in each stages.
        let root_exchange = query.stage_graph.stages.get(&0).unwrap();
        assert_eq!(root_exchange.root.node_type(), PlanNodeType::BatchExchange);
        let join_node = query.stage_graph.stages.get(&1).unwrap();
        assert_eq!(join_node.root.node_type(), PlanNodeType::BatchHashJoin);
        let scan_node1 = query.stage_graph.stages.get(&2).unwrap();
        assert_eq!(scan_node1.root.node_type(), PlanNodeType::BatchSeqScan);
        let scan_node2 = query.stage_graph.stages.get(&3).unwrap();
        assert_eq!(scan_node2.root.node_type(), PlanNodeType::BatchSeqScan);

        // -- Check augment phase --
        let worker1 = WorkerNode {
            id: 0,
            r#type: WorkerType::ComputeNode as i32,
            host: None,
            state: risingwave_pb::common::worker_node::State::Running as i32,
            parallel_units: vec![ParallelUnit { id: 1 }],
        };
        let worker2 = WorkerNode {
            id: 1,
            r#type: WorkerType::ComputeNode as i32,
            host: None,
            state: risingwave_pb::common::worker_node::State::Running as i32,
            parallel_units: vec![ParallelUnit { id: 2 }],
        };
        let worker3 = WorkerNode {
            id: 2,
            r#type: WorkerType::ComputeNode as i32,
            host: None,
            state: risingwave_pb::common::worker_node::State::Running as i32,
            parallel_units: vec![ParallelUnit { id: 3 }],
        };
        let workers = vec![worker1.clone(), worker2.clone(), worker3.clone()];
        let worker_node_manager = Arc::new(WorkerNodeManager::mock(workers));
        let mut scheduler = BatchScheduler::mock(worker_node_manager);
        let _query_result_loc = scheduler.schedule(&query).await;

        let root = scheduler.get_scheduled_stage_unchecked(&0);
        assert_eq!(root.augmented_stage.exchange_source.len(), 1);
        assert!(root.augmented_stage.exchange_source.get(&1).is_some());
        assert_eq!(root.assignments.len(), 1);
        assert_eq!(root.assignments.get(&0).unwrap(), &worker1);

        let join_node = scheduler.get_scheduled_stage_unchecked(&1);
        assert_eq!(join_node.augmented_stage.exchange_source.len(), 2);
        assert!(join_node.augmented_stage.exchange_source.get(&2).is_some());
        assert!(join_node.augmented_stage.exchange_source.get(&3).is_some());
        assert_eq!(join_node.assignments.len(), 3);
        assert_eq!(join_node.assignments.get(&0).unwrap(), &worker1);
        assert_eq!(join_node.assignments.get(&1).unwrap(), &worker2);
        assert_eq!(join_node.assignments.get(&2).unwrap(), &worker3);

        let scan_node_1 = scheduler.get_scheduled_stage_unchecked(&2);
        assert_eq!(scan_node_1.augmented_stage.exchange_source.len(), 0);
        assert_eq!(scan_node_1.assignments.len(), 3);
        assert_eq!(scan_node_1.assignments.get(&0).unwrap(), &worker1);
        assert_eq!(scan_node_1.assignments.get(&1).unwrap(), &worker2);
        assert_eq!(scan_node_1.assignments.get(&2).unwrap(), &worker3);

        let scan_node_2 = scheduler.get_scheduled_stage_unchecked(&2);
        assert_eq!(scan_node_2.augmented_stage.exchange_source.len(), 0);
        assert_eq!(scan_node_2.assignments.len(), 3);
        assert_eq!(scan_node_2.assignments.get(&0).unwrap(), &worker1);
        assert_eq!(scan_node_2.assignments.get(&1).unwrap(), &worker2);
        assert_eq!(scan_node_2.assignments.get(&2).unwrap(), &worker3);
    }
}
