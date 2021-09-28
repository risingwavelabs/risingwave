package com.risingwave.scheduler.query;

import static java.util.Objects.requireNonNull;

import com.risingwave.planner.rel.physical.batch.BatchPlan;
import com.risingwave.planner.rel.physical.batch.RisingWaveBatchPhyRel;
import com.risingwave.planner.rel.physical.batch.RwBatchExchange;
import com.risingwave.scheduler.shuffle.SinglePartitionSchema;
import com.risingwave.scheduler.stage.QueryStage;
import com.risingwave.scheduler.stage.StageId;
import org.apache.calcite.rel.RelNode;

public class PlanFragmenter {
  private final QueryId queryId = QueryId.next();

  private int nextStageId = 0;

  private final StageGraph.Builder graphBuilder;

  private PlanFragmenter() {
    this.graphBuilder = StageGraph.newBuilder();
  }

  // Break the query plan into fragments.
  public static Query planDistribution(BatchPlan plan) {
    requireNonNull(plan, "plan");
    var fragmenter = new PlanFragmenter();

    var rootStage = fragmenter.newQueryStage(plan.getRoot());
    fragmenter.buildStage(rootStage, plan.getRoot());
    return new Query(fragmenter.queryId, fragmenter.graphBuilder.build(rootStage.getStageId()));
  }

  // Recursively build the plan DAG.
  private void buildStage(QueryStage curStage, RisingWaveBatchPhyRel node) {
    // Children under pipeline-breaker separately forms a stage (aka plan fragment).

    // NOTE: The breaker's children will not be logically removed after plan slicing,
    // but their serialized plan will ignore the children. Therefore, the compute-node
    // will eventually only receive the sliced part.
    if (node instanceof RwBatchExchange) {
      for (RelNode rn : node.getInputs()) {
        RisingWaveBatchPhyRel child = (RisingWaveBatchPhyRel) rn;
        QueryStage childStage = newQueryStage(child);
        int exchangeId = ((RwBatchExchange) node).getUniqueId();
        graphBuilder.linkToChild(curStage.getStageId(), exchangeId, childStage.getStageId());

        buildStage(childStage, child);
      }
    } else {
      for (RelNode rn : node.getInputs()) {
        buildStage(curStage, (RisingWaveBatchPhyRel) rn);
      }
    }
  }

  private QueryStage newQueryStage(RisingWaveBatchPhyRel node) {
    StageId stageId = getNextStageId();
    var stage = new QueryStage(stageId, node, new SinglePartitionSchema());
    graphBuilder.addNode(stage);
    return stage;
  }

  private StageId getNextStageId() {
    return new StageId(queryId, nextStageId++);
  }
}
