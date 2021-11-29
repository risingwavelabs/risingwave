package com.risingwave.scheduler.query;

import static java.util.Objects.requireNonNull;

import com.risingwave.planner.rel.physical.BatchPlan;
import com.risingwave.planner.rel.physical.RisingWaveBatchPhyRel;
import com.risingwave.planner.rel.physical.RwBatchExchange;
import com.risingwave.scheduler.exchange.Distribution;
import com.risingwave.scheduler.exchange.SingleDistribution;
import com.risingwave.scheduler.stage.QueryStage;
import com.risingwave.scheduler.stage.StageId;
import org.apache.calcite.rel.RelNode;

/** PlanFragmenter split physical plan into multiple stages according to the Exchange operators */
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

    var rootStage = fragmenter.newQueryStage(plan.getRoot(), new SingleDistribution());
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
      var exchangeNode = (RwBatchExchange) node;
      for (RelNode rn : node.getInputs()) {
        RisingWaveBatchPhyRel child = (RisingWaveBatchPhyRel) rn;
        // We remark that the distribution schema is for the output of the childStage.
        QueryStage childStage = newQueryStage(child, exchangeNode.createDistribution());
        int exchangeId = exchangeNode.getUniqueId();
        graphBuilder.linkToChild(curStage.getStageId(), exchangeId, childStage.getStageId());

        buildStage(childStage, child);
      }
    } else {
      for (RelNode rn : node.getInputs()) {
        buildStage(curStage, (RisingWaveBatchPhyRel) rn);
      }
    }
  }

  private QueryStage newQueryStage(RisingWaveBatchPhyRel node, Distribution distribution) {
    StageId stageId = getNextStageId();
    var stage = new QueryStage(stageId, node, distribution);
    graphBuilder.addNode(stage);
    return stage;
  }

  private StageId getNextStageId() {
    return new StageId(queryId, nextStageId++);
  }
}
