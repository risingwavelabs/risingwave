package com.risingwave.scheduler;

import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.risingwave.node.WorkerNode;
import com.risingwave.node.WorkerNodeManager;
import com.risingwave.planner.rel.physical.RisingWaveBatchPhyRel;
import com.risingwave.scheduler.exchange.SingleDistribution;
import com.risingwave.scheduler.query.Query;
import com.risingwave.scheduler.stage.QueryStage;
import com.risingwave.scheduler.stage.ScheduledStage;
import com.risingwave.scheduler.stage.StageId;
import java.util.ArrayList;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.calcite.rel.core.TableScan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Resource Manager for determining which worker nodes a task should be running */
@Singleton
public class ResourceManager {
  private static final Logger log = LoggerFactory.getLogger(ResourceManager.class);

  private final WorkerNodeManager nodeManager;

  @Inject
  public ResourceManager(WorkerNodeManager nodeManager) {
    this.nodeManager = requireNonNull(nodeManager, "nodeManager");
  }

  /**
   * Calculates the number of parallelism of the stage, and assigns a list of nodes for execution.
   * As long as the query plan allows, we use all the nodes for the sake of testing. We may later
   * have a code path for testing only.
   */
  public QueryStage schedule(
      Query query, StageId stageId, ImmutableMap<StageId, ScheduledStage> stageChildren) {
    QueryStage stage = query.getQueryStageChecked(stageId);
    var distributionSchema = stage.getDistribution();
    var nextStageParallelism = 1;
    // except single distribution, we would use all of worker nodes to run one stage
    if (distributionSchema instanceof SingleDistribution) {
      nextStageParallelism = 1;
    } else {
      nextStageParallelism = nodeManager.allNodes().size();
    }
    var workerNodesForCurrentStage = new ArrayList<WorkerNode>();
    if (stageChildren.size() == 0) {
      if (involvesTableScan(stage.getRoot())) {
        // The frontend is unaware of the table location for now, so run on all worker nodes
        workerNodesForCurrentStage.addAll(nodeManager.allNodes());
      } else {
        // if there is no table scan, we use single node to run the stage
        // e.g. select * from 1. Running on N nodes would have N rows as result.
        workerNodesForCurrentStage.add(nodeManager.nextRandom());
      }
    } else {
      int useNumNodes = nodeManager.allNodes().size();
      // We use all the worker nodes except that the exchange distribution of children stage is
      // single
      for (var entry : stageChildren.entrySet()) {
        var scheduledStage = entry.getValue();
        var queryStage = scheduledStage.getStage();
        if (queryStage.getDistribution() instanceof SingleDistribution) {
          useNumNodes = 1;
          break;
        }
      }
      if (useNumNodes == nodeManager.allNodes().size()) {
        workerNodesForCurrentStage.addAll(nodeManager.allNodes());
      } else {
        workerNodesForCurrentStage.add(nodeManager.nextRandom());
      }
    }
    var augmentedStage =
        stage.augmentInfo(
            query,
            stageChildren,
            ImmutableList.copyOf(workerNodesForCurrentStage),
            nextStageParallelism);
    log.debug(
        "schedule stage:"
            + stageId
            + " "
            + ",nextStageParallelism:"
            + nextStageParallelism
            + ",currentStageParallelism:"
            + augmentedStage.getParallelism());
    return augmentedStage;
  }

  public boolean involvesTableScan(RisingWaveBatchPhyRel node) {
    if (node instanceof TableScan) {
      return true;
    }
    for (var child : node.getInputs()) {
      if (involvesTableScan((RisingWaveBatchPhyRel) child)) {
        return true;
      }
    }
    return false;
  }
}
