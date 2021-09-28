package com.risingwave.scheduler;

import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.risingwave.node.WorkerNodeManager;
import com.risingwave.planner.rel.physical.batch.RisingWaveBatchPhyRel;
import com.risingwave.scheduler.query.Query;
import com.risingwave.scheduler.stage.QueryStage;
import com.risingwave.scheduler.stage.ScheduledStage;
import com.risingwave.scheduler.stage.StageId;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.calcite.rel.core.TableScan;

@Singleton
public class ResourceManager {
  private final WorkerNodeManager nodeManager;

  @Inject
  public ResourceManager(WorkerNodeManager nodeManager) {
    this.nodeManager = requireNonNull(nodeManager, "nodeManager");
  }

  /**
   * Calculates the number of parallelism of the stage, and assigns a list of nodes for execution.
   */
  public QueryStage schedule(
      Query query, StageId stageId, ImmutableMap<StageId, ScheduledStage> stageChildren) {
    QueryStage stage = query.getQueryStageChecked(stageId);
    if (stageChildren.size() == 0 && involvesTableScan(stage.getRoot())) {
      // If this is a TableScan node, i.e. FilterScan, it will be replicated to
      // all nodes due to currently the unawareness of data location.
      return stage.augmentInfo(query, stageChildren, ImmutableList.copyOf(nodeManager.allNodes()));
    }
    return stage.augmentInfo(query, stageChildren, ImmutableList.of(nodeManager.nextRandom()));
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
