package com.risingwave.planner.rel.physical.batch;

import static java.util.Objects.requireNonNull;

import com.risingwave.proto.plan.PlanFragment;
import com.risingwave.proto.plan.PlanNode;
import com.risingwave.proto.plan.ShuffleInfo;

/**
 * Plan for adhoc query execution. The counterpart is streaming plan, which is designed for
 * streaming execution.
 */
public class BatchPlan {
  private final RisingWaveBatchPhyRel root;

  public BatchPlan(RisingWaveBatchPhyRel root) {
    this.root = requireNonNull(root, "Root can't be null!");
  }

  public RisingWaveBatchPhyRel getRoot() {
    return root;
  }

  public PlanFragment serialize() {
    ShuffleInfo shuffleInfo =
        ShuffleInfo.newBuilder().setPartitionMode(ShuffleInfo.PartitionMode.SINGLE).build();

    PlanNode rootNode = root.serialize();

    return PlanFragment.newBuilder().setRoot(rootNode).setShuffleInfo(shuffleInfo).build();
  }
}
