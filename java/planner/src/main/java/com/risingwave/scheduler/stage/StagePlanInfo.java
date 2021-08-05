package com.risingwave.scheduler.stage;

import static java.util.Objects.requireNonNull;

import com.risingwave.planner.rel.physical.batch.RisingWaveBatchPhyRel;
import com.risingwave.proto.plan.PlanFragment;
import com.risingwave.scheduler.shuffle.PartitionSchema;

public class StagePlanInfo {
  private final RisingWaveBatchPhyRel root;
  private final PartitionSchema partitionSchema;
  private final int parallelism;

  public StagePlanInfo(
      RisingWaveBatchPhyRel root, PartitionSchema partitionSchema, int parallelism) {
    this.root = requireNonNull(root, "root");
    this.partitionSchema = requireNonNull(partitionSchema, "partitionSchema");
    this.parallelism = parallelism;
  }

  public RisingWaveBatchPhyRel getRoot() {
    return root;
  }

  public PartitionSchema getPartitionSchema() {
    return partitionSchema;
  }

  public int getParallelism() {
    return parallelism;
  }

  public PlanFragment toPlanFragmentProto() {
    return PlanFragment.newBuilder()
        .setRoot(root.serialize())
        .setShuffleInfo(partitionSchema.toShuffleInfo())
        .build();
  }
}
