package com.risingwave.scheduler.shuffle;

import com.risingwave.proto.plan.ShuffleInfo;

/** Used by tasks of root stage. */
public class SinglePartitionSchema implements PartitionSchema {
  @Override
  public ShuffleInfo toShuffleInfo() {
    return ShuffleInfo.newBuilder().setPartitionMode(ShuffleInfo.PartitionMode.SINGLE).build();
  }
}
