package com.risingwave.scheduler.shuffle;

import com.risingwave.proto.plan.ShuffleInfo;

/** Describe partition schema of stage output. */
public interface PartitionSchema {
  ShuffleInfo toShuffleInfo();
}
