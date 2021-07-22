package com.risingwave.planner.rel.physical.streaming;

import com.risingwave.planner.rel.physical.RisingWavePhysicalRel;
import org.apache.calcite.plan.Convention;

public interface RisingWaveStreamingPhyRel extends RisingWavePhysicalRel {
  Convention STREAMING_PHYSICAL =
      new Convention.Impl("RisingWave Streaming Physical", RisingWaveStreamingPhyRel.class);
}
