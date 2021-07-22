package com.risingwave.planner.rel.physical.batch;

import com.risingwave.planner.rel.physical.RisingWavePhysicalRel;
import org.apache.calcite.plan.Convention;

public interface RisingWaveBatchPhyRel extends RisingWavePhysicalRel {
  Convention BATCH_PHYSICAL =
      new Convention.Impl("RisingWave Batch Physical", RisingWaveBatchPhyRel.class);
}
