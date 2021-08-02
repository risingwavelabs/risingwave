package com.risingwave.planner.rel.logical;

import com.risingwave.planner.rel.RisingWaveRel;
import org.apache.calcite.plan.Convention;

public interface RisingWaveLogicalRel extends RisingWaveRel {
  Convention LOGICAL = new Convention.Impl("RisingWave Logical", RisingWaveLogicalRel.class);
}
