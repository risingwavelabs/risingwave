package com.risingwave.planner.rel.logical;

import static com.google.common.base.Verify.verify;

import com.risingwave.planner.rel.RisingWaveRel;
import org.apache.calcite.plan.Convention;

public interface RisingWaveLogicalRel extends RisingWaveRel {
  Convention LOGICAL = new Convention.Impl("RisingWave Logical", RisingWaveLogicalRel.class);

  @Override
  default void checkConvention() {
    verify(getTraitSet().contains(LOGICAL), "Not logical plan node: %s", getClass().getName());
  }
}
