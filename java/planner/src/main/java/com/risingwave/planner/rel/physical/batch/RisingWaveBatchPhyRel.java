package com.risingwave.planner.rel.physical.batch;

import static com.google.common.base.Verify.verify;

import com.risingwave.planner.rel.physical.RisingWavePhysicalRel;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTraitSet;

public interface RisingWaveBatchPhyRel extends RisingWavePhysicalRel {
  Convention BATCH_PHYSICAL =
      new Convention.Impl("RisingWave Batch Physical", RisingWaveBatchPhyRel.class) {
        @Override
        public boolean canConvertConvention(Convention toConvention) {
          return true;
        }

        @Override
        public boolean useAbstractConvertersForConversion(
            RelTraitSet fromTraits, RelTraitSet toTraits) {
          return true;
        }
      };

  @Override
  default void checkConvention() {
    verify(
        getTraitSet().contains(BATCH_PHYSICAL),
        "Not batch physical plan: %s",
        getClass().getName());
  }
}
