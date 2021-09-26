package com.risingwave.planner.rel.physical.streaming;

import static com.google.common.base.Verify.verify;

import com.risingwave.planner.rel.RisingWaveRel;
import com.risingwave.proto.streaming.plan.StreamNode;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTraitSet;

public interface RisingWaveStreamingRel extends RisingWaveRel {
  Convention STREAMING =
      new Convention.Impl("RisingWave Streaming Plan", RisingWaveStreamingRel.class) {
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

  StreamNode serialize();

  @Override
  default void checkConvention() {
    verify(getTraitSet().contains(STREAMING), "Not streaming plan: %s", getClass().getName());
  }
}
