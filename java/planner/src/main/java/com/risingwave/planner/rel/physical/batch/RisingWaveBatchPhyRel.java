package com.risingwave.planner.rel.physical.batch;

import static com.google.common.base.Verify.verify;

import com.risingwave.planner.rel.common.dist.RwDistributionTrait;
import com.risingwave.planner.rel.common.dist.RwDistributionTraitDef;
import com.risingwave.planner.rel.common.dist.RwDistributions;
import com.risingwave.planner.rel.physical.RisingWavePhysicalRel;
import java.util.List;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.PhysicalNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.util.Pair;
import org.checkerframework.checker.nullness.qual.Nullable;

/** BatchPhyRel used in physical and distributed optimization phase */
public interface RisingWaveBatchPhyRel extends RisingWavePhysicalRel, PhysicalNode {
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

        @Override
        public @Nullable RelNode enforce(RelNode input, RelTraitSet required) {
          if (input.getConvention() != BATCH_PHYSICAL) {
            return null;
          }
          var coll = required.getCollation();
          if (coll != null && coll.getKeys().size() != 0) {
            input = RwBatchSort.create(input, coll);
          }
          return input;
        }
      };

  Convention BATCH_DISTRIBUTED =
      new Convention.Impl("RisingWave Batch Distributed", RisingWaveBatchPhyRel.class) {
        @Override
        public boolean canConvertConvention(Convention toConvention) {
          return true;
        }

        @Override
        public boolean useAbstractConvertersForConversion(
            RelTraitSet fromTraits, RelTraitSet toTraits) {
          return true;
        }

        @Override
        public @Nullable RelNode enforce(RelNode input, RelTraitSet required) {
          if (input.getConvention() != BATCH_DISTRIBUTED) {
            return null;
          }
          RwDistributionTrait dist = required.getTrait(RwDistributionTraitDef.getInstance());
          if (dist != null && dist != RwDistributions.ANY) {
            input = RwBatchExchange.create(input, dist);
          }
          return input;
        }
      };

  static <PhyRelT extends RisingWaveBatchPhyRel> RelOptRule getDistributedConvertRule(
      Class<PhyRelT> phyRelClazz) {
    return ConverterRule.Config.INSTANCE
        .withInTrait(BATCH_PHYSICAL)
        .withOutTrait(BATCH_DISTRIBUTED)
        .withRuleFactory(DistributedBatchConverterRule::new)
        .withOperandSupplier(t -> t.operand(phyRelClazz).anyInputs())
        .withDescription(
            String.format("Converting batch physical %s to distributed.", phyRelClazz.getName()))
        .as(ConverterRule.Config.class)
        .toRule();
  }

  /** distributed convert rule depend on `convertToDistributed` implementation */
  class DistributedBatchConverterRule extends ConverterRule {

    protected DistributedBatchConverterRule(Config config) {
      super(config);
    }

    @Override
    public @Nullable RelNode convert(RelNode rel) {
      final var phyRel = (RisingWaveBatchPhyRel) rel;
      return phyRel.convertToDistributed();
    }
  }

  default @Nullable RelNode convertToDistributed() {
    return null;
  }

  @Override
  default @Nullable Pair<RelTraitSet, List<RelTraitSet>> passThroughTraits(RelTraitSet required) {
    return null;
  }

  @Override
  default @Nullable Pair<RelTraitSet, List<RelTraitSet>> deriveTraits(
      RelTraitSet childTraits, int childId) {
    return null;
  }

  @Override
  default void checkConvention() {
    verify(
        getTraitSet().contains(BATCH_PHYSICAL) || getTraitSet().contains(BATCH_DISTRIBUTED),
        "Not batch physical plan: %s",
        getClass().getName());
  }
}
