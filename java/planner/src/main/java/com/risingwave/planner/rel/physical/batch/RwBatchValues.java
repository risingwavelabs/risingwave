package com.risingwave.planner.rel.physical.batch;

import static com.risingwave.planner.rel.logical.RisingWaveLogicalRel.LOGICAL;

import com.google.common.collect.ImmutableList;
import com.risingwave.planner.rel.logical.RwLogicalValues;
import com.risingwave.proto.plan.PlanNode;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;
import org.checkerframework.checker.nullness.qual.Nullable;

public class RwBatchValues extends Values implements RisingWaveBatchPhyRel {
  protected RwBatchValues(
      RelOptCluster cluster,
      RelDataType rowType,
      ImmutableList<ImmutableList<RexLiteral>> tuples,
      RelTraitSet traits) {
    super(cluster, rowType, tuples, traits);
    checkConvention();
  }

  @Override
  public PlanNode serialize() {
    throw new UnsupportedOperationException("RwBatchValues#serialize is not supported");
  }

  public static class BatchValuesConverterRule extends ConverterRule {
    public static final BatchValuesConverterRule INSTANCE =
        Config.INSTANCE
            .withInTrait(LOGICAL)
            .withOutTrait(BATCH_PHYSICAL)
            .withRuleFactory(BatchValuesConverterRule::new)
            .withOperandSupplier(t -> t.operand(RwLogicalValues.class).noInputs())
            .withDescription("Converting batch values")
            .as(Config.class)
            .toRule(BatchValuesConverterRule.class);

    protected BatchValuesConverterRule(Config config) {
      super(config);
    }

    @Override
    public @Nullable RelNode convert(RelNode rel) {
      RwLogicalValues rwLogicalValues = (RwLogicalValues) rel;
      return new RwBatchValues(
          rwLogicalValues.getCluster(),
          rwLogicalValues.getRowType(),
          rwLogicalValues.getTuples(),
          rwLogicalValues.getTraitSet().replace(BATCH_PHYSICAL));
    }
  }
}
