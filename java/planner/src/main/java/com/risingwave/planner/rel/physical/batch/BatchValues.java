package com.risingwave.planner.rel.physical.batch;

import static com.google.common.base.Preconditions.checkArgument;
import static com.risingwave.planner.rel.logical.RisingWaveLogicalRel.LOGICAL;

import com.google.common.collect.ImmutableList;
import com.risingwave.planner.rel.logical.RwValues;
import com.risingwave.proto.plan.PlanNode;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;
import org.checkerframework.checker.nullness.qual.Nullable;

public class BatchValues extends Values implements RisingWaveBatchPhyRel {
  protected BatchValues(
      RelOptCluster cluster,
      RelDataType rowType,
      ImmutableList<ImmutableList<RexLiteral>> tuples,
      RelTraitSet traits) {
    super(cluster, rowType, tuples, traits);
    checkArgument(traitSet.contains(RisingWaveBatchPhyRel.BATCH_PHYSICAL));
  }

  @Override
  public PlanNode serialize() {
    throw new UnsupportedOperationException();
  }

  public static class BatchValuesConverterRule extends ConverterRule {
    public static final BatchValuesConverterRule INSTANCE =
        Config.INSTANCE
            .withInTrait(LOGICAL)
            .withOutTrait(BATCH_PHYSICAL)
            .withRuleFactory(BatchValuesConverterRule::new)
            .withOperandSupplier(t -> t.operand(RwValues.class).noInputs())
            .withDescription("Converting batch values")
            .as(Config.class)
            .toRule(BatchValuesConverterRule.class);

    protected BatchValuesConverterRule(Config config) {
      super(config);
    }

    @Override
    public @Nullable RelNode convert(RelNode rel) {
      RwValues rwValues = (RwValues) rel;
      return new BatchValues(
          rwValues.getCluster(),
          rwValues.getRowType(),
          rwValues.getTuples(),
          rwValues.getTraitSet().replace(BATCH_PHYSICAL));
    }
  }
}
