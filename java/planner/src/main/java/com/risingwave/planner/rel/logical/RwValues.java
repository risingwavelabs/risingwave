package com.risingwave.planner.rel.logical;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;
import org.checkerframework.checker.nullness.qual.Nullable;

public class RwValues extends Values implements RisingWaveLogicalRel {
  protected RwValues(
      RelOptCluster cluster,
      RelDataType rowType,
      ImmutableList<ImmutableList<RexLiteral>> tuples,
      RelTraitSet traits) {
    super(cluster, rowType, tuples, traits);
    checkArgument(traitSet.contains(RisingWaveLogicalRel.LOGICAL), "Not logical convention.");
  }

  public static class RwValuesConverterRule extends ConverterRule {
    public static final RwValuesConverterRule INSTANCE =
        Config.INSTANCE
            .withInTrait(Convention.NONE)
            .withOutTrait(LOGICAL)
            .withRuleFactory(RwValuesConverterRule::new)
            .withOperandSupplier(t -> t.operand(LogicalValues.class).noInputs())
            .withDescription("Converting logical values")
            .as(Config.class)
            .toRule(RwValuesConverterRule.class);

    protected RwValuesConverterRule(Config config) {
      super(config);
    }

    @Override
    public @Nullable RelNode convert(RelNode rel) {
      LogicalValues logicalValues = (LogicalValues) rel;
      return new RwValues(
          logicalValues.getCluster(),
          logicalValues.getRowType(),
          logicalValues.getTuples(),
          logicalValues.getTraitSet().replace(LOGICAL));
    }
  }
}
