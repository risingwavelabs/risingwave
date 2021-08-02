package com.risingwave.planner.rel.logical;

import static com.google.common.base.Preconditions.checkArgument;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rex.RexNode;
import org.checkerframework.checker.nullness.qual.Nullable;

public class RwFilter extends Filter implements RisingWaveLogicalRel {
  protected RwFilter(RelOptCluster cluster, RelTraitSet traits, RelNode child, RexNode condition) {
    super(cluster, traits, child, condition);
    checkArgument(traitSet.contains(RisingWaveLogicalRel.LOGICAL), "Not logical convention.");
  }

  @Override
  public Filter copy(RelTraitSet traitSet, RelNode input, RexNode condition) {
    return new RwFilter(getCluster(), traitSet, input, condition);
  }

  public static class RwFilterConverterRule extends ConverterRule {
    public static final RwFilterConverterRule INSTANCE =
        Config.INSTANCE
            .withInTrait(Convention.NONE)
            .withOutTrait(LOGICAL)
            .withRuleFactory(RwFilterConverterRule::new)
            .withOperandSupplier(t -> t.operand(LogicalFilter.class).anyInputs())
            .withDescription("Converting logical filter to risingwave version.")
            .as(Config.class)
            .toRule(RwFilterConverterRule.class);

    protected RwFilterConverterRule(Config config) {
      super(config);
    }

    @Override
    public @Nullable RelNode convert(RelNode rel) {
      return new RwFilter(
          rel.getCluster(),
          rel.getTraitSet().replace(LOGICAL),
          rel.getInput(0),
          ((LogicalFilter) rel).getCondition());
    }
  }
}
