package com.risingwave.planner.rel.logical;

import com.risingwave.planner.sql.RisingWaveRexUtil;
import java.util.Set;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rex.RexNode;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Logical filter for risingwave. */
public class RwLogicalFilter extends Filter implements RisingWaveLogicalRel {
  protected RwLogicalFilter(
      RelOptCluster cluster, RelTraitSet traits, RelNode child, RexNode condition) {
    super(cluster, traits, child, condition);
    checkConvention();
  }

  @Override
  public Filter copy(RelTraitSet traitSet, RelNode input, RexNode condition) {
    return new RwLogicalFilter(getCluster(), traitSet, input, condition);
  }

  public static RwLogicalFilter create(
      RelNode input, RexNode condition, Set<CorrelationId> variablesSet) {
    return new RwLogicalFilter(
        input.getCluster(), input.getTraitSet().plus(LOGICAL), input, condition);
  }

  /** Rule to convert a {@link LogicalFilter} to a {@link RwLogicalFilter}. */
  public static class RwFilterConverterRule extends ConverterRule {
    public static final RwFilterConverterRule INSTANCE =
        Config.INSTANCE
            .withInTrait(Convention.NONE)
            .withOutTrait(LOGICAL)
            .withRuleFactory(RwFilterConverterRule::new)
            .withOperandSupplier(t -> t.operand(LogicalFilter.class).anyInputs())
            .withDescription("RisingWaveLogicalFilterConverter")
            .as(Config.class)
            .toRule(RwFilterConverterRule.class);

    protected RwFilterConverterRule(Config config) {
      super(config);
    }

    @Override
    public @Nullable RelNode convert(RelNode rel) {
      var logicalFilter = (LogicalFilter) rel;
      var input = ((LogicalFilter) rel).getInput();
      var newInput = RelOptRule.convert(input, input.getTraitSet().plus(LOGICAL).simplify());
      // Hack here to remove Sarg optimization of Calcite.
      var newCondition =
          RisingWaveRexUtil.expandSearch(
              rel.getCluster().getRexBuilder(), null, logicalFilter.getCondition());
      return new RwLogicalFilter(
          rel.getCluster(), rel.getTraitSet().plus(LOGICAL), newInput, newCondition);
    }
  }
}
