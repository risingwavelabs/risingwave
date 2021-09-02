package com.risingwave.planner.rel.logical;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rex.RexNode;
import org.checkerframework.checker.nullness.qual.Nullable;

public class RwLogicalSort extends Sort implements RisingWaveLogicalRel {
  public RwLogicalSort(
      RelOptCluster cluster,
      RelTraitSet traits,
      RelNode child,
      RelCollation collation,
      @Nullable RexNode offset,
      @Nullable RexNode fetch) {
    super(cluster, traits, child, collation, offset, fetch);
    checkConvention();
  }

  @Override
  public Sort copy(
      RelTraitSet traitSet,
      RelNode newInput,
      RelCollation newCollation,
      @Nullable RexNode offset,
      @Nullable RexNode fetch) {
    return new RwLogicalSort(getCluster(), traitSet, newInput, newCollation, offset, fetch);
  }

  public static class RwLogicalSortConverterRule extends ConverterRule {
    public static final RwLogicalSortConverterRule INSTANCE =
        Config.INSTANCE
            .withInTrait(Convention.NONE)
            .withOutTrait(LOGICAL)
            .withRuleFactory(RwLogicalSortConverterRule::new)
            .withOperandSupplier(t -> t.operand(LogicalSort.class).anyInputs())
            .withDescription("Converting logical sort to risingwave version.")
            .as(Config.class)
            .toRule(RwLogicalSortConverterRule.class);

    protected RwLogicalSortConverterRule(Config config) {
      super(config);
    }

    @Override
    public @Nullable RelNode convert(RelNode rel) {
      var logicalSort = (LogicalSort) rel;
      var input = logicalSort.getInput();
      var newInput = RelOptRule.convert(input, input.getTraitSet().plus(LOGICAL));
      var collationTraits = RelCollationTraitDef.INSTANCE.canonize(logicalSort.getCollation());
      var newTraits = logicalSort.getTraitSet().plus(LOGICAL).plus(collationTraits);

      return new RwLogicalSort(
          rel.getCluster(),
          newTraits,
          newInput,
          logicalSort.getCollation(),
          logicalSort.offset,
          logicalSort.fetch);
    }
  }
}
