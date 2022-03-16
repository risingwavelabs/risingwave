package com.risingwave.planner.rel.logical;

import java.util.List;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.checkerframework.checker.nullness.qual.Nullable;

public class RwLogicalDelete extends TableModify implements RisingWaveLogicalRel {
  protected RwLogicalDelete(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelOptTable table,
      Prepare.CatalogReader catalogReader,
      RelNode input,
      @Nullable List<String> updateColumnList) {
    super(
        cluster,
        traitSet,
        table,
        catalogReader,
        input,
        Operation.DELETE,
        updateColumnList,
        null,
        false);
    checkConvention();
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new RwLogicalDelete(
        getCluster(),
        traitSet,
        getTable(),
        getCatalogReader(),
        sole(inputs),
        getUpdateColumnList());
  }

  public static final class LogicalDeleteConverterRule extends ConverterRule {
    public static final LogicalDeleteConverterRule INSTANCE =
        Config.INSTANCE
            .withInTrait(Convention.NONE)
            .withOutTrait(LOGICAL)
            .withRuleFactory(LogicalDeleteConverterRule::new)
            .withOperandSupplier(
                t ->
                    t.operand(LogicalTableModify.class)
                        .predicate(rel -> rel.getOperation() == Operation.DELETE)
                        .convert(Convention.NONE))
            .withDescription("Converting calcite logical delete to RisingWave.")
            .as(Config.class)
            .toRule(LogicalDeleteConverterRule.class);

    private LogicalDeleteConverterRule(Config config) {
      super(config);
    }

    @Override
    public @Nullable RelNode convert(RelNode rel) {
      var tableModify = (LogicalTableModify) rel;
      var input = tableModify.getInput();
      var newInput = RelOptRule.convert(input, input.getTraitSet().plus(LOGICAL));

      RelTraitSet newTraitSet = tableModify.getTraitSet().plus(RisingWaveLogicalRel.LOGICAL);
      return new RwLogicalDelete(
          tableModify.getCluster(),
          newTraitSet,
          tableModify.getTable(),
          tableModify.getCatalogReader(),
          newInput,
          tableModify.getUpdateColumnList());
    }
  }
}
