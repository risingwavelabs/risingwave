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

public class RwLogicalInsert extends TableModify implements RisingWaveLogicalRel {
  protected RwLogicalInsert(
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
        Operation.INSERT,
        updateColumnList,
        null,
        false);
    checkConvention();
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new RwLogicalInsert(
        getCluster(),
        traitSet,
        getTable(),
        getCatalogReader(),
        sole(inputs),
        getUpdateColumnList());
  }

  public static final class LogicalInsertConverterRule extends ConverterRule {
    public static final LogicalInsertConverterRule INSTANCE =
        Config.INSTANCE
            .withInTrait(Convention.NONE)
            .withOutTrait(LOGICAL)
            .withRuleFactory(LogicalInsertConverterRule::new)
            .withOperandSupplier(
                t ->
                    t.operand(LogicalTableModify.class)
                        .predicate(rel -> rel.getOperation() == Operation.INSERT)
                        .convert(Convention.NONE))
            .withDescription("Converting calcite logical insert to risingwave.")
            .as(Config.class)
            .toRule(LogicalInsertConverterRule.class);

    private LogicalInsertConverterRule(Config config) {
      super(config);
    }

    @Override
    public @Nullable RelNode convert(RelNode rel) {
      var tableModify = (LogicalTableModify) rel;
      var input = tableModify.getInput();
      var newInput = RelOptRule.convert(input, input.getTraitSet().plus(LOGICAL));

      RelTraitSet newTraitSet = tableModify.getTraitSet().plus(RisingWaveLogicalRel.LOGICAL);
      return new RwLogicalInsert(
          tableModify.getCluster(),
          newTraitSet,
          tableModify.getTable(),
          tableModify.getCatalogReader(),
          newInput,
          tableModify.getUpdateColumnList());
    }
  }
}
