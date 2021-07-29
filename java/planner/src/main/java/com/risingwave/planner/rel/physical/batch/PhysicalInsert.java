package com.risingwave.planner.rel.physical.batch;

import static com.google.common.base.Preconditions.checkArgument;
import static com.risingwave.planner.rel.logical.RisingWaveLogicalRel.LOGICAL;

import com.risingwave.planner.rel.logical.LogicalInsert;
import com.risingwave.proto.plan.PlanNode;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.TableModify;
import org.checkerframework.checker.nullness.qual.Nullable;

public class PhysicalInsert extends TableModify implements RisingWaveBatchPhyRel {
  protected PhysicalInsert(
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
    checkArgument(traitSet.contains(RisingWaveBatchPhyRel.BATCH_PHYSICAL));
  }

  @Override
  public PlanNode serialize() {
    throw new UnsupportedOperationException();
  }

  public static class PhysicalInsertConverterRule extends ConverterRule {
    public static final PhysicalInsertConverterRule INSTANCE =
        Config.INSTANCE
            .withInTrait(LOGICAL)
            .withOutTrait(BATCH_PHYSICAL)
            .withRuleFactory(PhysicalInsertConverterRule::new)
            .withOperandSupplier(t -> t.operand(LogicalInsert.class).convert(LOGICAL))
            .withDescription("Converting logical insert into physical insert")
            .as(Config.class)
            .toRule(PhysicalInsertConverterRule.class);

    protected PhysicalInsertConverterRule(Config config) {
      super(config);
    }

    @Override
    public @Nullable RelNode convert(RelNode rel) {
      LogicalInsert logicalInsert = (LogicalInsert) rel;
      RelTraitSet newTraitSet = logicalInsert.getTraitSet().replace(BATCH_PHYSICAL);

      return new PhysicalInsert(
          logicalInsert.getCluster(),
          newTraitSet,
          logicalInsert.getTable(),
          logicalInsert.getCatalogReader(),
          logicalInsert.getInput(),
          logicalInsert.getUpdateColumnList());
    }
  }
}
