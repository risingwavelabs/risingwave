package com.risingwave.planner.rel.physical;

import static com.risingwave.planner.rel.logical.RisingWaveLogicalRel.LOGICAL;
import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableList;
import com.risingwave.catalog.ColumnCatalog;
import com.risingwave.catalog.MaterializedViewCatalog;
import com.risingwave.catalog.TableCatalog;
import com.risingwave.planner.rel.common.dist.RwDistributions;
import com.risingwave.planner.rel.logical.RwLogicalInsert;
import com.risingwave.proto.plan.InsertNode;
import com.risingwave.proto.plan.PlanNode;
import com.risingwave.rpc.Messages;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.TableModify;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Physical version insert operator.
 *
 * @see RwLogicalInsert
 */
public class RwBatchInsert extends TableModify implements RisingWaveBatchPhyRel {
  protected RwBatchInsert(
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
  public TableModify copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new RwBatchInsert(
        getCluster(),
        traitSet,
        getTable(),
        getCatalogReader(),
        sole(inputs),
        getUpdateColumnList());
  }

  @Override
  public RelNode convertToDistributed() {
    return copy(
        getTraitSet().replace(BATCH_DISTRIBUTED).plus(RwDistributions.SINGLETON),
        ImmutableList.of(
            RelOptRule.convert(
                input,
                input.getTraitSet().replace(BATCH_DISTRIBUTED).plus(RwDistributions.SINGLETON))));
  }

  @Override
  public PlanNode serialize() {
    TableCatalog tableCatalog = getTable().unwrapOrThrow(TableCatalog.class);
    ImmutableList<Integer> columnIds;
    if (getUpdateColumnList() != null) {
      columnIds =
          getUpdateColumnList().stream()
              .map(tableCatalog::getColumnChecked)
              .map(ColumnCatalog::getId)
              .map(ColumnCatalog.ColumnId::getValue)
              .collect(ImmutableList.toImmutableList());
    } else {
      columnIds =
          tableCatalog.getAllColumns().stream()
              .map(c -> c.getId().getValue())
              .collect(ImmutableList.toImmutableList());
    }

    // For table v2, we can only insert into "associated materialized view".
    var mvCatalog = (MaterializedViewCatalog) tableCatalog;
    var tableSourceId =
        requireNonNull(mvCatalog.getAssociatedTableId(), "cannot insert into materialized view");

    InsertNode.Builder insertNodeBuilder =
        InsertNode.newBuilder().setTableSourceRefId(Messages.getTableRefId(tableSourceId));

    var identity =
        "RwBatchInsertExecutor(TableName:"
            + this.table.getQualifiedName()
            + ",ColumnIds:"
            + columnIds
            + ")";

    insertNodeBuilder.addAllColumnIds(columnIds);
    return PlanNode.newBuilder()
        .setInsert(insertNodeBuilder.build())
        .addChildren(((RisingWaveBatchPhyRel) input).serialize())
        .setIdentity(identity)
        .build();
  }

  /** Insert converter rule between logical and physical. */
  public static class BatchInsertConverterRule extends ConverterRule {
    public static final BatchInsertConverterRule INSTANCE =
        Config.INSTANCE
            .withInTrait(LOGICAL)
            .withOutTrait(BATCH_PHYSICAL)
            .withRuleFactory(BatchInsertConverterRule::new)
            .withOperandSupplier(t -> t.operand(RwLogicalInsert.class).anyInputs())
            .withDescription("Converting batch insert")
            .as(Config.class)
            .toRule(BatchInsertConverterRule.class);

    protected BatchInsertConverterRule(Config config) {
      super(config);
    }

    @Override
    public @Nullable RelNode convert(RelNode rel) {
      RwLogicalInsert logicalInsert = (RwLogicalInsert) rel;
      RelTraitSet requiredInputTraits =
          logicalInsert.getInput().getTraitSet().replace(BATCH_PHYSICAL);
      RelNode newInput = RelOptRule.convert(logicalInsert.getInput(), requiredInputTraits);
      return new RwBatchInsert(
          rel.getCluster(),
          rel.getTraitSet().plus(BATCH_PHYSICAL),
          logicalInsert.getTable(),
          logicalInsert.getCatalogReader(),
          newInput,
          logicalInsert.getUpdateColumnList());
    }
  }
}
