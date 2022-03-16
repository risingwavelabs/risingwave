package com.risingwave.planner.rel.physical;

import static com.risingwave.planner.rel.logical.RisingWaveLogicalRel.LOGICAL;
import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableList;
import com.risingwave.catalog.MaterializedViewCatalog;
import com.risingwave.common.entity.EntityBase;
import com.risingwave.planner.rel.common.dist.RwDistributions;
import com.risingwave.planner.rel.logical.RwLogicalDelete;
import com.risingwave.proto.plan.DeleteNode;
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
 * Physical version delete operator.
 *
 * @see RwLogicalDelete
 */
public class RwBatchDelete extends TableModify implements RisingWaveBatchPhyRel {
  protected RwBatchDelete(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelOptTable table,
      Prepare.CatalogReader catalogReader,
      RelNode input) {
    super(cluster, traitSet, table, catalogReader, input, Operation.DELETE, null, null, false);
    checkConvention();

    var mvCatalog = getTable().unwrapOrThrow(MaterializedViewCatalog.class);
    patchScan(input, mvCatalog);
  }

  @Override
  public TableModify copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new RwBatchDelete(getCluster(), traitSet, getTable(), getCatalogReader(), sole(inputs));
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
    // For table v2, we can only delete from "associated materialized view".
    var mvCatalog = getTable().unwrapOrThrow(MaterializedViewCatalog.class);
    var tableSourceId =
        requireNonNull(mvCatalog.getAssociatedTableId(), "cannot delete from materialized view");

    DeleteNode.Builder deleteNodeBuilder = DeleteNode.newBuilder();
    deleteNodeBuilder.setTableSourceRefId(Messages.getTableRefId(tableSourceId));

    var identity = "RwBatchDeleteExecutor(TableName:" + this.table.getQualifiedName() + ")";

    return PlanNode.newBuilder()
        .setDelete(deleteNodeBuilder.build())
        .addChildren(((RisingWaveBatchPhyRel) input).serialize())
        .setIdentity(identity)
        .build();
  }

  // Patch the `RwBatchScan` to contain all columns including the hidden `_row_id`.
  private static void patchScan(RelNode node, MaterializedViewCatalog mvCatalog) {
    var inputLength = node.getInputs().size();
    for (var i = 0; i < inputLength; i++) {
      var input = node.getInput(i);
      if (input instanceof RwBatchScan) {
        var columnIds =
            mvCatalog.getAllColumnsV2().stream()
                .map(EntityBase::getId)
                .collect(ImmutableList.toImmutableList());
        node.replaceInput(i, ((RwBatchScan) input).copy(columnIds));
      }
      patchScan(input, mvCatalog);
    }
  }

  /** Delete converter rule between logical and physical. */
  public static class BatchDeleteConverterRule extends ConverterRule {
    public static final BatchDeleteConverterRule INSTANCE =
        Config.INSTANCE
            .withInTrait(LOGICAL)
            .withOutTrait(BATCH_PHYSICAL)
            .withRuleFactory(BatchDeleteConverterRule::new)
            .withOperandSupplier(t -> t.operand(RwLogicalDelete.class).anyInputs())
            .withDescription("Converting batch delete")
            .as(Config.class)
            .toRule(BatchDeleteConverterRule.class);

    protected BatchDeleteConverterRule(Config config) {
      super(config);
    }

    @Override
    public @Nullable RelNode convert(RelNode rel) {
      RwLogicalDelete logicalDelete = (RwLogicalDelete) rel;
      RelTraitSet requiredInputTraits =
          logicalDelete.getInput().getTraitSet().replace(BATCH_PHYSICAL);
      RelNode newInput = RelOptRule.convert(logicalDelete.getInput(), requiredInputTraits);

      return new RwBatchDelete(
          rel.getCluster(),
          rel.getTraitSet().plus(BATCH_PHYSICAL),
          logicalDelete.getTable(),
          logicalDelete.getCatalogReader(),
          newInput);
    }
  }
}
