package com.risingwave.planner.rel.physical;

import com.google.common.collect.ImmutableList;
import com.risingwave.catalog.ColumnCatalog;
import com.risingwave.catalog.TableCatalog;
import com.risingwave.planner.rel.common.RwScan;
import com.risingwave.planner.rel.common.dist.RwDistributions;
import com.risingwave.proto.plan.CellBasedTableDesc;
import com.risingwave.proto.plan.ColumnDesc;
import com.risingwave.proto.plan.PlanNode;
import com.risingwave.proto.plan.RowSeqScanNode;
import java.util.Collections;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.hint.RelHint;

/** Executor to scan from Materialized View */
public class RwBatchScan extends RwScan implements RisingWaveBatchPhyRel {

  protected RwBatchScan(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      List<RelHint> hints,
      RelOptTable table,
      TableCatalog.TableId tableId,
      ImmutableList<ColumnCatalog.ColumnId> columnIds) {
    super(cluster, traitSet, hints, table, tableId, columnIds);
    checkConvention();
  }

  public static RwBatchScan create(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelOptTable table,
      ImmutableList<ColumnCatalog.ColumnId> columnIds) {
    TableCatalog tableCatalog = table.unwrapOrThrow(TableCatalog.class);

    RelTraitSet newTraitSet = traitSet.plus(RisingWaveBatchPhyRel.BATCH_PHYSICAL);
    return new RwBatchScan(
        cluster, newTraitSet, Collections.emptyList(), table, tableCatalog.getId(), columnIds);
  }

  public RwBatchScan copy(RelTraitSet traitSet) {
    return new RwBatchScan(
        getCluster(), traitSet, getHints(), getTable(), getTableId(), getColumnIds());
  }

  public RwBatchScan copy(ImmutableList<ColumnCatalog.ColumnId> columnIds) {
    return new RwBatchScan(
        getCluster(), getTraitSet(), getHints(), getTable(), getTableId(), columnIds);
  }

  @Override
  public RelNode convertToDistributed() {
    return copy(getTraitSet().replace(BATCH_DISTRIBUTED).plus(RwDistributions.RANDOM_DISTRIBUTED));
  }

  @Override
  public PlanNode serialize() {
    var table = getTable().unwrapOrThrow(TableCatalog.class);

    // FIXME: here should add pk's desc in the table desc.
    var builder =
        RowSeqScanNode.newBuilder()
            .setTableDesc(CellBasedTableDesc.newBuilder().setTableId(tableId.getValue()).build());

    columnIds.forEach(
        c -> {
          var dataType = table.getColumnChecked(c).getDesc().getDataType().getProtobufType();
          var columnDescBuilder = ColumnDesc.newBuilder();
          columnDescBuilder.setColumnId(c.getValue());
          columnDescBuilder.setColumnType(dataType);
          columnDescBuilder.setName(table.getColumnChecked(c).getName());
          builder.addColumnDescs(columnDescBuilder.build());
        });
    return PlanNode.newBuilder()
        .setRowSeqScan(builder.build())
        .setIdentity(BatchPlan.getCurrentNodeIdentity(this))
        .build();
  }
}
