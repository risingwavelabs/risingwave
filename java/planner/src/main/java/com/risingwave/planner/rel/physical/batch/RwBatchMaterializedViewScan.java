package com.risingwave.planner.rel.physical.batch;

import static com.risingwave.execution.context.ExecutionContext.contextOf;
import static com.risingwave.planner.planner.PlannerUtils.isSingleMode;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.Any;
import com.risingwave.catalog.ColumnCatalog;
import com.risingwave.catalog.TableCatalog;
import com.risingwave.planner.rel.common.RwScan;
import com.risingwave.planner.rel.common.dist.RwDistributions;
import com.risingwave.proto.plan.PlanNode;
import com.risingwave.proto.plan.RowSeqScanNode;
import com.risingwave.proto.plan.TableRefId;
import com.risingwave.rpc.Messages;
import java.util.Collections;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.hint.RelHint;

/** Executor to scan from Materialized View */
public class RwBatchMaterializedViewScan extends RwScan implements RisingWaveBatchPhyRel {

  protected RwBatchMaterializedViewScan(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      List<RelHint> hints,
      RelOptTable table,
      TableCatalog.TableId tableId,
      ImmutableList<ColumnCatalog.ColumnId> columnIds) {
    super(cluster, traitSet, hints, table, tableId, columnIds);
    checkConvention();
  }

  public static RwBatchMaterializedViewScan create(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelOptTable table,
      ImmutableList<ColumnCatalog.ColumnId> columnIds) {
    TableCatalog tableCatalog = table.unwrapOrThrow(TableCatalog.class);

    RelDistribution distTrait =
        isSingleMode(contextOf(cluster))
            ? RwDistributions.SINGLETON
            : RwDistributions.RANDOM_DISTRIBUTED;

    RelTraitSet newTraitSet = traitSet.plus(RisingWaveBatchPhyRel.BATCH_PHYSICAL).plus(distTrait);

    return new RwBatchMaterializedViewScan(
        cluster, newTraitSet, Collections.emptyList(), table, tableCatalog.getId(), columnIds);
  }

  @Override
  public PlanNode serialize() {
    TableRefId tableRefId = Messages.getTableRefId(tableId);
    RowSeqScanNode.Builder builder = RowSeqScanNode.newBuilder().setTableRefId(tableRefId);
    columnIds.forEach(c -> builder.addColumnIds(c.getValue()));
    return PlanNode.newBuilder()
        .setNodeType(PlanNode.PlanNodeType.ROW_SEQ_SCAN)
        .setBody(Any.pack(builder.build()))
        .build();
  }
}
