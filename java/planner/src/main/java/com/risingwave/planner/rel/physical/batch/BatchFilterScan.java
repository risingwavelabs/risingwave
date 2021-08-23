package com.risingwave.planner.rel.physical.batch;

import static com.risingwave.execution.context.ExecutionContext.contextOf;
import static com.risingwave.execution.handler.RpcExecutor.getTableRefId;
import static com.risingwave.planner.planner.PlannerUtils.isSingleMode;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.Any;
import com.risingwave.catalog.ColumnCatalog;
import com.risingwave.catalog.TableCatalog;
import com.risingwave.planner.rel.common.FilterScanBase;
import com.risingwave.planner.rel.common.dist.RwDistributions;
import com.risingwave.proto.plan.PlanNode;
import com.risingwave.proto.plan.SeqScanNode;
import com.risingwave.proto.plan.TableRefId;
import java.util.Collections;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.hint.RelHint;

public class BatchFilterScan extends FilterScanBase implements RisingWaveBatchPhyRel {
  protected BatchFilterScan(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      List<RelHint> hints,
      RelOptTable table,
      TableCatalog.TableId tableId,
      ImmutableList<ColumnCatalog.ColumnId> columnIds) {
    super(cluster, traitSet, hints, table, tableId, columnIds);
    checkConvention();
  }

  public static BatchFilterScan create(
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

    return new BatchFilterScan(
        cluster, newTraitSet, Collections.emptyList(), table, tableCatalog.getId(), columnIds);
  }

  @Override
  public PlanNode serialize() {
    TableRefId tableRefId = getTableRefId(tableId);
    SeqScanNode.Builder seqScanNodeBuilder = SeqScanNode.newBuilder().setTableRefId(tableRefId);
    columnIds.forEach(c -> seqScanNodeBuilder.addColumnIds(c.getValue()));
    return PlanNode.newBuilder()
        .setNodeType(PlanNode.PlanNodeType.SEQ_SCAN)
        .setBody(Any.pack(seqScanNodeBuilder.build()))
        .build();
  }
}
