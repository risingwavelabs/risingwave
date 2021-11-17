package com.risingwave.planner.rel.physical.batch;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.Any;
import com.risingwave.catalog.ColumnCatalog;
import com.risingwave.catalog.TableCatalog;
import com.risingwave.planner.rel.common.RwScan;
import com.risingwave.planner.rel.common.dist.RwDistributions;
import com.risingwave.proto.plan.PlanNode;
import com.risingwave.proto.plan.StreamScanNode;
import com.risingwave.proto.plan.TableRefId;
import com.risingwave.rpc.Messages;
import java.util.Collections;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.hint.RelHint;

/** Executor to scan from a stream source (e.g. Kafka) */
public class RwBatchStreamScan extends RwScan implements RisingWaveBatchPhyRel {
  protected RwBatchStreamScan(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      List<RelHint> hints,
      RelOptTable table,
      TableCatalog.TableId tableId,
      ImmutableList<ColumnCatalog.ColumnId> columnIds) {
    super(cluster, traitSet, hints, table, tableId, columnIds);
    checkConvention();
  }

  public static RwBatchStreamScan create(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelOptTable table,
      ImmutableList<ColumnCatalog.ColumnId> columnIds) {
    TableCatalog tableCatalog = table.unwrapOrThrow(TableCatalog.class);

    RelTraitSet newTraitSet = traitSet.plus(RisingWaveBatchPhyRel.BATCH_PHYSICAL);

    return new RwBatchStreamScan(
        cluster, newTraitSet, Collections.emptyList(), table, tableCatalog.getId(), columnIds);
  }

  public RwBatchStreamScan copy(RelTraitSet traitSet) {
    return new RwBatchStreamScan(
        getCluster(), traitSet, getHints(), getTable(), getTableId(), getColumnIds());
  }

  @Override
  public RelNode convertToDistributed() {
    return copy(getTraitSet().replace(BATCH_DISTRIBUTED).plus(RwDistributions.RANDOM_DISTRIBUTED));
  }

  @Override
  public PlanNode serialize() {
    TableRefId tableRefId = Messages.getTableRefId(tableId);
    StreamScanNode.Builder streamScanNodeBuilder =
        StreamScanNode.newBuilder().setTableRefId(tableRefId);
    columnIds.forEach(c -> streamScanNodeBuilder.addColumnIds(c.getValue()));
    return PlanNode.newBuilder()
        .setNodeType(PlanNode.PlanNodeType.STREAM_SCAN)
        .setBody(Any.pack(streamScanNodeBuilder.build()))
        .build();
  }
}
