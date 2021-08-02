package com.risingwave.planner.rel.physical.batch;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.Any;
import com.risingwave.catalog.ColumnCatalog;
import com.risingwave.catalog.TableCatalog;
import com.risingwave.planner.rel.common.FilterScanBase;
import com.risingwave.proto.plan.DatabaseRefId;
import com.risingwave.proto.plan.FilterScanNode;
import com.risingwave.proto.plan.PlanNode;
import com.risingwave.proto.plan.SchemaRefId;
import com.risingwave.proto.plan.TableRefId;
import java.util.Collections;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
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
    checkArgument(traitSet.contains(RisingWaveBatchPhyRel.BATCH_PHYSICAL));
  }

  public static BatchFilterScan create(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelOptTable table,
      ImmutableList<ColumnCatalog.ColumnId> columnIds) {
    TableCatalog tableCatalog = table.unwrapOrThrow(TableCatalog.class);
    RelTraitSet newTraitSet = traitSet.replace(RisingWaveBatchPhyRel.BATCH_PHYSICAL);

    return new BatchFilterScan(
        cluster, newTraitSet, Collections.emptyList(), table, tableCatalog.getId(), columnIds);
  }

  @Override
  public PlanNode serialize() {
    FilterScanNode.Builder filterScanNodeBuilder =
        FilterScanNode.newBuilder()
            .setTableRefId(
                TableRefId.newBuilder()
                    .setSchemaRefId(
                        SchemaRefId.newBuilder()
                            .setDatabaseRefId(DatabaseRefId.newBuilder().setDatabaseId(0))
                            .setSchemaId(0))
                    .setTableId(tableId.getValue()));
    columnIds.forEach(c -> filterScanNodeBuilder.addColumnIds(c.getValue()));

    return PlanNode.newBuilder()
        .setNodeType(PlanNode.PlanNodeType.FILTER_SCAN)
        .setBody(Any.pack(filterScanNodeBuilder.build()))
        .build();
  }
}
