package com.risingwave.planner.rel.logical;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.risingwave.catalog.ColumnCatalog;
import com.risingwave.catalog.TableCatalog;
import com.risingwave.planner.rel.common.FilterScanBase;
import java.util.Collections;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.hint.RelHint;

public class RwFilterScan extends FilterScanBase implements RisingWaveLogicalRel {

  private RwFilterScan(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      List<RelHint> hints,
      RelOptTable table,
      TableCatalog.TableId tableId,
      ImmutableList<ColumnCatalog.ColumnId> columnIds) {
    super(cluster, traitSet, hints, table, tableId, columnIds);
    Preconditions.checkArgument(
        traitSet.contains(RisingWaveLogicalRel.LOGICAL), "Not logical convention.");
  }

  public RwFilterScan copy(ImmutableList<ColumnCatalog.ColumnId> columnIds) {
    return new RwFilterScan(
        getCluster(), getTraitSet(), getHints(), getTable(), tableId, columnIds);
  }

  public static RwFilterScan create(
      RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table) {
    TableCatalog tableCatalog = table.unwrapOrThrow(TableCatalog.class);
    RelTraitSet newTraitSet = traitSet.replace(RisingWaveLogicalRel.LOGICAL);

    return new RwFilterScan(
        cluster,
        newTraitSet,
        Collections.emptyList(),
        table,
        tableCatalog.getId(),
        tableCatalog.getAllColumnIdsSorted());
  }
}
