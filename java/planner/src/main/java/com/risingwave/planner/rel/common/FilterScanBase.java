package com.risingwave.planner.rel.common;

import com.google.common.collect.ImmutableList;
import com.risingwave.catalog.ColumnCatalog;
import com.risingwave.catalog.TableCatalog;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.hint.RelHint;

public class FilterScanBase extends TableScan {
  protected final TableCatalog.TableId tableId;
  protected final ImmutableList<ColumnCatalog.ColumnId> columnIds;

  protected FilterScanBase(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      List<RelHint> hints,
      RelOptTable table,
      TableCatalog.TableId tableId,
      ImmutableList<ColumnCatalog.ColumnId> columnIds) {
    super(cluster, traitSet, hints, table);
    this.tableId = tableId;
    this.columnIds = columnIds;
  }

  public TableCatalog.TableId getTableId() {
    return tableId;
  }

  public ImmutableList<ColumnCatalog.ColumnId> getColumnIds() {
    return columnIds;
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    super.explainTerms(pw);

    if (!columnIds.isEmpty()) {
      TableCatalog tableCatalog = getTable().unwrapOrThrow(TableCatalog.class);
      String columnNames =
          columnIds.stream()
              .map(tableCatalog::getColumnChecked)
              .map(ColumnCatalog::getEntityName)
              .map(ColumnCatalog.ColumnName::getValue)
              .collect(Collectors.joining(","));

      pw.item("columns", columnNames);
    }

    return pw;
  }
}
