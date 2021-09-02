package com.risingwave.planner.rel.logical;

import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableList;
import com.risingwave.catalog.ColumnCatalog;
import com.risingwave.catalog.TableCatalog;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;

public class RwLogicalInsertValues extends AbstractRelNode implements RisingWaveLogicalRel {
  private final TableCatalog table;
  private final ImmutableList<ColumnCatalog.ColumnId> columnIds;
  // We change this to rex node here since we may have cast expression.
  private final ImmutableList<ImmutableList<RexNode>> tuples;

  public RwLogicalInsertValues(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      TableCatalog table,
      ImmutableList<ColumnCatalog.ColumnId> columnIds,
      ImmutableList<ImmutableList<RexNode>> tuples) {
    super(cluster, traitSet);
    this.table = requireNonNull(table, "Table can't be null!");
    this.columnIds = requireNonNull(columnIds, "columnIds can't be null!");
    this.tuples = requireNonNull(tuples, "tuples can't be null!");
    checkConvention();
  }

  public TableCatalog getTableCatalog() {
    return table;
  }

  public ImmutableList<ColumnCatalog.ColumnId> getColumnIds() {
    return columnIds;
  }

  public ImmutableList<ImmutableList<RexNode>> getTuples() {
    return tuples;
  }

  @Override
  protected RelDataType deriveRowType() {
    return RelOptUtil.createDmlRowType(SqlKind.INSERT, getCluster().getTypeFactory());
  }
}
