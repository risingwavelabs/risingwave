package com.risingwave.planner.rel.physical.batch;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableList;
import com.risingwave.catalog.ColumnCatalog;
import com.risingwave.catalog.TableCatalog;
import com.risingwave.proto.plan.PlanNode;
import java.util.stream.Collectors;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.sql.SqlKind;

public class PhysicalInsertValues extends AbstractRelNode implements RisingWaveBatchPhyRel {
  private final TableCatalog table;
  private final ImmutableList<ColumnCatalog.ColumnId> columnIds;
  private final ImmutableList<ImmutableList<RexLiteral>> tuples;

  public PhysicalInsertValues(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      TableCatalog table,
      ImmutableList<ColumnCatalog.ColumnId> columnIds,
      ImmutableList<ImmutableList<RexLiteral>> tuples) {
    super(cluster, traitSet);
    this.table = requireNonNull(table, "Table can't be null!");
    this.columnIds = requireNonNull(columnIds, "columnIds can't be null!");
    this.tuples = requireNonNull(tuples, "tuples can't be null!");
    checkArgument(traitSet.contains(RisingWaveBatchPhyRel.BATCH_PHYSICAL));
  }

  @Override
  public PlanNode serialize() {
    throw new UnsupportedOperationException();
  }

  @Override
  protected RelDataType deriveRowType() {
    return RelOptUtil.createDmlRowType(SqlKind.INSERT, getCluster().getTypeFactory());
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    super.explainTerms(pw);

    pw.item("table", table.getEntityName().getValue());

    if (!columnIds.isEmpty()) {
      String columnNames = table.joinColumnNames(columnIds, ",");
      pw.item("columns", columnNames);
    }

    String values =
        tuples.stream()
            .map(PhysicalInsertValues::toString)
            .collect(Collectors.joining(",", "(", ")"));
    pw.item("values", values);
    return pw;
  }

  private static String toString(ImmutableList<RexLiteral> row) {
    requireNonNull(row, "row");
    return row.stream().map(RexLiteral::toString).collect(Collectors.joining(",", "(", ")"));
  }
}
