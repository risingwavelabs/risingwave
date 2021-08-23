package com.risingwave.planner.rel.physical.batch;

import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.Any;
import com.risingwave.catalog.ColumnCatalog;
import com.risingwave.catalog.TableCatalog;
import com.risingwave.execution.handler.RpcExecutor;
import com.risingwave.planner.rel.serialization.RexToProtoSerializer;
import com.risingwave.proto.plan.InsertValueNode;
import com.risingwave.proto.plan.PlanNode;
import java.nio.charset.StandardCharsets;
import java.util.stream.Collectors;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.TimeString;
import org.apache.calcite.util.TimestampString;

public class BatchInsertValues extends AbstractRelNode implements RisingWaveBatchPhyRel {
  private final TableCatalog table;
  private final ImmutableList<ColumnCatalog.ColumnId> columnIds;
  private final ImmutableList<ImmutableList<RexLiteral>> tuples;

  public BatchInsertValues(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      TableCatalog table,
      ImmutableList<ColumnCatalog.ColumnId> columnIds,
      ImmutableList<ImmutableList<RexLiteral>> tuples) {
    super(cluster, traitSet);
    this.table = requireNonNull(table, "Table can't be null!");
    this.columnIds = requireNonNull(columnIds, "columnIds can't be null!");
    this.tuples = requireNonNull(tuples, "tuples can't be null!");
    checkConvention();
  }

  @Override
  public PlanNode serialize() {
    InsertValueNode.Builder insertValueNodeBuilder =
        InsertValueNode.newBuilder().setTableRefId(RpcExecutor.getTableRefId(table.getId()));
    // TODO: Only consider constant values (no casting) for now.
    for (ColumnCatalog columnCatalog : table.getAllColumnCatalogs()) {
      insertValueNodeBuilder.addColumnIds(columnCatalog.getId().getValue());
    }

    for (int i = 0; i < tuples.size(); ++i) {
      ImmutableList<RexLiteral> tuple = tuples.get(i);
      InsertValueNode.ExprTuple.Builder exprTupleBuilder = InsertValueNode.ExprTuple.newBuilder();
      for (int j = 0; j < tuple.size(); ++j) {
        AddCastVisitor addCastVisitor = new AddCastVisitor(getCluster().getRexBuilder());
        RexNode value = tuples.get(i).get(j).accept(addCastVisitor);

        RexToProtoSerializer rexToProtoSerializer = new RexToProtoSerializer();

        // Add to Expr tuple.
        exprTupleBuilder.addCells(value.accept(rexToProtoSerializer));
      }
      insertValueNodeBuilder.addInsertTuples(exprTupleBuilder.build());
    }

    return PlanNode.newBuilder()
        .setNodeType(PlanNode.PlanNodeType.INSERT_VALUE)
        .setBody(Any.pack(insertValueNodeBuilder.build()))
        .build();
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
        tuples.stream().map(BatchInsertValues::toString).collect(Collectors.joining(",", "(", ")"));
    pw.item("values", values);
    return pw;
  }

  private static String toString(ImmutableList<RexLiteral> row) {
    requireNonNull(row, "row");
    return row.stream().map(RexLiteral::toString).collect(Collectors.joining(",", "(", ")"));
  }

  private static class AddCastVisitor extends RexVisitorImpl<RexNode> {
    private final RexBuilder rexBuilder;

    private AddCastVisitor(RexBuilder rexBuilder) {
      super(true);
      this.rexBuilder = rexBuilder;
    }

    @Override
    public RexNode visitLiteral(RexLiteral literal) {
      switch (literal.getType().getSqlTypeName()) {
        case DATE:
          return addCastToDate(literal);
        case TIMESTAMP:
          return addCastToTimestamp(literal);
        case TIME:
          return addCastToTime(literal);
        default:
          return literal;
      }
    }

    private RexNode addCastToDate(RexLiteral literal) {
      DateString value = requireNonNull(literal.getValueAs(DateString.class), "value");
      RexLiteral newLiteral =
          rexBuilder.makeCharLiteral(
              new NlsString(
                  value.toString(), StandardCharsets.UTF_8.name(), SqlCollation.IMPLICIT));

      RexNode castNode = rexBuilder.makeAbstractCast(literal.getType(), newLiteral);
      return castNode;
    }

    private RexNode addCastToTime(RexLiteral literal) {
      TimeString value = requireNonNull(literal.getValueAs(TimeString.class), "value");
      RexLiteral newLiteral =
          rexBuilder.makeCharLiteral(
              new NlsString(
                  value.toString(), StandardCharsets.UTF_8.name(), SqlCollation.IMPLICIT));

      RexNode castNode = rexBuilder.makeAbstractCast(literal.getType(), newLiteral);
      return castNode;
    }

    private RexNode addCastToTimestamp(RexLiteral literal) {
      TimestampString value = requireNonNull(literal.getValueAs(TimestampString.class), "value");
      RexLiteral newLiteral =
          rexBuilder.makeCharLiteral(
              new NlsString(
                  value.toString(), StandardCharsets.UTF_8.name(), SqlCollation.IMPLICIT));

      RexNode castNode = rexBuilder.makeAbstractCast(literal.getType(), newLiteral);
      return castNode;
    }
  }
}
