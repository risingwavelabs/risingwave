package com.risingwave.planner.rel.physical.batch;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.risingwave.catalog.ColumnCatalog;
import com.risingwave.catalog.TableCatalog;
import com.risingwave.common.datatype.RisingWaveDataType;
import com.risingwave.common.exception.PgErrorCode;
import com.risingwave.common.exception.PgException;
import com.risingwave.execution.handler.RpcExecutor;
import com.risingwave.proto.data.DataType;
import com.risingwave.proto.expr.ConstantValue;
import com.risingwave.proto.expr.ExprNode;
import com.risingwave.proto.plan.InsertValueNode;
import com.risingwave.proto.plan.PlanNode;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.stream.Collectors;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.sql.SqlKind;

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
    checkArgument(traitSet.contains(RisingWaveBatchPhyRel.BATCH_PHYSICAL));
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
        RexLiteral value = tuples.get(i).get(j);
        DataType dataType = ((RisingWaveDataType) value.getType()).getProtobufType();

        // Build Expr Node.
        ExprNode.Builder exprNodeBuilder =
            ExprNode.newBuilder()
                .setExprType(ExprNode.ExprNodeType.CONSTANT_VALUE)
                .setBody(
                    Any.pack(
                        ConstantValue.newBuilder()
                            .setBody(ByteString.copyFrom(getBytesRepresentation(value, dataType)))
                            .build()))
                .setReturnType(dataType);

        // Add to Expr tuple.
        exprTupleBuilder.addCells(exprNodeBuilder);
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

  private static byte[] getBytesRepresentation(RexLiteral val, DataType dataType) {
    ByteBuffer bb;
    switch (dataType.getTypeName()) {
      case INT16:
        {
          bb = ByteBuffer.allocate(2).order(ByteOrder.BIG_ENDIAN);
          bb.putShort(
              requireNonNull(
                  val.getValueAs(Short.class),
                  "RexLiteral return a null value in byte array serialization!"));
          break;
        }
      case INT32:
        {
          bb = ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN);
          bb.putInt(
              requireNonNull(
                  val.getValueAs(Integer.class),
                  "RexLiteral return a null value in byte array serialization!"));
          break;
        }
      case INT64:
        {
          bb = ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN);
          bb.putLong(
              requireNonNull(
                  val.getValueAs(Long.class),
                  "RexLiteral return a null value in byte array serialization!"));
          break;
        }
      case FLOAT:
        {
          bb = ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN);
          bb.putFloat(
              requireNonNull(
                  val.getValueAs(Float.class),
                  "RexLiteral return a null value in byte array serialization!"));
          break;
        }
      case DOUBLE:
        {
          bb = ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN);
          bb.putDouble(
              requireNonNull(
                  val.getValueAs(Double.class),
                  "RexLiteral return a null value in byte array serialization!"));
          break;
        }
      case CHAR:
      case VARCHAR:
        {
          // FIXME: No overflow detection here.
          byte[] str =
              requireNonNull(
                      val.getValueAs(String.class),
                      "RexLiteral return a null value in byte array serialization!")
                  .getBytes(StandardCharsets.UTF_8);
          bb = ByteBuffer.allocate(str.length).order(ByteOrder.BIG_ENDIAN);
          bb.put(str);
          break;
        }
      default:
        throw new PgException(PgErrorCode.INTERNAL_ERROR, "Unsupported type: %s", dataType);
    }
    return bb.array();
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
}
