package com.risingwave.planner.rel.serialization;

import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.risingwave.common.datatype.RisingWaveDataType;
import com.risingwave.common.datatype.RisingWaveTypeFactory;
import com.risingwave.common.exception.PgErrorCode;
import com.risingwave.common.exception.PgException;
import com.risingwave.proto.data.DataType;
import com.risingwave.proto.expr.ConstantValue;
import com.risingwave.proto.expr.ExprNode;
import com.risingwave.proto.expr.FunctionCall;
import com.risingwave.proto.expr.InputRefExpr;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.TimeString;
import org.apache.calcite.util.TimestampString;
import org.apache.calcite.util.TimestampWithTimeZoneString;

public class RexToProtoSerializer extends RexVisitorImpl<ExprNode> {
  private static final ImmutableMap<SqlKind, ExprNode.ExprNodeType> SQL_TO_FUNC_MAPPING =
      ImmutableMap.<SqlKind, ExprNode.ExprNodeType>builder()
          .put(SqlKind.CAST, ExprNode.ExprNodeType.CAST)
          .put(SqlKind.PLUS, ExprNode.ExprNodeType.ADD)
          .put(SqlKind.MINUS, ExprNode.ExprNodeType.SUBTRACT)
          .put(SqlKind.TIMES, ExprNode.ExprNodeType.MULTIPLY)
          .put(SqlKind.DIVIDE, ExprNode.ExprNodeType.DIVIDE)
          .put(SqlKind.EQUALS, ExprNode.ExprNodeType.EQUAL)
          .put(SqlKind.NOT_EQUALS, ExprNode.ExprNodeType.NOT_EQUAL)
          .put(SqlKind.LESS_THAN, ExprNode.ExprNodeType.LESS_THAN)
          .put(SqlKind.LESS_THAN_OR_EQUAL, ExprNode.ExprNodeType.LESS_THAN_OR_EQUAL)
          .put(SqlKind.GREATER_THAN, ExprNode.ExprNodeType.GREATER_THAN)
          .put(SqlKind.GREATER_THAN_OR_EQUAL, ExprNode.ExprNodeType.GREATER_THAN_OR_EQUAL)
          .put(SqlKind.AND, ExprNode.ExprNodeType.AND)
          .put(SqlKind.OR, ExprNode.ExprNodeType.OR)
          .put(SqlKind.NOT, ExprNode.ExprNodeType.NOT)
          .put(SqlKind.SUM, ExprNode.ExprNodeType.SUM)
          .put(SqlKind.COUNT, ExprNode.ExprNodeType.COUNT)
          .put(SqlKind.MIN, ExprNode.ExprNodeType.MIN)
          .put(SqlKind.MAX, ExprNode.ExprNodeType.MAX)
          .build();
  private static final ImmutableMap<String, ExprNode.ExprNodeType> STRING_TO_FUNC_MAPPING =
      ImmutableMap.<String, ExprNode.ExprNodeType>builder()
          .put("SUBSTRING", ExprNode.ExprNodeType.SUBSTR)
          .build();

  public RexToProtoSerializer() {
    super(true);
  }

  private static byte[] getBytesRepresentation(RexLiteral val, DataType dataType) {
    requireNonNull(val.getValue(), "val.value");
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
      case DECIMAL:
        {
          bb =
              ByteBuffer.wrap(
                  val.getValueAs(BigDecimal.class).toString().getBytes(StandardCharsets.UTF_8));
          break;
        }
      case CHAR:
      case VARCHAR:
        {
          bb =
              ByteBuffer.wrap(
                  val.getValueAs(NlsString.class).getValue().getBytes(StandardCharsets.UTF_8));
          break;
        }
      case DATE:
        {
          bb =
              ByteBuffer.wrap(
                  val.getValueAs(DateString.class).toString().getBytes(StandardCharsets.UTF_8));
          break;
        }
      case TIME:
        {
          bb =
              ByteBuffer.wrap(
                  val.getValueAs(TimeString.class).toString().getBytes(StandardCharsets.UTF_8));
          break;
        }
      case TIMESTAMP:
        {
          bb =
              ByteBuffer.wrap(
                  val.getValueAs(TimestampString.class)
                      .toString()
                      .getBytes(StandardCharsets.UTF_8));
          break;
        }
      case INTERVAL:
        {
          switch (dataType.getIntervalType()) {
            case YEAR:
              {
                bb = ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN);
                bb.putInt(
                    requireNonNull(
                        val.getValueAs(Integer.class),
                        "RexLiteral return a null value in byte array serialization!"));
                break;
              }
            default:
              throw new PgException(PgErrorCode.INTERNAL_ERROR, "Unsupported type: %s", dataType);
          }
          break;
        }
      default:
        throw new PgException(PgErrorCode.INTERNAL_ERROR, "Unsupported type: %s", dataType);
    }
    return bb.array();
  }

  @Override
  public ExprNode visitLiteral(RexLiteral literal) {
    RisingWaveDataType dataType = (RisingWaveDataType) literal.getType();
    var sqlTypeName = dataType.getSqlTypeName();
    if (sqlTypeName == SqlTypeName.DECIMAL) {
      // Note that the BigDecimal seems to define negative scale value in the doc, which is
      // inconsistent with PG. Currently we do not allow scale to be negative.
      var decimalLiteral = literal.getValueAs(BigDecimal.class);
      dataType =
          new RisingWaveTypeFactory()
              .createSqlType(
                  SqlTypeName.DECIMAL, decimalLiteral.precision(), decimalLiteral.scale());
    }

    DataType protoDataType = dataType.getProtobufType();
    var retExpr = makeConstantExpr(literal, protoDataType, protoDataType);

    // Hack here. It's not elegant. FIXME: Should remove this after Constant Folding.
    // It is directly creating a cast expression instead of constant value expression
    // for literal like time/timestamp/date.
    if (sqlTypeName == SqlTypeName.DATE
        || sqlTypeName == SqlTypeName.TIME
        || sqlTypeName == SqlTypeName.TIMESTAMP
        || sqlTypeName == SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE) {
      var constExpr =
          makeConstantExpr(
              literal,
              DataType.newBuilder()
                  .setIsNullable(false)
                  .setTypeName(DataType.TypeName.CHAR)
                  .setPrecision(getPrecision(literal))
                  .build(),
              dataType.getProtobufType());
      var children = new ArrayList<ExprNode>();
      children.add(constExpr);
      var callExpr = makeFunctionCallExpr(children, protoDataType, ExprNode.ExprNodeType.CAST);
      return callExpr;
    }
    return retExpr;
  }

  @Override
  public ExprNode visitInputRef(RexInputRef inputRef) {
    int columnIdx = inputRef.getIndex();
    DataType dataType = ((RisingWaveDataType) inputRef.getType()).getProtobufType();
    InputRefExpr inputRefExpr = InputRefExpr.newBuilder().setColumnIdx(columnIdx).build();
    return ExprNode.newBuilder()
        .setExprType(ExprNode.ExprNodeType.INPUT_REF)
        .setBody(Any.pack(inputRefExpr))
        .setReturnType(dataType)
        .build();
  }

  @Override
  public ExprNode visitCall(RexCall call) {
    DataType protoDataType = ((RisingWaveDataType) call.getType()).getProtobufType();
    List<ExprNode> children =
        call.getOperands().stream()
            .map(rexNode -> rexNode.accept(this))
            .collect(Collectors.toList());
    return makeFunctionCallExpr(
        children, protoDataType, funcCallOf(call.getKind(), call.getOperator().getName()));
  }

  private static ExprNode.ExprNodeType funcCallOf(SqlKind kind, String name) {
    if (kind == SqlKind.OTHER_FUNCTION) {
      return Optional.of(STRING_TO_FUNC_MAPPING.get(name))
          .orElseThrow(
              () ->
                  new PgException(
                      PgErrorCode.INTERNAL_ERROR, "Unmappable function call:" + " %s", name));
    }
    return Optional.of(SQL_TO_FUNC_MAPPING.get(kind))
        .orElseThrow(
            () ->
                new PgException(
                    PgErrorCode.INTERNAL_ERROR, "Unmappable function call:" + " %s", kind));
  }

  /**
   * Make a constant expression. Regularly, `returnProtoDataType` are the same with `protoDataType`.
   * But For type like date/time/timestamp/timestampz, in order to wrap a type cast here, use normal
   * type for get bytes representation and set return type to be `CHAR`.
   */
  private static ExprNode makeConstantExpr(
      RexLiteral literal, DataType returnProtoDataType, DataType protoDataType) {
    return ExprNode.newBuilder()
        .setExprType(ExprNode.ExprNodeType.CONSTANT_VALUE)
        .setBody(
            Any.pack(
                ConstantValue.newBuilder()
                    .setBody(
                        ByteString.copyFrom(
                            RexToProtoSerializer.getBytesRepresentation(literal, protoDataType)))
                    .build()))
        .setReturnType(returnProtoDataType)
        .build();
  }

  private static ExprNode makeFunctionCallExpr(
      List<ExprNode> children, DataType protoDataType, ExprNode.ExprNodeType exprType) {
    FunctionCall body = FunctionCall.newBuilder().addAllChildren(children).build();

    return ExprNode.newBuilder()
        .setExprType(exprType)
        .setReturnType(protoDataType)
        .setBody(Any.pack(body))
        .build();
  }

  private static int getPrecision(RexLiteral literal) {
    RisingWaveDataType type = (RisingWaveDataType) literal.getType();
    var sqlTypeName = type.getSqlTypeName();
    if (sqlTypeName == SqlTypeName.DATE) {
      return requireNonNull(literal.getValueAs(DateString.class), "value").toString().length();
    } else if (sqlTypeName == SqlTypeName.TIME) {
      return requireNonNull(literal.getValueAs(TimeString.class), "value").toString().length();
    } else if (sqlTypeName == SqlTypeName.TIMESTAMP) {
      return requireNonNull(literal.getValueAs(TimestampString.class), "value").toString().length();
    } else if (sqlTypeName == SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE) {
      return requireNonNull(literal.getValueAs(TimestampWithTimeZoneString.class), "value")
          .toString()
          .length();
    } else {
      throw new RuntimeException(
          "Only support cast date/time/timestamp/timestampz " + "type in RexToProtoSerializer");
    }
  }
}
