package com.risingwave.planner.sql.fun;

import static org.apache.calcite.sql.validate.SqlNonNullableAccessors.getOperandLiteralValueOrThrow;

import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlMonotonicity;
import org.apache.calcite.util.Util;

/**
 * Override of {@link org.apache.calcite.sql.fun.SqlExtractFunction}.
 *
 * <p>We need this to override its return type.
 */
public class RwSqlExtractFunction extends SqlFunction {
  private static final SqlReturnTypeInference RETURN_TYPE =
      ReturnTypes.explicit(factory -> factory.createSqlType(SqlTypeName.DECIMAL));
  // ~ Constructors -----------------------------------------------------------

  // SQL2003, Part 2, Section 4.4.3 - extract returns a exact numeric
  public RwSqlExtractFunction() {
    super(
        "EXTRACT",
        SqlKind.EXTRACT,
        RETURN_TYPE,
        null,
        OperandTypes.INTERVALINTERVAL_INTERVALDATETIME,
        SqlFunctionCategory.SYSTEM);
  }

  // ~ Methods ----------------------------------------------------------------

  @Override
  public String getSignatureTemplate(int operandsCount) {
    Util.discard(operandsCount);
    return "{0}({1} FROM {2})";
  }

  @Override
  public void unparse(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
    final SqlWriter.Frame frame = writer.startFunCall(getName());
    call.operand(0).unparse(writer, 0, 0);
    writer.sep("FROM");
    call.operand(1).unparse(writer, 0, 0);
    writer.endFunCall(frame);
  }

  @Override
  public SqlMonotonicity getMonotonicity(SqlOperatorBinding call) {
    TimeUnitRange value = getOperandLiteralValueOrThrow(call, 0, TimeUnitRange.class);
    switch (value) {
      case YEAR:
        return call.getOperandMonotonicity(1).unstrict();
      default:
        return SqlMonotonicity.NOT_MONOTONIC;
    }
  }
}
