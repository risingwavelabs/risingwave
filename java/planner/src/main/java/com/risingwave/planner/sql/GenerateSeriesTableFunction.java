package com.risingwave.planner.sql;

import static com.google.common.base.Verify.verify;
import static org.apache.calcite.sql.type.OperandTypes.family;

import com.google.common.collect.Lists;
import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.StructKind;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.SqlTableFunction;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlSingleOperandTypeChecker;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * Postgres doc: https://www.postgresql.org/docs/14/functions-srf.html <br>
 * <code>generate_series</code> returns an anonymous table during execution. For <code>
 * generate_series(INTEGER, INTEGER)</code>, it returns a table with one column of INTEGER.
 */
public class GenerateSeriesTableFunction extends SqlFunction implements SqlTableFunction {
  private static final SqlSingleOperandTypeChecker INTEGER_INTEGER =
      family(SqlTypeFamily.INTEGER, SqlTypeFamily.INTEGER);

  private final SqlReturnTypeInference rowType;

  private GenerateSeriesTableFunction(
      SqlSingleOperandTypeChecker operandChecker, SqlReturnTypeInference rowType) {
    // Table function must return CURSOR type.
    super(
        "generate_series",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.CURSOR,
        operandChecker.typeInference(),
        operandChecker,
        SqlFunctionCategory.NUMERIC);
    this.rowType = rowType;
  }

  /**
   * @return All the function overloads.
   */
  public static List<GenerateSeriesTableFunction> createAllOverloads() {
    return Lists.newArrayList(
        new GenerateSeriesTableFunction(
            INTEGER_INTEGER, GenerateSeriesTableFunction::createRowType));
  }

  @Override
  public SqlReturnTypeInference getRowTypeInference() {
    return this.rowType;
  }

  private static RelDataType createRowType(SqlOperatorBinding opBinding) {
    final RelDataTypeFactory typeFactory = opBinding.getTypeFactory();

    // Prints "generate_series(A, B)" as the column name.
    SqlPrettyWriter writer = new SqlPrettyWriter();
    verify(opBinding instanceof SqlCallBinding);
    ((SqlCallBinding) opBinding).getCall().unparse(writer, 0, 0);

    return typeFactory
        .builder()
        .kind(StructKind.FULLY_QUALIFIED)
        .add(writer.toString(), SqlTypeName.INTEGER, 1)
        .build();
  }
}
