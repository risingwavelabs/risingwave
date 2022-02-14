package com.risingwave.planner.sql;

import com.risingwave.common.datatype.RwReturnTypes;
import com.risingwave.planner.sql.fun.RwSqlExtractFunction;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlMonotonicBinaryOperator;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.util.ReflectiveSqlOperatorTable;
import org.apache.calcite.util.Optionality;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;

/**
 * A collection of operators as supplements to {@link
 * org.apache.calcite.sql.fun.SqlStdOperatorTable}.
 */
public class RisingWaveOverrideOperatorTable extends ReflectiveSqlOperatorTable {
  private static @MonotonicNonNull RisingWaveOverrideOperatorTable instance;

  /** Returns the operator table instance, creating it if necessary. */
  public static synchronized RisingWaveOverrideOperatorTable instance() {
    if (instance == null) {
      // Creates and initializes the operator table.
      // Uses two-phase construction, because we can't initialize the
      // table until the constructor of the sub-class has completed.
      instance = new RisingWaveOverrideOperatorTable();
      instance.init();
    }
    return instance;
  }

  /** Infix arithmetic plus operator, '<code>+</code>'. */
  public static final SqlBinaryOperator PLUS =
      new SqlMonotonicBinaryOperator(
          "+",
          SqlKind.PLUS,
          40,
          true,
          RwReturnTypes.NULLABLE_SUM,
          InferTypes.FIRST_KNOWN,
          OperandTypes.PLUS_OPERATOR);

  /** Infix arithmetic minus operator, '<code>-</code>'. */
  public static final SqlBinaryOperator MINUS =
      new SqlMonotonicBinaryOperator(
          "-",
          SqlKind.MINUS,
          40,
          true,
          RwReturnTypes.NULLABLE_SUM,
          InferTypes.FIRST_KNOWN,
          OperandTypes.MINUS_OPERATOR);

  /** <code>SUM</code> aggregate function. */
  public static final SqlAggFunction SUM =
      new SqlAggFunction(
          "SUM",
          null,
          SqlKind.SUM,
          RwReturnTypes.AGG_SUM,
          null,
          OperandTypes.NUMERIC,
          SqlFunctionCategory.NUMERIC,
          false,
          false,
          Optionality.FORBIDDEN) {};

  public static final SqlFunction PG_SLEEP =
      new SqlFunction(
          "PG_SLEEP",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.INTEGER_NULLABLE,
          null,
          OperandTypes.family(SqlTypeFamily.DECIMAL),
          SqlFunctionCategory.TIMEDATE);

  /** Logical <code>AND</code> operator. */
  public static final SqlBinaryOperator AND =
      new RwSqlBinaryOperator(
          "AND",
          SqlKind.AND,
          24,
          true,
          RwReturnTypes.RW_AND_BOOLEAN_NULLABLE,
          InferTypes.BOOLEAN,
          OperandTypes.BOOLEAN_BOOLEAN);

  /** Logical <code>OR</code> operator. */
  public static final SqlBinaryOperator OR =
      new RwSqlBinaryOperator(
          "OR",
          SqlKind.OR,
          24,
          true,
          RwReturnTypes.RW_OR_BOOLEAN_NULLABLE,
          InferTypes.BOOLEAN,
          OperandTypes.BOOLEAN_BOOLEAN);

  /** Pg's boolne function */
  public static final SqlFunction BOOL_NE =
      new SqlFunction(
          "BOOLNE",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.BOOLEAN_NULLABLE,
          InferTypes.BOOLEAN,
          OperandTypes.BOOLEAN_BOOLEAN,
          SqlFunctionCategory.SYSTEM);

  /** Pg's booleq function */
  public static final SqlFunction BOOL_EQ =
      new SqlFunction(
          "BOOLEQ",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.BOOLEAN_NULLABLE,
          InferTypes.BOOLEAN,
          OperandTypes.BOOLEAN_BOOLEAN,
          SqlFunctionCategory.SYSTEM);

  /** Pg's translate function */
  public static final SqlFunction TRANSLATE =
      new SqlFunction(
          "TRANSLATE",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.VARCHAR_2000,
          InferTypes.RETURN_TYPE,
          OperandTypes.STRING_STRING_STRING,
          SqlFunctionCategory.SYSTEM);

  /** Pg's extract function */
  public static final SqlFunction EXTRACT = new RwSqlExtractFunction();
}
