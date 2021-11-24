package com.risingwave.planner.sql;

import com.risingwave.common.datatype.RwReturnTypes;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlMonotonicBinaryOperator;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.util.ReflectiveSqlOperatorTable;
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
}
