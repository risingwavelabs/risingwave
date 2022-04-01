package com.risingwave.planner.sql;

import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.util.Litmus;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * We need this because the dirty logic in method {@link RwSqlBinaryOperator#validRexOperands},
 * which inherits from calcite.
 */
public class RwSqlBinaryOperator extends SqlBinaryOperator {
  /**
   * Creates a SqlBinaryOperator.
   *
   * @param name Name of operator
   * @param kind Kind
   * @param prec Precedence
   * @param leftAssoc Left-associativity
   * @param returnTypeInference Strategy to infer return type
   * @param operandTypeInference Strategy to infer operand types
   * @param operandTypeChecker Validator for operand types
   */
  public RwSqlBinaryOperator(
      String name,
      SqlKind kind,
      int prec,
      boolean leftAssoc,
      @Nullable SqlReturnTypeInference returnTypeInference,
      @Nullable SqlOperandTypeInference operandTypeInference,
      @Nullable SqlOperandTypeChecker operandTypeChecker) {
    super(
        name, kind, prec, leftAssoc, returnTypeInference, operandTypeInference, operandTypeChecker);
  }

  @Override
  public boolean validRexOperands(int count, Litmus litmus) {
    if (count != 2) {
      // Special exception for AND and OR.
      if ((this == RisingWaveOverrideOperatorTable.AND
              || this == RisingWaveOverrideOperatorTable.OR)
          && count > 2) {
        return true;
      }
      return litmus.fail("wrong operand count {} for {}", count, this);
    }
    return litmus.succeed();
  }
}
