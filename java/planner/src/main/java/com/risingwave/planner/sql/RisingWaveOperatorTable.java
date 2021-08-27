package com.risingwave.planner.sql;

import com.risingwave.common.datatype.RisingWaveDataTypeSystem;
import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.fun.SqlMonotonicBinaryOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.validate.SqlNameMatcher;
import org.checkerframework.checker.nullness.qual.Nullable;

public class RisingWaveOperatorTable implements SqlOperatorTable {
  private final SqlStdOperatorTable delegation = SqlStdOperatorTable.instance();

  public static final SqlReturnTypeInference DATE_PLUS_INTERVAL =
      opBinding -> {
        RelDataTypeFactory typeFactory = opBinding.getTypeFactory();
        RelDataType type1 = opBinding.getOperandType(0);
        RelDataType type2 = opBinding.getOperandType(1);
        return ((RisingWaveDataTypeSystem) typeFactory.getTypeSystem())
            .deriveDateWithIntervalType(typeFactory, type1, type2);
      };

  public static final SqlReturnTypeInference NULLABLE_SUM =
      ReturnTypes.chain(
          DATE_PLUS_INTERVAL, ReturnTypes.DECIMAL_SUM_NULLABLE, ReturnTypes.LEAST_RESTRICTIVE);

  /** Infix arithmetic plus operator, '<code>+</code>'. */
  public static final SqlBinaryOperator PLUS =
      new SqlMonotonicBinaryOperator(
          "+",
          SqlKind.PLUS,
          40,
          true,
          NULLABLE_SUM,
          InferTypes.FIRST_KNOWN,
          OperandTypes.PLUS_OPERATOR);

  @Override
  public void lookupOperatorOverloads(
      SqlIdentifier opName,
      @Nullable SqlFunctionCategory category,
      SqlSyntax syntax,
      List<SqlOperator> operatorList,
      SqlNameMatcher nameMatcher) {
    // Here hack to override the behaviour for PLUS.
    // Add a rule in NULLABLE_SUM to infer the proper type.
    // FIXME: Should find a more elegant way.
    if (opName.getSimple() == "+" && syntax == SqlSyntax.BINARY) {
      operatorList.add(PLUS);
    } else {
      delegation.lookupOperatorOverloads(opName, category, syntax, operatorList, nameMatcher);
    }
  }

  @Override
  public List<SqlOperator> getOperatorList() {
    return delegation.getOperatorList();
  }
}
