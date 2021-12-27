package com.risingwave.common.datatype;

import static org.apache.calcite.sql.type.ReturnTypes.BOOLEAN_NULLABLE_OPTIMIZED;

import java.util.stream.IntStream;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;

/** A collection of return-type inference strategies as supplements to {@link ReturnTypes}. */
public abstract class RwReturnTypes {

  // DATE + INTERVAL results with a DATE type.
  // This is postgres behavior.
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

  public static final SqlReturnTypeInference AGG_SUM =
      opBinding -> {
        final RelDataTypeFactory typeFactory = opBinding.getTypeFactory();
        final RelDataType type =
            ((RisingWaveDataTypeSystem) typeFactory.getTypeSystem())
                .deriveSumTypeLocal(typeFactory, opBinding.getOperandType(0));
        if (opBinding.getGroupCount() == 0 || opBinding.hasFilter()) {
          return typeFactory.createTypeWithNullability(type, true);
        } else {
          return type;
        }
      };

  public static final SqlReturnTypeInference RW_AND_BOOLEAN_NULLABLE =
      opBinding -> {
        // Make it simple first, don't take cast into account.
        var allLiterals =
            IntStream.range(0, opBinding.getOperandCount())
                .allMatch(idx -> opBinding.isOperandLiteral(idx, false));

        if (allLiterals) {
          var hasFalse =
              IntStream.range(0, opBinding.getOperandCount())
                  .anyMatch(
                      idx ->
                          Boolean.FALSE.equals(
                              opBinding.getOperandLiteralValue(idx, Boolean.class)));
          if (hasFalse) {
            return ((RisingWaveDataType)
                    opBinding.getTypeFactory().createSqlType(SqlTypeName.BOOLEAN))
                .withNullability(false);
          }
        }

        return BOOLEAN_NULLABLE_OPTIMIZED.inferReturnType(opBinding);
      };

  public static final SqlReturnTypeInference RW_OR_BOOLEAN_NULLABLE =
      opBinding -> {
        // Make it simple first, don't take cast into account.
        var allLiterals =
            IntStream.range(0, opBinding.getOperandCount())
                .allMatch(idx -> opBinding.isOperandLiteral(idx, false));

        if (allLiterals) {
          var hasTrue =
              IntStream.range(0, opBinding.getOperandCount())
                  .anyMatch(
                      idx ->
                          Boolean.TRUE.equals(
                              opBinding.getOperandLiteralValue(idx, Boolean.class)));
          if (hasTrue) {
            return ((RisingWaveDataType)
                    opBinding.getTypeFactory().createSqlType(SqlTypeName.BOOLEAN))
                .withNullability(false);
          }
        }

        return BOOLEAN_NULLABLE_OPTIMIZED.inferReturnType(opBinding);
      };
}
