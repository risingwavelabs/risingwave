package com.risingwave.common.datatype;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;

/** A collection of return-type inference strategies as supplements to {@link ReturnTypes}. */
public abstract class RwReturnTypes {
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
}
