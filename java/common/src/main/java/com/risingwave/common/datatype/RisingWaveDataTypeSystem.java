package com.risingwave.common.datatype;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystemImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;

/** Customized DataTypeSystem implementation. */
public class RisingWaveDataTypeSystem extends RelDataTypeSystemImpl {
  public RelDataType deriveDateWithIntervalType(
      RelDataTypeFactory typeFactory, RelDataType type1, RelDataType type2) {
    if (SqlTypeUtil.isDate(type1) && SqlTypeUtil.isInterval(type2)) {
      // No precision here. DATE +-*/ with INTERVAL infer 0 precision timestamp.
      return typeFactory.createSqlType(SqlTypeName.TIMESTAMP);
    } else {
      return null;
    }
  }

  /**
   * Derive the return type for sum aggregate function. It does not override {@link
   * RelDataTypeSystemImpl#deriveSumType} because the default rule is still used by the global
   * phase.
   */
  public RelDataType deriveSumTypeLocal(RelDataTypeFactory typeFactory, RelDataType argumentType) {
    switch (argumentType.getSqlTypeName()) {
      case SMALLINT:
      case INTEGER:
        return typeFactory.createTypeWithNullability(
            typeFactory.createSqlType(SqlTypeName.BIGINT), argumentType.isNullable());
      case BIGINT:
        return typeFactory.createTypeWithNullability(
            typeFactory.createSqlType(SqlTypeName.DECIMAL, getMaxNumericPrecision(), 0),
            argumentType.isNullable());
      default:
        return super.deriveSumType(typeFactory, argumentType);
    }
  }

  @Override
  public int getMaxNumericScale() {
    return DecimalType.MAX_SCALE;
  }

  @Override
  public int getMaxNumericPrecision() {
    return DecimalType.MAX_PRECISION;
  }
}
