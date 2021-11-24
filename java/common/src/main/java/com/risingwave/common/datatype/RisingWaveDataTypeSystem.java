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

  @Override
  public int getMaxNumericScale() {
    return DecimalType.MAX_SCALE;
  }

  @Override
  public int getMaxNumericPrecision() {
    return DecimalType.MAX_PRECISION;
  }
}
