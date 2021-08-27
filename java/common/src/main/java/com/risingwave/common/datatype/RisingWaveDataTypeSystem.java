package com.risingwave.common.datatype;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystemImpl;
import org.apache.calcite.sql.type.SqlTypeName;

public class RisingWaveDataTypeSystem extends RelDataTypeSystemImpl {
  public RelDataType deriveDateWithIntervalType(
      RelDataTypeFactory typeFactory, RelDataType type1, RelDataType type2) {
    if (type1.getSqlTypeName() == SqlTypeName.DATE
        && type2.getSqlTypeName() == SqlTypeName.INTERVAL_YEAR) {
      // No precision here. DATE +-*/ with INTERVAL infer 0 precision timestamp.
      return typeFactory.createSqlType(SqlTypeName.TIMESTAMP);
    } else {
      return null;
    }
  }
}
