package com.risingwave.common.datatype;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;

public class RisingWaveTypeFactory extends JavaTypeFactoryImpl {
  public RisingWaveTypeFactory() {
    super(new RisingWaveDataTypeSystem());
  }

  public RelDataType createSqlType(SqlTypeName sqlType) {
    switch (sqlType) {
      case INTEGER:
        return new NumericTypeBase(SqlTypeName.INTEGER, 4);
      case BIGINT:
        return new NumericTypeBase(SqlTypeName.BIGINT, 8);
      case FLOAT:
        return new NumericTypeBase(SqlTypeName.FLOAT, 4);
      case DOUBLE:
        return new NumericTypeBase(SqlTypeName.DOUBLE, 8);
      default:
        return super.createSqlType(sqlType);
    }
  }
}
