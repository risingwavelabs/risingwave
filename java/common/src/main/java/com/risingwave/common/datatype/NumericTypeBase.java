package com.risingwave.common.datatype;

import org.apache.calcite.sql.type.SqlTypeName;

public class NumericTypeBase extends PrimitiveTypeBase {
  // Data size in bytes
  private final int dataSize;

  public NumericTypeBase(SqlTypeName sqlTypeName, int dataSize) {
    this(true, sqlTypeName, dataSize);
  }

  public NumericTypeBase(boolean nullable, SqlTypeName sqlTypeName, int dataSize) {
    super(nullable, sqlTypeName);
    this.dataSize = dataSize;
  }

  @Override
  public String getFullTypeString() {
    return null;
  }
}
