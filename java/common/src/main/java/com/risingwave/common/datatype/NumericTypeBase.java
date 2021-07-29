package com.risingwave.common.datatype;

import com.google.common.base.Objects;
import org.apache.calcite.sql.type.SqlTypeName;

public class NumericTypeBase extends PrimitiveTypeBase {
  // Data size in bytes
  private final int dataSize;

  public NumericTypeBase(SqlTypeName sqlTypeName, int dataSize) {
    this(false, sqlTypeName, dataSize);
    resetDigest();
  }

  public NumericTypeBase(boolean nullable, SqlTypeName sqlTypeName, int dataSize) {
    super(nullable, sqlTypeName);
    this.dataSize = dataSize;
  }

  @Override
  protected void generateTypeString(StringBuilder sb, boolean withDetail) {
    sb.append(getSqlTypeName().name());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    NumericTypeBase that = (NumericTypeBase) o;
    return super.equals(o) && dataSize == that.dataSize;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(dataSize);
  }
}
