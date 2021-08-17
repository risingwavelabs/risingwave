package com.risingwave.common.datatype;

import com.google.common.base.Objects;
import com.risingwave.common.exception.PgErrorCode;
import com.risingwave.common.exception.PgException;
import com.risingwave.proto.data.DataType;
import com.risingwave.proto.data.DataType.TypeName;
import org.apache.calcite.rel.type.RelDataTypeComparability;
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

  @Override
  protected PrimitiveTypeBase copyWithNullability(boolean nullable) {
    return new NumericTypeBase(nullable, getSqlTypeName(), this.dataSize);
  }

  @Override
  public RelDataTypeComparability getComparability() {
    return RelDataTypeComparability.ALL;
  }

  @Override
  public DataType getProtobufType() {
    switch (getSqlTypeName()) {
      case SMALLINT:
        return buildProtobufType(TypeName.INT16);
      case INTEGER:
        return buildProtobufType(TypeName.INT32);
      case BIGINT:
        return buildProtobufType(TypeName.INT64);
      case FLOAT:
        return buildProtobufType(TypeName.FLOAT);
      case DOUBLE:
        return buildProtobufType(TypeName.DOUBLE);
      default:
        throw new PgException(PgErrorCode.INTERNAL_ERROR, "unsupported type: %s", getSqlTypeName());
    }
  }

  private static DataType buildProtobufType(TypeName typeName) {
    return DataType.newBuilder().setTypeName(typeName).build();
  }
}
