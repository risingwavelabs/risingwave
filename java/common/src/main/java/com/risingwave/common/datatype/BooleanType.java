package com.risingwave.common.datatype;

import com.risingwave.proto.data.DataType;
import org.apache.calcite.rel.type.RelDataTypeComparability;
import org.apache.calcite.sql.type.SqlTypeName;

public class BooleanType extends PrimitiveTypeBase {
  public BooleanType() {
    this(false);
  }

  public BooleanType(boolean nullable) {
    super(nullable, SqlTypeName.BOOLEAN);
    resetDigest();
  }

  @Override
  public DataType getProtobufType() {
    return DataType.newBuilder()
        .setTypeName(DataType.TypeName.BOOLEAN)
        .setIsNullable(nullable)
        .build();
  }

  @Override
  protected void generateTypeString(StringBuilder sb, boolean withDetail) {
    sb.append(sqlTypeName);
  }

  @Override
  public RelDataTypeComparability getComparability() {
    return RelDataTypeComparability.ALL;
  }

  @Override
  protected PrimitiveTypeBase copyWithNullability(boolean nullable) {
    return new BooleanType(nullable);
  }
}
