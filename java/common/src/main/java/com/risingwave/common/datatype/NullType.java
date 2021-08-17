package com.risingwave.common.datatype;

import com.risingwave.proto.data.DataType;
import org.apache.calcite.rel.type.RelDataTypeComparability;
import org.apache.calcite.sql.type.SqlTypeName;

public class NullType extends PrimitiveTypeBase {
  public static final NullType SINGLETON = new NullType();

  private NullType() {
    super(true, SqlTypeName.NULL);
  }

  @Override
  public DataType getProtobufType() {
    return DataType.newBuilder()
        .setTypeName(DataType.TypeName.NULL)
        .setIsNullable(nullable)
        .build();
  }

  @Override
  protected void generateTypeString(StringBuilder sb, boolean withDetail) {
    sb.append(sqlTypeName);
  }

  @Override
  public RelDataTypeComparability getComparability() {
    return RelDataTypeComparability.NONE;
  }

  @Override
  protected PrimitiveTypeBase copyWithNullability(boolean nullable) {
    return this;
  }
}
