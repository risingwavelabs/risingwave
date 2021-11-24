package com.risingwave.common.datatype;

import com.risingwave.proto.data.DataType;
import org.apache.calcite.rel.type.RelDataTypeComparability;
import org.apache.calcite.sql.type.SqlTypeName;

/** Type for {@link SqlTypeName#NULL}. */
public class NullType extends PrimitiveTypeBase {
  NullType(RisingWaveDataTypeSystem typeSystem) {
    super(true, SqlTypeName.NULL, typeSystem);
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
