package com.risingwave.common.datatype;

import com.risingwave.proto.data.DataType;
import org.apache.calcite.rel.type.RelDataTypeComparability;
import org.apache.calcite.sql.type.SqlTypeName;

/** Required by calcite. */
public class UnknownType extends PrimitiveTypeBase {
  UnknownType(RisingWaveDataTypeSystem typeSystem) {
    super(true, SqlTypeName.NULL, typeSystem);
  }

  @Override
  protected PrimitiveTypeBase copyWithNullability(boolean nullable) {
    return this;
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
    sb.append("UNKNOWN");
  }

  @Override
  public RelDataTypeComparability getComparability() {
    return RelDataTypeComparability.NONE;
  }
}
