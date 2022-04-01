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
    throw new UnsupportedOperationException();
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
