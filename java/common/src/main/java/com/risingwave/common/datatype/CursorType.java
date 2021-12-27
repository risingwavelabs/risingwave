package com.risingwave.common.datatype;

import com.risingwave.proto.data.DataType;
import org.apache.calcite.rel.type.RelDataTypeComparability;
import org.apache.calcite.sql.type.SqlTypeName;

/** Type for {@link SqlTypeName#CURSOR}. */
public class CursorType extends PrimitiveTypeBase {

  protected CursorType(RisingWaveDataTypeSystem typeSystem) {
    super(false, SqlTypeName.CURSOR, typeSystem);
  }

  @Override
  PrimitiveTypeBase copyWithNullability(boolean nullable) {
    return new CursorType(typeSystem);
  }

  @Override
  public DataType getProtobufType() {
    return DataType.newBuilder().setTypeName(DataType.TypeName.CURSOR).setIsNullable(false).build();
  }

  @Override
  protected void generateTypeString(StringBuilder sb, boolean withDetail) {
    sb.append(getSqlTypeName());
  }

  @Override
  public RelDataTypeComparability getComparability() {
    return RelDataTypeComparability.NONE;
  }
}
