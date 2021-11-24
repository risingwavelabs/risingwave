package com.risingwave.common.datatype;

import com.risingwave.proto.data.DataType;
import org.apache.calcite.rel.type.RelDataTypeComparability;
import org.apache.calcite.sql.type.SqlTypeName;

/** Type for {@link SqlTypeName#DATE}. */
public class DateType extends PrimitiveTypeBase {
  public DateType(RisingWaveDataTypeSystem typeSystem) {
    this(false, typeSystem);
  }

  public DateType(boolean nullable, RisingWaveDataTypeSystem typeSystem) {
    super(nullable, SqlTypeName.DATE, typeSystem);
    resetDigest();
  }

  @Override
  public DataType getProtobufType() {
    return DataType.newBuilder()
        .setTypeName(DataType.TypeName.DATE)
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
    return new DateType(nullable, typeSystem);
  }
}
