package com.risingwave.common.datatype;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.base.Objects;
import com.google.common.base.Verify;
import com.risingwave.proto.data.DataType;
import java.util.OptionalInt;
import org.apache.calcite.rel.type.RelDataTypeComparability;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * Type for {@link SqlTypeName#TIMESTAMP} and {@link SqlTypeName#TIMESTAMP_WITH_LOCAL_TIME_ZONE}.
 */
public class TimestampType extends PrimitiveTypeBase {
  private final int precision;

  public TimestampType(
      OptionalInt precision, SqlTypeName typeName, RisingWaveDataTypeSystem typeSystem) {
    this(false, precision.orElse(0), typeName, typeSystem);
  }

  private TimestampType(
      boolean nullable, int precision, SqlTypeName typeName, RisingWaveDataTypeSystem typeSystem) {
    super(nullable, typeName, typeSystem);
    Verify.verify(
        typeName == SqlTypeName.TIMESTAMP || typeName == SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE,
        "{} type can not be used for timestamp type",
        typeName);
    checkArgument(precision >= 0, "precision can't be negative: {}", precision);
    this.precision = precision;
    resetDigest();
  }

  @Override
  public DataType getProtobufType() {
    var typeName =
        this.sqlTypeName == SqlTypeName.TIMESTAMP
            ? DataType.TypeName.TIMESTAMP
            : DataType.TypeName.TIMESTAMPZ;
    return DataType.newBuilder()
        .setTypeName(typeName)
        .setPrecision(precision)
        .setIsNullable(nullable)
        .build();
  }

  @Override
  protected void generateTypeString(StringBuilder sb, boolean withDetail) {
    sb.append(sqlTypeName);
    if (withDetail) {
      sb.append("(").append(precision).append(")");
    }
  }

  @Override
  public RelDataTypeComparability getComparability() {
    return RelDataTypeComparability.ALL;
  }

  @Override
  public int getPrecision() {
    return precision;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    TimestampType that = (TimestampType) o;
    return precision == that.precision;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(super.hashCode(), precision);
  }

  @Override
  protected PrimitiveTypeBase copyWithNullability(boolean nullable) {
    return new TimestampType(nullable, this.precision, this.sqlTypeName, typeSystem);
  }
}
