package com.risingwave.common.datatype;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.base.Objects;
import com.risingwave.proto.data.DataType;
import java.util.OptionalInt;
import org.apache.calcite.rel.type.RelDataTypeComparability;
import org.apache.calcite.sql.type.SqlTypeName;

/** Type for {@link SqlTypeName#TIME}. */
public class TimeType extends PrimitiveTypeBase {
  // TODO: Add support for timezone now
  private final int precision;

  public TimeType(boolean nullable, int precision, RisingWaveDataTypeSystem typeSystem) {
    super(nullable, SqlTypeName.TIME, typeSystem);
    checkArgument(precision >= 0, "precision must not be negative: {}", precision);
    this.precision = precision;
    resetDigest();
  }

  public TimeType(OptionalInt precision, RisingWaveDataTypeSystem typeSystem) {
    this(false, precision.orElse(0), typeSystem);
  }

  @Override
  public DataType getProtobufType() {
    return DataType.newBuilder()
        .setTypeName(DataType.TypeName.TIME)
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
    TimeType timeType = (TimeType) o;
    return precision == timeType.precision;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(super.hashCode(), precision);
  }

  @Override
  protected PrimitiveTypeBase copyWithNullability(boolean nullable) {
    return new TimeType(nullable, this.precision, typeSystem);
  }
}
