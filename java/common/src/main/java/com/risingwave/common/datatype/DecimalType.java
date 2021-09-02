package com.risingwave.common.datatype;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.base.Objects;
import com.risingwave.proto.data.DataType;
import java.util.OptionalInt;
import org.apache.calcite.rel.type.RelDataTypeComparability;
import org.apache.calcite.sql.type.SqlTypeName;

public class DecimalType extends PrimitiveTypeBase {
  // Align with backend
  public static final int MAX_PRECISION = 28;
  // FIXME: MAX_SCALE should not be too large since it's the default scale in CREATE_TABLE
  public static final int MAX_SCALE = 10;
  private final int precision;
  private final int scale;

  private DecimalType(boolean nullable, int precision, int scale) {
    super(nullable, SqlTypeName.DECIMAL);
    checkArgument(precision > 0, "precision must be positive: {}", precision);
    checkArgument(scale >= 0, "scale must not be negative: {}", scale);
    checkArgument(precision > scale, "Scale %s is larger than precision %s", scale, precision);
    this.precision = precision;
    this.scale = scale;
    resetDigest();
  }

  public DecimalType(OptionalInt precision, OptionalInt scale) {
    // According to
    // https://www.postgresql.org/docs/current/datatype-numeric.html#DATATYPE-NUMERIC-DECIMAL,
    // if precision is present, scale should be zero, otherwise max scale.
    this(
        false,
        precision.orElse(MAX_PRECISION),
        scale.orElseGet(
            () -> {
              if (precision.isPresent()) {
                return 0;
              } else {
                return MAX_SCALE;
              }
            }));
  }

  @Override
  public DataType getProtobufType() {
    return DataType.newBuilder()
        .setTypeName(DataType.TypeName.DECIMAL)
        .setPrecision(precision)
        .setScale(scale)
        .setIsNullable(nullable)
        .build();
  }

  @Override
  protected void generateTypeString(StringBuilder sb, boolean withDetail) {
    sb.append("DECIMAL");
    if (withDetail) {
      sb.append("(").append(precision).append(",").append(scale).append(")");
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
  public int getScale() {
    return scale;
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
    DecimalType that = (DecimalType) o;
    return precision == that.precision && scale == that.scale;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(super.hashCode(), precision, scale);
  }

  @Override
  protected PrimitiveTypeBase copyWithNullability(boolean nullable) {
    return new DecimalType(nullable, this.precision, this.scale);
  }
}
