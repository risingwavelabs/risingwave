package com.risingwave.common.datatype;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.base.Objects;
import com.risingwave.proto.data.DataType;
import java.util.OptionalInt;
import org.apache.calcite.rel.type.RelDataTypeComparability;
import org.apache.calcite.sql.type.SqlTypeName;

public class TimestampType extends PrimitiveTypeBase {
  private final int precision;

  public TimestampType(OptionalInt precision) {
    this(false, precision.orElse(0));
  }

  public TimestampType(boolean nullable, int precision) {
    super(nullable, SqlTypeName.TIMESTAMP);
    checkArgument(precision >= 0, "precision can't be negative: {}", precision);
    this.precision = precision;
  }

  @Override
  public DataType getProtobufType() {
    return DataType.newBuilder()
        .setTypeName(DataType.TypeName.TIMESTAMP)
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
}
