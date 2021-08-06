package com.risingwave.common.datatype;

import static com.google.common.base.Preconditions.checkArgument;

import com.risingwave.common.exception.PgErrorCode;
import com.risingwave.common.exception.PgException;
import java.nio.charset.Charset;
import java.util.OptionalInt;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.type.SqlTypeName;

public class RisingWaveTypeFactory extends JavaTypeFactoryImpl {
  public RisingWaveTypeFactory() {
    super(new RisingWaveDataTypeSystem());
  }

  @Override
  public RelDataType createSqlType(SqlTypeName sqlType) {
    return createSqlType(sqlType, OptionalInt.empty(), OptionalInt.empty());
  }

  @Override
  public RisingWaveDataType createSqlType(SqlTypeName sqlType, int precision) {
    return createSqlType(sqlType, OptionalInt.of(precision), OptionalInt.empty());
  }

  @Override
  public RisingWaveDataType createSqlType(SqlTypeName sqlType, int precision, int scale) {
    return createSqlType(sqlType, OptionalInt.of(precision), OptionalInt.of(scale));
  }

  private RisingWaveDataType createSqlType(
      SqlTypeName sqlType, OptionalInt precision, OptionalInt scale) {
    switch (sqlType) {
      case BOOLEAN:
        return new BooleanType();
      case SMALLINT:
        return new NumericTypeBase(SqlTypeName.SMALLINT, 2);
      case INTEGER:
        return new NumericTypeBase(SqlTypeName.INTEGER, 4);
      case BIGINT:
        return new NumericTypeBase(SqlTypeName.BIGINT, 8);
      case FLOAT:
        return new NumericTypeBase(SqlTypeName.FLOAT, 4);
      case DOUBLE:
        return new NumericTypeBase(SqlTypeName.DOUBLE, 8);
      case DECIMAL:
        return new DecimalType(precision, scale);
      case CHAR:
        return new StringType(SqlTypeName.CHAR, precision);
      case VARCHAR:
        return new StringType(SqlTypeName.VARCHAR, precision);
      case TIME:
        return new TimeType(precision);
      case TIMESTAMP:
        return new TimestampType(precision);
      case DATE:
        return new DateType();
      case NULL:
        return NullType.SINGLETON;
      default:
        if (IntervalType.isIntervalType(sqlType)) {
          return new IntervalType(sqlType);
        }
        throw new PgException(PgErrorCode.INTERNAL_ERROR, "Unrecognized sql type: %s", sqlType);
    }
  }

  @Override
  public RelDataType createTypeWithCharsetAndCollation(
      RelDataType type, Charset charset, SqlCollation collation) {
    checkArgument(type instanceof StringType, "Invalid type: %s", type.getClass());
    return ((StringType) type).withCharsetAndCollation(charset, collation);
  }
}
