package com.risingwave.common.datatype;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.collect.ImmutableList;
import com.risingwave.common.exception.PgErrorCode;
import com.risingwave.common.exception.PgException;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.util.AbstractList;
import java.util.List;
import java.util.OptionalInt;
import java.util.stream.IntStream;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rel.type.StructKind;
import org.apache.calcite.runtime.Geometries;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.type.SqlTypeName;

public class RisingWaveTypeFactory extends JavaTypeFactoryImpl {
  public RisingWaveTypeFactory() {
    super(new RisingWaveDataTypeSystem());
  }

  @Override
  public RelDataType createTypeWithNullability(RelDataType type, boolean nullable) {
    // Why we may have RelRecordType? Because RelDataTypeFactoryImpl(super class),
    // marks following method:
    // createStructType(final List<? extends Map.Entry<String, RelDataType>> fieldList)
    // as final and always returns RelRecordType.
    // We leave it to be fixed later.
    checkArgument(
        (type instanceof RisingWaveDataType) || (type instanceof RelRecordType),
        "Type is: %s",
        type.getClass(),
        type);
    if (type instanceof RisingWaveDataType) {
      return ((RisingWaveDataType) type).withNullability(nullable);
    } else {
      return super.createTypeWithNullability(type, nullable);
    }
  }

  @Override
  public RelDataType createStructType(List<RelDataType> typeList, List<String> fieldNameList) {
    return createStructType(StructKind.FULLY_QUALIFIED, fieldNameList, typeList, false);
  }

  @Override
  public RelDataType createStructType(
      StructKind kind, List<RelDataType> typeList, List<String> fieldNameList) {
    return createStructType(kind, fieldNameList, typeList, false);
  }

  @SuppressWarnings("deprecation")
  @Override
  public RelDataType createStructType(FieldInfo fieldInfo) {
    return createStructType(
        StructKind.FULLY_QUALIFIED,
        new AbstractList<>() {
          @Override
          public String get(int index) {
            return fieldInfo.getFieldName(index);
          }

          @Override
          public int size() {
            return fieldInfo.getFieldCount();
          }
        },
        new AbstractList<>() {
          @Override
          public RelDataType get(int index) {
            return fieldInfo.getFieldType(index);
          }

          @Override
          public int size() {
            return fieldInfo.getFieldCount();
          }
        },
        false);
  }

  private StructType createStructType(
      StructKind kind, List<String> names, List<RelDataType> types, boolean nullable) {
    List<RelDataTypeField> fields =
        IntStream.range(0, types.size())
            .mapToObj(idx -> new RelDataTypeFieldImpl(names.get(idx), idx, types.get(idx)))
            .collect(ImmutableList.toImmutableList());
    return new StructType(kind, nullable, fields);
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
        return new TimestampType(precision, SqlTypeName.TIMESTAMP);
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        return new TimestampType(precision, SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE);
      case DATE:
        return new DateType();
      case NULL:
        return NullType.SINGLETON;
      case SYMBOL:
        return new SymbolType();
      default:
        throw new PgException(PgErrorCode.INTERNAL_ERROR, "Unrecognized sql type: %s", sqlType);
    }
  }

  @Override
  public RelDataType createTypeWithCharsetAndCollation(
      RelDataType type, Charset charset, SqlCollation collation) {
    checkArgument(type instanceof StringType, "Invalid type: %s", type.getClass());
    return ((StringType) type).withCharsetAndCollation(charset, collation);
  }

  @Override
  public RelDataType createSqlIntervalType(SqlIntervalQualifier intervalQualifier) {
    return new IntervalType(intervalQualifier);
  }

  @Override
  public Type getJavaClass(RelDataType type) {
    switch (type.getSqlTypeName()) {
      case VARCHAR:
      case CHAR:
        return String.class;
      case DATE:
      case TIME:
      case TIME_WITH_LOCAL_TIME_ZONE:
      case INTEGER:
      case INTERVAL_YEAR:
      case INTERVAL_YEAR_MONTH:
      case INTERVAL_MONTH:
        return type.isNullable() ? Integer.class : int.class;
      case TIMESTAMP:
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
      case BIGINT:
      case INTERVAL_DAY:
      case INTERVAL_DAY_HOUR:
      case INTERVAL_DAY_MINUTE:
      case INTERVAL_DAY_SECOND:
      case INTERVAL_HOUR:
      case INTERVAL_HOUR_MINUTE:
      case INTERVAL_HOUR_SECOND:
      case INTERVAL_MINUTE:
      case INTERVAL_MINUTE_SECOND:
      case INTERVAL_SECOND:
        return type.isNullable() ? Long.class : long.class;
      case SMALLINT:
        return type.isNullable() ? Short.class : short.class;
      case TINYINT:
        return type.isNullable() ? Byte.class : byte.class;
      case DECIMAL:
        return BigDecimal.class;
      case BOOLEAN:
        return type.isNullable() ? Boolean.class : boolean.class;
      case DOUBLE:
      case FLOAT: // sic
        return type.isNullable() ? Double.class : double.class;
      case REAL:
        return type.isNullable() ? Float.class : float.class;
      case BINARY:
      case VARBINARY:
        return ByteString.class;
      case GEOMETRY:
        return Geometries.Geom.class;
      case SYMBOL:
        return Enum.class;
      case ANY:
        return Object.class;
      case NULL:
        return Void.class;
      default:
        break;
    }

    return super.getJavaClass(type);
  }

  @Override
  public RelDataType createArrayType(RelDataType elementType, long maxCardinality) {
    throw new UnsupportedOperationException("createArrayType");
  }

  @Override
  public RelDataType createMapType(RelDataType keyType, RelDataType valueType) {
    throw new UnsupportedOperationException("createMapType");
  }

  @Override
  public RelDataType createMultisetType(RelDataType elementType, long maxCardinality) {
    throw new UnsupportedOperationException("createMultisetType");
  }

  @Override
  public RelDataType createUnknownType() {
    return UnknownType.INSTANCE;
  }

  @Override
  public RelDataType createJoinType(RelDataType... types) {
    return super.createJoinType(types);
  }
}
