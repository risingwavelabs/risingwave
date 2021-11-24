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

/** Overrides Calcite to construct RisingWaveDataType. */
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
      return canonize(((RisingWaveDataType) type).withNullability(nullable));
    } else {
      return super.createTypeWithNullability(type, nullable);
    }
  }

  protected RisingWaveDataType canonize(RisingWaveDataType type) {
    return (RisingWaveDataType) super.canonize(type);
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
        return canonize(new BooleanType((RisingWaveDataTypeSystem) typeSystem));
      case SMALLINT:
        return canonize(
            new NumericTypeBase(SqlTypeName.SMALLINT, 2, (RisingWaveDataTypeSystem) typeSystem));
      case INTEGER:
        return canonize(
            new NumericTypeBase(SqlTypeName.INTEGER, 4, (RisingWaveDataTypeSystem) typeSystem));
      case BIGINT:
        return canonize(
            new NumericTypeBase(SqlTypeName.BIGINT, 8, (RisingWaveDataTypeSystem) typeSystem));
      case FLOAT:
        return canonize(
            new NumericTypeBase(SqlTypeName.FLOAT, 4, (RisingWaveDataTypeSystem) typeSystem));
      case DOUBLE:
        return canonize(
            new NumericTypeBase(SqlTypeName.DOUBLE, 8, (RisingWaveDataTypeSystem) typeSystem));
      case DECIMAL:
        return canonize(new DecimalType(precision, scale, (RisingWaveDataTypeSystem) typeSystem));
      case CHAR:
        return canonize(
            new StringType(SqlTypeName.CHAR, precision, (RisingWaveDataTypeSystem) typeSystem));
      case VARCHAR:
        return canonize(
            new StringType(SqlTypeName.VARCHAR, precision, (RisingWaveDataTypeSystem) typeSystem));
      case TIME:
        return canonize(new TimeType(precision, (RisingWaveDataTypeSystem) typeSystem));
      case TIMESTAMP:
        return canonize(
            new TimestampType(
                precision, SqlTypeName.TIMESTAMP, (RisingWaveDataTypeSystem) typeSystem));
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        return canonize(
            new TimestampType(
                precision,
                SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE,
                (RisingWaveDataTypeSystem) typeSystem));
      case DATE:
        return canonize(new DateType((RisingWaveDataTypeSystem) typeSystem));
      case NULL:
        return canonize(new NullType((RisingWaveDataTypeSystem) typeSystem));
      case SYMBOL:
        return canonize(new SymbolType((RisingWaveDataTypeSystem) typeSystem));
      default:
        throw new PgException(PgErrorCode.INTERNAL_ERROR, "Unrecognized sql type: %s", sqlType);
    }
  }

  public RisingWaveDataType createDataType(com.risingwave.proto.data.DataType dataType) {
    switch (dataType.getTypeName()) {
      case INT16:
        return new NumericTypeBase(SqlTypeName.SMALLINT, 2, (RisingWaveDataTypeSystem) typeSystem);
      case INT32:
        return new NumericTypeBase(SqlTypeName.INTEGER, 4, (RisingWaveDataTypeSystem) typeSystem);
      case INT64:
        return new NumericTypeBase(SqlTypeName.BIGINT, 8, (RisingWaveDataTypeSystem) typeSystem);
      case FLOAT:
        return new NumericTypeBase(SqlTypeName.FLOAT, 4, (RisingWaveDataTypeSystem) typeSystem);
      case DOUBLE:
        return new NumericTypeBase(SqlTypeName.DOUBLE, 8, (RisingWaveDataTypeSystem) typeSystem);
      case BOOLEAN:
        return new BooleanType((RisingWaveDataTypeSystem) typeSystem);
      case DATE:
        return new DateType((RisingWaveDataTypeSystem) typeSystem);
      case TIME:
        return new TimeType(
            OptionalInt.of(dataType.getPrecision()), (RisingWaveDataTypeSystem) typeSystem);
      case TIMESTAMP:
        return new TimestampType(
            OptionalInt.of(dataType.getPrecision()),
            SqlTypeName.TIMESTAMP,
            (RisingWaveDataTypeSystem) typeSystem);
      case TIMESTAMPZ:
        return new TimestampType(
            OptionalInt.of(dataType.getPrecision()),
            SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE,
            (RisingWaveDataTypeSystem) typeSystem);
      case CHAR:
        return new StringType(
            SqlTypeName.CHAR,
            OptionalInt.of(dataType.getPrecision()),
            (RisingWaveDataTypeSystem) typeSystem);
      case VARCHAR:
        return new StringType(
            SqlTypeName.VARCHAR,
            OptionalInt.of(dataType.getPrecision()),
            (RisingWaveDataTypeSystem) typeSystem);
      case DECIMAL:
        return new DecimalType(
            OptionalInt.of(dataType.getPrecision()),
            OptionalInt.of(dataType.getScale()),
            (RisingWaveDataTypeSystem) typeSystem);
      default:
        throw new PgException(PgErrorCode.INTERNAL_ERROR, "Unrecognized data type: %s", dataType);
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
    return new IntervalType(intervalQualifier, (RisingWaveDataTypeSystem) typeSystem);
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
    return canonize(new UnknownType((RisingWaveDataTypeSystem) typeSystem));
  }

  @Override
  public RelDataType createJoinType(RelDataType... types) {
    return super.createJoinType(types);
  }
}
