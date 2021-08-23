package com.risingwave.common.datatype;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.collect.ImmutableList;
import com.risingwave.common.exception.PgErrorCode;
import com.risingwave.common.exception.PgException;
import java.nio.charset.Charset;
import java.util.AbstractList;
import java.util.List;
import java.util.OptionalInt;
import java.util.stream.IntStream;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.StructKind;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.type.SqlTypeName;

public class RisingWaveTypeFactory extends JavaTypeFactoryImpl {
  public RisingWaveTypeFactory() {
    super(new RisingWaveDataTypeSystem());
  }

  @Override
  public RelDataType createTypeWithNullability(RelDataType type, boolean nullable) {
    checkArgument(type instanceof RisingWaveDataType, "Type is: %s", type.getClass());
    return ((RisingWaveDataType) type).withNullability(nullable);
  }

  @Override
  public RelDataType createStructType(Class type) {
    throw new UnsupportedOperationException("Create struct type from class!");
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
        return new TimestampType(precision);
      case DATE:
        return new DateType();
      case NULL:
        return NullType.SINGLETON;
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
}
