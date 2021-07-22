package com.risingwave.common.datatype;

import java.nio.charset.Charset;
import java.util.Collections;
import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeComparability;
import org.apache.calcite.rel.type.RelDataTypeFamily;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypePrecedenceList;
import org.apache.calcite.rel.type.StructKind;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.type.SqlTypeExplicitPrecedenceList;
import org.apache.calcite.sql.type.SqlTypeName;
import org.checkerframework.checker.nullness.qual.Nullable;

public abstract class PrimitiveTypeBase implements RisingWaveDataType {
  private final boolean nullable;
  private final SqlTypeName sqlTypeName;

  protected PrimitiveTypeBase(boolean nullable, SqlTypeName sqlTypeName) {
    this.nullable = nullable;
    this.sqlTypeName = sqlTypeName;
  }

  @Override
  public boolean isStruct() {
    return false;
  }

  @Override
  public List<RelDataTypeField> getFieldList() {
    return Collections.emptyList();
  }

  @Override
  public List<String> getFieldNames() {
    return Collections.emptyList();
  }

  @Override
  public int getFieldCount() {
    return 0;
  }

  @Override
  public StructKind getStructKind() {
    return StructKind.NONE;
  }

  @Override
  public @Nullable RelDataTypeField getField(
      String fieldName, boolean caseSensitive, boolean elideRecord) {
    return null;
  }

  @Override
  public boolean isNullable() {
    return nullable;
  }

  @Override
  public @Nullable RelDataType getComponentType() {
    return null;
  }

  @Override
  public @Nullable RelDataType getKeyType() {
    return null;
  }

  @Override
  public @Nullable RelDataType getValueType() {
    return null;
  }

  @Override
  public boolean equalsSansFieldNames(@Nullable RelDataType that) {
    return equals(that);
  }

  @Override
  public @Nullable Charset getCharset() {
    return null;
  }

  @Override
  public @Nullable SqlCollation getCollation() {
    return null;
  }

  @Override
  public @Nullable SqlIntervalQualifier getIntervalQualifier() {
    return null;
  }

  @Override
  public int getPrecision() {
    return 0;
  }

  @Override
  public int getScale() {
    return 0;
  }

  @Override
  public SqlTypeName getSqlTypeName() {
    return sqlTypeName;
  }

  @Override
  public @Nullable SqlIdentifier getSqlIdentifier() {
    return null;
  }

  @Override
  public RelDataTypeFamily getFamily() {
    return sqlTypeName.getFamily();
  }

  @Override
  public RelDataTypePrecedenceList getPrecedenceList() {
    return new SqlTypeExplicitPrecedenceList(Collections.singletonList(getSqlTypeName()));
  }

  @Override
  public RelDataTypeComparability getComparability() {
    return RelDataTypeComparability.NONE;
  }

  @Override
  public boolean isDynamicStruct() {
    return false;
  }
}
