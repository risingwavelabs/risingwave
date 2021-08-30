package com.risingwave.common.datatype;

import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.risingwave.common.exception.PgErrorCode;
import com.risingwave.common.exception.PgException;
import com.risingwave.proto.data.DataType;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeComparability;
import org.apache.calcite.rel.type.RelDataTypeFamily;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelDataTypePrecedenceList;
import org.apache.calcite.rel.type.StructKind;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.type.SqlTypeExplicitPrecedenceList;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Util;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Risingwave's version of {@link org.apache.calcite.rel.type.RelRecordType}. */
public class StructType extends RisingWaveTypeBase {
  private final StructKind kind;
  private final boolean nullable;
  private final ImmutableList<RelDataTypeField> fields;

  public StructType(StructKind kind, boolean nullable, List<RelDataTypeField> fields) {
    this.kind = requireNonNull(kind, "kind");
    this.nullable = nullable;
    this.fields = ImmutableList.copyOf(fields);
    resetDigest();
  }

  @Override
  public DataType getProtobufType() {
    throw new PgException(PgErrorCode.INTERNAL_ERROR, "Converting struct type to protobuf");
  }

  @Override
  public StructType withNullability(boolean nullable) {
    if (this.nullable == nullable) {
      return this;
    }
    return new StructType(this.kind, nullable, this.fields);
  }

  @Override
  protected void generateTypeString(StringBuilder sb, boolean withDetail) {
    sb.append("RecordType");
    switch (kind) {
      case PEEK_FIELDS:
        sb.append(":peek");
        break;
      case PEEK_FIELDS_DEFAULT:
        sb.append(":peek_default");
        break;
      case PEEK_FIELDS_NO_EXPAND:
        sb.append(":peek_no_expand");
        break;
      default:
        break;
    }
    sb.append("(");
    for (Ord<RelDataTypeField> ord : Ord.zip(requireNonNull(fields, "fieldList"))) {
      if (ord.i > 0) {
        sb.append(", ");
      }
      RelDataTypeField field = ord.e;
      if (withDetail) {
        sb.append(field.getType().getFullTypeString());
      } else {
        sb.append(field.getType().toString());
      }
      sb.append(" ");
      sb.append(field.getName());
    }
    sb.append(")");
  }

  @Override
  public boolean isStruct() {
    return true;
  }

  @Override
  public List<RelDataTypeField> getFieldList() {
    return fields;
  }

  @Override
  public List<String> getFieldNames() {
    return fields.stream().map(RelDataTypeField::getName).collect(Collectors.toList());
  }

  @Override
  public int getFieldCount() {
    return fields.size();
  }

  @Override
  public StructKind getStructKind() {
    return this.kind;
  }

  @Override
  public @Nullable RelDataTypeField getField(
      String fieldName, boolean caseSensitive, boolean elideRecord) {
    if (fields == null) {
      throw new IllegalStateException(
          "Trying to access field " + fieldName + " in a type with no fields: " + this);
    }
    for (RelDataTypeField field : fields) {
      if (Util.matches(caseSensitive, field.getName(), fieldName)) {
        return field;
      }
    }
    if (elideRecord) {
      final List<Slot> slots = new ArrayList<>();
      getFieldRecurse(slots, this, 0, fieldName, caseSensitive);
      loop:
      for (Slot slot : slots) {
        switch (slot.count) {
          case 0:
            break; // no match at this depth; try deeper
          case 1:
            return slot.field;
          default:
            break loop; // duplicate fields at this depth; abandon search
        }
      }
    }
    // Extra field
    if (fields.size() > 0) {
      final RelDataTypeField lastField = Iterables.getLast(fields);
      if (lastField.getName().equals("_extra")) {
        return new RelDataTypeFieldImpl(fieldName, -1, lastField.getType());
      }
    }

    // a dynamic * field will match any field name.
    for (RelDataTypeField field : fields) {
      if (field.isDynamicStar()) {
        // the requested field could be in the unresolved star
        return field;
      }
    }

    return null;
  }

  private static void getFieldRecurse(
      List<Slot> slots, RelDataType type, int depth, String fieldName, boolean caseSensitive) {
    while (slots.size() <= depth) {
      slots.add(new Slot());
    }
    final Slot slot = slots.get(depth);
    for (RelDataTypeField field : type.getFieldList()) {
      if (Util.matches(caseSensitive, field.getName(), fieldName)) {
        slot.count++;
        slot.field = field;
      }
    }
    // No point looking to depth + 1 if there is a hit at depth.
    if (slot.count == 0) {
      for (RelDataTypeField field : type.getFieldList()) {
        if (field.getType().isStruct()) {
          getFieldRecurse(slots, field.getType(), depth + 1, fieldName, caseSensitive);
        }
      }
    }
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
    return PRECISION_NOT_SPECIFIED;
  }

  @Override
  public int getScale() {
    return SCALE_NOT_SPECIFIED;
  }

  @Override
  public SqlTypeName getSqlTypeName() {
    return SqlTypeName.ROW;
  }

  @Override
  public @Nullable SqlIdentifier getSqlIdentifier() {
    return null;
  }

  @Override
  public RelDataTypeFamily getFamily() {
    return getSqlTypeName().getFamily();
  }

  @Override
  public RelDataTypePrecedenceList getPrecedenceList() {
    return new SqlTypeExplicitPrecedenceList(Collections.singletonList(getSqlTypeName()));
  }

  @Override
  public RelDataTypeComparability getComparability() {
    return RelDataTypeComparability.ALL;
  }

  @Override
  public boolean isDynamicStruct() {
    return false;
  }

  @Override
  public String toString() {
    return digest;
  }

  private static class Slot {
    int count;
    @Nullable RelDataTypeField field;
  }
}
