package com.risingwave.common.datatype;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

import com.google.common.base.Objects;
import com.risingwave.proto.data.DataType;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.OptionalInt;
import org.apache.calcite.rel.type.RelDataTypeComparability;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.type.SqlTypeName;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Type for char and varchar.
 *
 * <p>Currently the only supported encoding is utf-8.
 */
public class StringType extends PrimitiveTypeBase {
  /** Fixed size for char, max size for varchar. */
  private final int maxSize;
  /** Fixed size char or not. */
  private final boolean fixedSize;

  private final Charset charset;
  private final SqlCollation collation;

  public StringType(SqlTypeName sqlTypeName, OptionalInt maxSize) {
    this(false, sqlTypeName, maxSize.orElse(1));
  }

  public StringType(boolean nullable, SqlTypeName sqlTypeName, int maxSize) {
    this(nullable, sqlTypeName, maxSize, StandardCharsets.UTF_8, SqlCollation.COERCIBLE);
  }

  public StringType(
      boolean nullable,
      SqlTypeName sqlTypeName,
      int maxSize,
      Charset charset,
      SqlCollation collation) {
    super(nullable, sqlTypeName);
    checkArgument(maxSize >= 0, "Max size is negative: %s", maxSize);
    checkArgument(
        checkSqlTypeName(sqlTypeName), "Invalid sql type name for string type: {}", sqlTypeName);
    this.maxSize = maxSize;
    this.fixedSize = sqlTypeName == SqlTypeName.CHAR;
    this.charset = requireNonNull(charset, "charset");
    this.collation = requireNonNull(collation, "collation");
    resetDigest();
  }

  private static boolean checkSqlTypeName(SqlTypeName sqlTypeName) {
    return SqlTypeName.CHAR == sqlTypeName || SqlTypeName.VARCHAR == sqlTypeName;
  }

  public int getMaxSize() {
    return maxSize;
  }

  public boolean isFixedSize() {
    return fixedSize;
  }

  public StringType withCharsetAndCollation(Charset charset, SqlCollation collation) {
    return new StringType(nullable, sqlTypeName, maxSize, charset, collation);
  }

  @Override
  public @Nullable Charset getCharset() {
    return charset;
  }

  @Override
  public @Nullable SqlCollation getCollation() {
    return collation;
  }

  @Override
  public DataType getProtobufType() {
    DataType.TypeName typeName =
        sqlTypeName == SqlTypeName.CHAR ? DataType.TypeName.CHAR : DataType.TypeName.VARCHAR;

    return DataType.newBuilder()
        .setTypeName(typeName)
        .setPrecision(maxSize)
        .setIsNullable(nullable)
        .build();
  }

  @Override
  protected void generateTypeString(StringBuilder sb, boolean withDetail) {
    sb.append(getSqlTypeName());
    if (withDetail) {
      sb.append("(").append(maxSize).append(")");
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
    StringType that = (StringType) o;
    return maxSize == that.maxSize && fixedSize == that.fixedSize;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(super.hashCode(), maxSize, fixedSize);
  }

  @Override
  protected PrimitiveTypeBase copyWithNullability(boolean nullable) {
    return new StringType(nullable, getSqlTypeName(), this.maxSize, this.charset, this.collation);
  }
}
