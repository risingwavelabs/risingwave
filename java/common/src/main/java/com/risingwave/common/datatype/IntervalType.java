package com.risingwave.common.datatype;

import static com.google.common.base.Preconditions.checkArgument;
import static com.risingwave.proto.data.DataType.IntervalType.DAY;
import static com.risingwave.proto.data.DataType.IntervalType.DAY_TO_HOUR;
import static com.risingwave.proto.data.DataType.IntervalType.DAY_TO_MINUTE;
import static com.risingwave.proto.data.DataType.IntervalType.DAY_TO_SECOND;
import static com.risingwave.proto.data.DataType.IntervalType.HOUR;
import static com.risingwave.proto.data.DataType.IntervalType.HOUR_TO_MINUTE;
import static com.risingwave.proto.data.DataType.IntervalType.HOUR_TO_SECOND;
import static com.risingwave.proto.data.DataType.IntervalType.MINUTE;
import static com.risingwave.proto.data.DataType.IntervalType.MINUTE_TO_SECOND;
import static com.risingwave.proto.data.DataType.IntervalType.MONTH;
import static com.risingwave.proto.data.DataType.IntervalType.YEAR;
import static com.risingwave.proto.data.DataType.IntervalType.YEAR_TO_MONTH;
import static org.apache.calcite.sql.type.SqlTypeName.INTERVAL_DAY;
import static org.apache.calcite.sql.type.SqlTypeName.INTERVAL_DAY_HOUR;
import static org.apache.calcite.sql.type.SqlTypeName.INTERVAL_DAY_MINUTE;
import static org.apache.calcite.sql.type.SqlTypeName.INTERVAL_DAY_SECOND;
import static org.apache.calcite.sql.type.SqlTypeName.INTERVAL_HOUR;
import static org.apache.calcite.sql.type.SqlTypeName.INTERVAL_HOUR_MINUTE;
import static org.apache.calcite.sql.type.SqlTypeName.INTERVAL_HOUR_SECOND;
import static org.apache.calcite.sql.type.SqlTypeName.INTERVAL_MINUTE;
import static org.apache.calcite.sql.type.SqlTypeName.INTERVAL_MINUTE_SECOND;
import static org.apache.calcite.sql.type.SqlTypeName.INTERVAL_MONTH;
import static org.apache.calcite.sql.type.SqlTypeName.INTERVAL_YEAR;
import static org.apache.calcite.sql.type.SqlTypeName.INTERVAL_YEAR_MONTH;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableMap;
import com.risingwave.proto.data.DataType;
import org.apache.calcite.rel.type.RelDataTypeComparability;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlWriterConfig;
import org.apache.calcite.sql.dialect.PostgresqlSqlDialect;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.util.SqlString;

public class IntervalType extends PrimitiveTypeBase {
  private static final ImmutableMap<SqlTypeName, DataType.IntervalType> INTERVAL_TYPE_MAPPING =
      ImmutableMap.<SqlTypeName, DataType.IntervalType>builder()
          .put(INTERVAL_YEAR, YEAR)
          .put(INTERVAL_MONTH, MONTH)
          .put(INTERVAL_DAY, DAY)
          .put(INTERVAL_HOUR, HOUR)
          .put(INTERVAL_MINUTE, MINUTE)
          .put(INTERVAL_YEAR_MONTH, YEAR_TO_MONTH)
          .put(INTERVAL_DAY_HOUR, DAY_TO_HOUR)
          .put(INTERVAL_DAY_MINUTE, DAY_TO_MINUTE)
          .put(INTERVAL_DAY_SECOND, DAY_TO_SECOND)
          .put(INTERVAL_HOUR_MINUTE, HOUR_TO_MINUTE)
          .put(INTERVAL_HOUR_SECOND, HOUR_TO_SECOND)
          .put(INTERVAL_MINUTE_SECOND, MINUTE_TO_SECOND)
          .build();

  private final SqlIntervalQualifier intervalQualifier;

  public IntervalType(SqlIntervalQualifier qualifier) {
    this(false, qualifier);
  }

  public IntervalType(boolean nullable, SqlIntervalQualifier qualifier) {
    super(nullable, qualifier.typeName());
    checkArgument(
        INTERVAL_TYPE_MAPPING.containsKey(sqlTypeName),
        "Invalid interval sql type: " + "{}",
        sqlTypeName);
    this.intervalQualifier = qualifier;
    resetDigest();
  }

  @Override
  public DataType getProtobufType() {
    return DataType.newBuilder()
        .setTypeName(DataType.TypeName.INTERVAL)
        .setIntervalType(INTERVAL_TYPE_MAPPING.get(sqlTypeName))
        .setIsNullable(nullable)
        .build();
  }

  @Override
  protected void generateTypeString(StringBuilder sb, boolean withDetail) {
    sb.append("INTERVAL ");
    final SqlDialect dialect = PostgresqlSqlDialect.DEFAULT;
    final SqlWriterConfig config =
        SqlPrettyWriter.config()
            .withAlwaysUseParentheses(false)
            .withSelectListItemsOnSeparateLines(false)
            .withIndentation(0)
            .withDialect(dialect);
    final SqlPrettyWriter writer = new SqlPrettyWriter(config);

    intervalQualifier.unparse(writer, 0, 0);
    final String sql = writer.toString();
    sb.append(new SqlString(dialect, sql).getSql());
  }

  @Override
  public RelDataTypeComparability getComparability() {
    return RelDataTypeComparability.ALL;
  }

  public static boolean isIntervalType(SqlTypeName sqlTypeName) {
    return INTERVAL_TYPE_MAPPING.containsKey(sqlTypeName);
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
    IntervalType that = (IntervalType) o;
    return Objects.equal(intervalQualifier, that.intervalQualifier);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(super.hashCode(), intervalQualifier);
  }

  @Override
  protected PrimitiveTypeBase copyWithNullability(boolean nullable) {
    return new IntervalType(nullable, intervalQualifier);
  }
}
