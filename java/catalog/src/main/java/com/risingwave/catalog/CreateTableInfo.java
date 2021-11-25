package com.risingwave.catalog;

import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.util.Pair;

/** CreateTableInfo is a class that contains information about the table to be */
public class CreateTableInfo {
  private final String name;
  private final ImmutableList<Pair<String, ColumnDesc>> columns;
  private final ImmutableMap<String, String> properties;
  private final boolean appendOnly;
  private final boolean mv;
  private final boolean stream;
  private final String rowFormat;
  private final String rowSchemaLocation;
  private final RelCollation collation;
  private final Integer offset;
  private final Integer limit;

  private CreateTableInfo(
      String tableName,
      ImmutableList<Pair<String, ColumnDesc>> columns,
      ImmutableMap<String, String> properties,
      boolean appendOnly,
      boolean mv,
      boolean stream,
      String rowFormat,
      String rowSchemaLocation,
      @Nullable RelCollation collation,
      @Nullable Integer offset,
      @Nullable Integer limit) {
    this.name = tableName;
    this.columns = columns;
    this.properties = properties;
    this.appendOnly = appendOnly;
    this.mv = mv;
    this.stream = stream;
    this.rowFormat = rowFormat;
    this.rowSchemaLocation = rowSchemaLocation;
    this.collation = collation;
    this.offset = offset;
    this.limit = limit;
  }

  public String getName() {
    return name;
  }

  public ImmutableList<Pair<String, ColumnDesc>> getColumns() {
    return columns;
  }

  public boolean isAppendOnly() {
    return appendOnly;
  }

  public boolean isMv() {
    return mv;
  }

  public static Builder builder(String tableName) {
    return new Builder(tableName);
  }

  public boolean isStream() {
    return stream;
  }

  public ImmutableMap<String, String> getProperties() {
    return properties;
  }

  public String getRowFormat() {
    return rowFormat;
  }

  public String getRowSchemaLocation() {
    return rowSchemaLocation;
  }

  public RelCollation getCollation() {
    return collation;
  }

  public Integer getOffset() {
    return offset;
  }

  public Integer getLimit() {
    return limit;
  }

  /** Builder class for CreateTableInfo. */
  public static class Builder {
    private final String tableName;
    private final List<Pair<String, ColumnDesc>> columns = new ArrayList<>();
    private Map<String, String> properties = new HashMap<>();
    private boolean appendOnly = false;
    private boolean mv = false;
    private boolean stream = false;
    private String rowFormat = "";
    private String rowSchemaLocation = "";
    private RelCollation collation = null;
    private Integer offset = null;
    private Integer limit = null;

    private Builder(String tableName) {
      this.tableName = requireNonNull(tableName, "table name can't be null!");
    }

    public Builder addColumn(String name, ColumnDesc columnDesc) {
      columns.add(Pair.of(name, columnDesc));
      return this;
    }

    public void setCollation(RelCollation collation) {
      this.collation = collation;
    }

    public void setOffset(Integer offset) {
      this.offset = offset;
    }

    public void setLimit(Integer limit) {
      this.limit = limit;
    }

    public Builder setAppendOnly(boolean appendOnly) {
      this.appendOnly = appendOnly;
      return this;
    }

    public Builder setMv(boolean mv) {
      this.mv = mv;
      return this;
    }

    public Builder setRowSchemaLocation(String rowSchemaLocation) {
      this.rowSchemaLocation = rowSchemaLocation;
      return this;
    }

    public void setStream(boolean stream) {
      this.stream = stream;
    }

    public void setProperties(Map<String, String> properties) {
      this.properties = properties;
    }

    public void setRowFormat(String rowFormat) {
      this.rowFormat = rowFormat;
    }

    public CreateTableInfo build() {
      return new CreateTableInfo(
          tableName,
          ImmutableList.copyOf(columns),
          ImmutableMap.copyOf(properties),
          appendOnly,
          mv,
          stream,
          rowFormat,
          rowSchemaLocation,
          collation,
          offset,
          limit);
    }
  }
}
