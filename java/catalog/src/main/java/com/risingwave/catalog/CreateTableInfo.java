package com.risingwave.catalog;

import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.risingwave.proto.plan.TableRefId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Pair;

/** CreateTableInfo is a class that contains information about the table to be */
public class CreateTableInfo {
  private final String name;
  private final ImmutableList<Pair<String, ColumnDesc>> columns;
  private final ImmutableIntList primaryKeyIndices;
  private final ImmutableMap<String, String> properties;
  private final boolean appendOnly;
  private final boolean source;
  private final String rowFormat;
  private final String rowSchemaLocation;
  private final ImmutableList<TableRefId> dependentTables;

  protected CreateTableInfo(
      String tableName,
      ImmutableList<Pair<String, ColumnDesc>> columns,
      ImmutableIntList primaryKeyIndices,
      ImmutableMap<String, String> properties,
      boolean appendOnly,
      boolean source,
      String rowFormat,
      String rowSchemaLocation,
      ImmutableList<TableRefId> dependentTables) {
    this.name = tableName;
    this.columns = columns;
    this.primaryKeyIndices = primaryKeyIndices;
    this.properties = properties;
    this.appendOnly = appendOnly;
    this.source = source;
    this.rowFormat = rowFormat;
    this.rowSchemaLocation = rowSchemaLocation;
    this.dependentTables = dependentTables;
  }

  public String getName() {
    return name;
  }

  public ImmutableList<Pair<String, ColumnDesc>> getColumns() {
    return columns;
  }

  public ImmutableIntList getPrimaryKeyIndices() {
    return primaryKeyIndices;
  }

  public boolean isAppendOnly() {
    return appendOnly;
  }

  public boolean isMv() {
    return false;
  }

  public static Builder builder(String tableName) {
    return new Builder(tableName);
  }

  public boolean isSource() {
    return source;
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

  /** Builder class for CreateTableInfo. */
  public static class Builder {
    protected final String tableName;
    protected final List<Pair<String, ColumnDesc>> columns = new ArrayList<>();
    protected List<Integer> primaryKeyIndices = new ArrayList<>();
    protected Map<String, String> properties = new HashMap<>();
    protected boolean appendOnly = false;
    protected boolean mv = false;
    protected boolean source = false;
    protected String rowFormat = "";
    protected String rowSchemaLocation = "";
    protected List<TableRefId> dependentTables = new ArrayList<>();

    protected Builder(String tableName) {
      this.tableName = requireNonNull(tableName, "table name can't be null!");
    }

    public Builder addColumn(String name, ColumnDesc columnDesc) {
      columns.add(Pair.of(name, columnDesc));
      return this;
    }

    public Builder addPrimaryKey(Integer primaryKey) {
      this.primaryKeyIndices.add(primaryKey);
      return this;
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

    public void setSource(boolean source) {
      this.source = source;
    }

    public void setProperties(Map<String, String> properties) {
      this.properties = properties;
    }

    public void setRowFormat(String rowFormat) {
      this.rowFormat = rowFormat;
    }

    public void setDependentTables(List<Integer> dependentTables) {
      TableRefId.Builder builder = TableRefId.newBuilder();
      this.dependentTables =
          dependentTables.stream()
              .map(id -> builder.setTableId(id).build())
              .collect(Collectors.toList());
    }

    public CreateTableInfo build() {
      return new CreateTableInfo(
          tableName,
          ImmutableList.copyOf(columns),
          ImmutableIntList.copyOf(primaryKeyIndices),
          ImmutableMap.copyOf(properties),
          appendOnly,
          source,
          rowFormat,
          rowSchemaLocation,
          ImmutableList.copyOf(dependentTables));
    }
  }
}
