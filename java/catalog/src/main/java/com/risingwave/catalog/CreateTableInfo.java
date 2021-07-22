package com.risingwave.catalog;

import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.util.Pair;

public class CreateTableInfo {
  private final String name;
  private final ImmutableList<Pair<String, ColumnDesc>> columns;
  private final boolean appendOnly;
  private final boolean mv;

  private CreateTableInfo(
      String tableName,
      ImmutableList<Pair<String, ColumnDesc>> columns,
      boolean appendOnly,
      boolean mv) {
    this.name = tableName;
    this.columns = columns;
    this.appendOnly = appendOnly;
    this.mv = mv;
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

  public static class Builder {
    private final String tableName;
    private final List<Pair<String, ColumnDesc>> columns = new ArrayList<>();
    private boolean appendOnly = false;
    private boolean mv = false;

    private Builder(String tableName) {
      this.tableName = requireNonNull(tableName, "table name can't be null!");
    }

    public Builder addColumn(String name, ColumnDesc columnDesc) {
      columns.add(Pair.of(name, columnDesc));
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

    public CreateTableInfo build() {
      return new CreateTableInfo(tableName, ImmutableList.copyOf(columns), appendOnly, mv);
    }
  }
}
