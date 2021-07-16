package com.risingwave.catalog;

public class ColumnCatalog extends BaseEntity {
  private final int columnId;
  private final String name;
  private final ColumnDesc desc;

  public ColumnCatalog(int columnId, String name, ColumnDesc desc) {
    super(columnId, name);
    this.columnId = columnId;
    this.name = name;
    this.desc = desc;
  }

  public int getColumnId() {
    return columnId;
  }

  public String getName() {
    return name;
  }

  public ColumnDesc getDesc() {
    return desc;
  }
}
