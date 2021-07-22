package com.risingwave.catalog;

import com.risingwave.common.entity.EntityBase;
import com.risingwave.common.entity.NonRootLikeBase;

public class ColumnCatalog extends EntityBase<ColumnCatalog.ColumnId, ColumnCatalog.ColumnName> {
  private final ColumnDesc desc;

  ColumnCatalog(ColumnId columnId, ColumnName name, ColumnDesc desc) {
    super(columnId, name);
    this.desc = desc;
  }

  public ColumnDesc getDesc() {
    return desc;
  }

  public static class ColumnId extends NonRootLikeBase<Integer, TableCatalog.TableId> {
    public ColumnId(Integer value, TableCatalog.TableId parent) {
      super(value, parent);
    }
  }

  public static class ColumnName extends NonRootLikeBase<String, TableCatalog.TableName> {
    public ColumnName(String value, TableCatalog.TableName parent) {
      super(value, parent);
    }
  }
}
