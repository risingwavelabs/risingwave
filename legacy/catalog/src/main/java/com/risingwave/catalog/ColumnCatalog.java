package com.risingwave.catalog;

import com.google.common.base.MoreObjects;
import com.risingwave.common.entity.EntityBase;
import com.risingwave.common.entity.NonRootLikeBase;

/** Column Catalog Definition. */
public class ColumnCatalog extends EntityBase<ColumnCatalog.ColumnId, ColumnCatalog.ColumnName> {
  private final ColumnDesc desc;

  ColumnCatalog(ColumnId columnId, ColumnName name, ColumnDesc desc) {
    super(columnId, name);
    this.desc = desc;
  }

  public ColumnDesc getDesc() {
    return desc;
  }

  public String getName() {
    return getEntityName().getValue();
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("name", getName())
        .add("id", getId().getValue())
        .add("desc", desc)
        .toString();
  }

  /** Column Id Definition */
  public static class ColumnId extends NonRootLikeBase<Integer, TableCatalog.TableId> {
    public ColumnId(Integer value, TableCatalog.TableId parent) {
      super(value, parent);
    }
  }

  /** Column Name Definition */
  public static class ColumnName extends NonRootLikeBase<String, TableCatalog.TableName> {
    public ColumnName(String value, TableCatalog.TableName parent) {
      super(value, parent);
    }
  }
}
