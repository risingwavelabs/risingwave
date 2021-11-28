package com.risingwave.catalog;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import javax.annotation.Nullable;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Pair;

/**
 * Create Materialize View Info When we create a materialized view, there could be sort/limit/topN
 * at the end. We would achieve the sort part(if it is a sort or topN) at the materialized view, or
 * more specifically, at the storage of the materialized view.
 *
 * <p>The limit part(if it is a limit or topN) would be achieved when OLAP queries this materialized
 * view.
 *
 * <p>Therefore, we need to additionally keep collation(sort key/order), offset and limit in the
 * catalog so that we can modify the execution plan of an olap query on materialized view properly.
 *
 * <p>See BatchScanConverterRule for more information.
 */
public class CreateMaterializedViewInfo extends CreateTableInfo {
  private final RelCollation collation;
  // RexNode is used by Calcite to represent these two fields. Since we need to pass around offset
  // and limit
  // several times, and finally set them as RwBatchLimit's field. Here we follow
  // the convention of Calcite.
  private final RexNode offset;
  private final RexNode limit;

  private CreateMaterializedViewInfo(
      String tableName,
      ImmutableList<Pair<String, ColumnDesc>> columns,
      ImmutableMap<String, String> properties,
      boolean appendOnly,
      String rowFormat,
      String rowSchemaLocation,
      @Nullable RelCollation collation,
      @Nullable RexNode offset,
      @Nullable RexNode limit) {
    super(tableName, columns, properties, appendOnly, false, rowFormat, rowSchemaLocation);
    this.collation = collation;
    this.offset = offset;
    this.limit = limit;
  }

  public RelCollation getCollation() {
    return collation;
  }

  public RexNode getOffset() {
    return offset;
  }

  public RexNode getLimit() {
    return limit;
  }

  @Override
  public boolean isMv() {
    return true;
  }

  public static CreateMaterializedViewInfo.Builder builder(String tableName) {
    return new CreateMaterializedViewInfo.Builder(tableName);
  }

  /** Builder */
  public static class Builder extends CreateTableInfo.Builder {
    private RelCollation collation = null;
    private RexNode offset = null;
    private RexNode limit = null;

    private Builder(String tableName) {
      super(tableName);
    }

    public void setCollation(RelCollation collation) {
      this.collation = collation;
    }

    public void setOffset(RexNode offset) {
      this.offset = offset;
    }

    public void setLimit(RexNode limit) {
      this.limit = limit;
    }

    public CreateMaterializedViewInfo build() {
      return new CreateMaterializedViewInfo(
          tableName,
          ImmutableList.copyOf(columns),
          ImmutableMap.copyOf(properties),
          appendOnly,
          rowFormat,
          rowSchemaLocation,
          collation,
          offset,
          limit);
    }
  }
}
