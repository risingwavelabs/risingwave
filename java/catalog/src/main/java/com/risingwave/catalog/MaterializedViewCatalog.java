package com.risingwave.catalog;

import com.google.common.collect.ImmutableMap;
import java.util.Collection;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.util.ImmutableIntList;

/** Materialized View Catalog */
public class MaterializedViewCatalog extends TableCatalog {

  private final RelCollation collation;
  private final RexNode offset;
  private final RexNode fetch;

  // TODO: remove fields offset/limit if not necessary any more in the future.
  public MaterializedViewCatalog(
      TableId id,
      TableName name,
      Collection<ColumnCatalog> columns,
      boolean stream,
      ImmutableIntList primaryKeyColumnIds,
      DataDistributionType distributionType,
      ImmutableMap<String, String> properties,
      String rowFormat,
      String rowSchemaLocation,
      RelCollation collation,
      RexNode offset,
      RexNode limit) {
    super(
        id,
        name,
        columns,
        stream,
        primaryKeyColumnIds,
        distributionType,
        properties,
        rowFormat,
        rowSchemaLocation);
    this.collation = collation;
    this.offset = offset;
    this.fetch = limit;
  }

  public RelCollation getCollation() {
    return collation;
  }

  public RexNode getOffset() {
    return offset;
  }

  public RexNode getFetch() {
    return fetch;
  }

  @Override
  public boolean isMaterializedView() {
    return true;
  }

  @Override
  public Schema.TableType getJdbcTableType() {
    return Schema.TableType.MATERIALIZED_VIEW;
  }
}
