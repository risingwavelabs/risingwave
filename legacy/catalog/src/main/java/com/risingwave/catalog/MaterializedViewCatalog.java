package com.risingwave.catalog;

import com.google.common.collect.ImmutableMap;
import com.risingwave.proto.plan.RowFormatType;
import java.util.Collection;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.util.ImmutableIntList;

/** Materialized View Catalog */
public class MaterializedViewCatalog extends TableCatalog {

  private final RelCollation collation;

  private final TableId associated;

  public MaterializedViewCatalog(
      TableId id,
      TableName name,
      Collection<ColumnCatalog> columns,
      boolean stream,
      ImmutableIntList primaryKeyIndices,
      DataDistributionType distributionType,
      ImmutableMap<String, String> properties,
      RowFormatType rowFormat,
      String rowSchemaLocation,
      RelCollation collation,
      TableId associated) {
    super(
        id,
        name,
        columns,
        stream,
        primaryKeyIndices,
        distributionType,
        properties,
        rowFormat,
        rowSchemaLocation);
    this.collation = collation;
    this.associated = associated;
  }

  public RelCollation getCollation() {
    return collation;
  }

  @Override
  public boolean isMaterializedView() {
    return true;
  }

  @Override
  public boolean isAssociatedMaterializedView() {
    return associated != null;
  }

  public TableId getAssociatedTableId() {
    return associated;
  }

  @Override
  public Schema.TableType getJdbcTableType() {
    return Schema.TableType.MATERIALIZED_VIEW;
  }
}
