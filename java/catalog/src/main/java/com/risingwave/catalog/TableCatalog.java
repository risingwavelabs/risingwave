package com.risingwave.catalog;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.StructKind;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.util.ImmutableIntList;
import org.checkerframework.checker.nullness.qual.Nullable;

public class TableCatalog extends AbstractNonLeafEntity<ColumnCatalog> implements Table {
  private final boolean materializedView;
  private final ImmutableIntList primaryKeyColumnIds;
  private final DataDistributionType distributionType;
  private final Integer columnId;

  public TableCatalog(int id, String name, Collection<ColumnCatalog> columns,
                      boolean materializedView,
                      ImmutableIntList primaryKeyColumnIds, DataDistributionType distributionType,
                      Integer columnId) {
    super(id, name, columns);
    this.materializedView = materializedView;
    this.primaryKeyColumnIds = primaryKeyColumnIds;
    this.distributionType = distributionType;
    this.columnId = columnId;
  }

  public boolean isMaterializedView() {
    return materializedView;
  }

  public ImmutableIntList getPrimaryKeyColumnIds() {
    return primaryKeyColumnIds;
  }

  public DataDistributionType getDistributionType() {
    return distributionType;
  }

  public Integer getColumnId() {
    return columnId;
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    List<RelDataType> columnDataTypes = getChildren()
        .stream()
        .map(ColumnCatalog::getDesc)
        .map(ColumnDesc::getDataType)
        .collect(Collectors.toList());

    List<String> fieldNames = getChildren()
        .stream()
        .map(ColumnCatalog::getName)
        .collect(Collectors.toList());

    return typeFactory
        .createStructType(StructKind.FULLY_QUALIFIED, columnDataTypes, fieldNames);
  }

  @Override
  public Statistic getStatistic() {
    return Statistics.UNKNOWN;
  }

  @Override
  public Schema.TableType getJdbcTableType() {
    return materializedView ? Schema.TableType.MATERIALIZED_VIEW : Schema.TableType.TABLE;
  }

  @Override
  public boolean isRolledUp(String column) {
    return false;
  }

  @Override
  public boolean rolledUpColumnValidInsideAgg(String column, SqlCall call,
                                              @Nullable SqlNode parent,
                                              @Nullable CalciteConnectionConfig config) {
    return false;
  }
}
