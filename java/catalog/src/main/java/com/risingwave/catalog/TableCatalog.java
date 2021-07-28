package com.risingwave.catalog;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.collect.ImmutableList;
import com.risingwave.common.entity.EntityBase;
import com.risingwave.common.entity.NonRootLikeBase;
import com.risingwave.common.error.MetaServiceError;
import com.risingwave.common.exception.RisingWaveException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
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

public class TableCatalog extends EntityBase<TableCatalog.TableId, TableCatalog.TableName>
    implements Table {
  private final AtomicInteger nextColumnId = new AtomicInteger(0);
  private final List<ColumnCatalog> columns;
  private final ConcurrentMap<ColumnCatalog.ColumnId, ColumnCatalog> columnById;
  private final ConcurrentMap<ColumnCatalog.ColumnName, ColumnCatalog> columnByName;
  private final boolean materializedView;
  private final ImmutableIntList primaryKeyColumnIds;
  private final DataDistributionType distributionType;
  private final Integer columnId;

  TableCatalog(
      TableId id,
      TableName name,
      Collection<ColumnCatalog> columns,
      boolean materializedView,
      ImmutableIntList primaryKeyColumnIds,
      DataDistributionType distributionType,
      Integer columnId) {
    super(id, name);
    this.columns = new ArrayList<>(columns);
    this.columnById = EntityBase.groupBy(columns, ColumnCatalog::getId);
    this.columnByName = EntityBase.groupBy(columns, ColumnCatalog::getEntityName);
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

  public ImmutableList<ColumnCatalog.ColumnId> getAllColumnIds() {
    return ImmutableList.copyOf(columnById.keySet());
  }

  public ImmutableList<ColumnCatalog.ColumnId> getAllColumnIdsSorted() {
    ColumnCatalog.ColumnId[] columnIds = columnById.keySet().toArray(new ColumnCatalog.ColumnId[0]);
    Arrays.sort(columnIds, Comparator.comparingInt(NonRootLikeBase::getValue));
    return ImmutableList.copyOf(columnIds);
  }

  public Optional<ColumnCatalog> getColumn(ColumnCatalog.ColumnId columnId) {
    checkNotNull(columnId, "column id can't be null!");
    return Optional.ofNullable(columnById.get(columnId));
  }

  public ColumnCatalog getColumnChecked(ColumnCatalog.ColumnId columnId) {
    // TODO: Use PgErrorCode
    return getColumn(columnId).orElseThrow(() -> new RuntimeException("Column id not found!"));
  }

  void addColumn(String name, ColumnDesc columnDesc) {
    ColumnCatalog.ColumnName columnName = new ColumnCatalog.ColumnName(name, getEntityName());
    if (columnByName.containsKey(columnName)) {
      throw RisingWaveException.from(MetaServiceError.COLUMN_ALREADY_EXISTS, name, getEntityName());
    }

    ColumnCatalog.ColumnId columnId =
        new ColumnCatalog.ColumnId(nextColumnId.incrementAndGet(), getId());

    ColumnCatalog column = new ColumnCatalog(columnId, columnName, columnDesc);
    registerColumn(column);
  }

  private void registerColumn(ColumnCatalog column) {
    columns.add(column);
    columnByName.put(column.getEntityName(), column);
    columnById.put(column.getId(), column);
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    List<RelDataType> columnDataTypes =
        columns.stream()
            .map(ColumnCatalog::getDesc)
            .map(ColumnDesc::getDataType)
            .collect(Collectors.toList());

    List<String> fieldNames =
        columns.stream()
            .map(ColumnCatalog::getEntityName)
            .map(ColumnCatalog.ColumnName::getValue)
            .collect(Collectors.toList());

    return typeFactory.createStructType(StructKind.FULLY_QUALIFIED, columnDataTypes, fieldNames);
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
  public boolean rolledUpColumnValidInsideAgg(
      String column,
      SqlCall call,
      @Nullable SqlNode parent,
      @Nullable CalciteConnectionConfig config) {
    return false;
  }

  public static class TableId extends NonRootLikeBase<Integer, SchemaCatalog.SchemaId> {

    public TableId(Integer value, SchemaCatalog.SchemaId parent) {
      super(value, parent);
    }
  }

  public static class TableName extends NonRootLikeBase<String, SchemaCatalog.SchemaName> {
    public TableName(String value, SchemaCatalog.SchemaName parent) {
      super(value, parent);
    }

    public static TableName of(String db, String schema, String table) {
      return new TableName(table, SchemaCatalog.SchemaName.of(db, schema));
    }
  }
}
