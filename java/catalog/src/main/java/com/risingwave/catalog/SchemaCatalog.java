package com.risingwave.catalog;

import com.risingwave.common.entity.EntityBase;
import com.risingwave.common.entity.NonRootLikeBase;
import com.risingwave.common.error.MetaServiceError;
import com.risingwave.common.exception.RisingWaveException;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.calcite.jdbc.CalciteSchemaWrapper;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.SchemaVersion;
import org.apache.calcite.schema.Table;
import org.apache.calcite.util.ImmutableIntList;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Schema Catalog Definition */
public class SchemaCatalog extends EntityBase<SchemaCatalog.SchemaId, SchemaCatalog.SchemaName>
    implements Schema {
  private final AtomicInteger nextTableId = new AtomicInteger(0);
  private final ConcurrentMap<TableCatalog.TableId, TableCatalog> tableById;
  private final ConcurrentMap<TableCatalog.TableName, TableCatalog> tableByName;
  private Long version;

  SchemaCatalog(SchemaId schemaId, SchemaName schemaName) {
    this(schemaId, schemaName, Collections.emptyList());
  }

  SchemaCatalog(SchemaId schemaId, SchemaName schemaName, Collection<TableCatalog> tables) {
    super(schemaId, schemaName);
    this.tableById = EntityBase.groupBy(tables, TableCatalog::getId);
    this.tableByName = EntityBase.groupBy(tables, TableCatalog::getEntityName);
  }

  public void setVersion(long version) {
    this.version = version;
  }

  public Long getVersion() {
    return this.version;
  }

  MaterializedViewCatalog createMaterializedView(
      CreateMaterializedViewInfo createMaterializedViewInfo) {
    var id = nextTableId.getAndIncrement();
    TableCatalog.TableName tableName =
        new TableCatalog.TableName(createMaterializedViewInfo.getName(), getEntityName());

    if (tableByName.containsKey(tableName)) {
      throw RisingWaveException.from(
          MetaServiceError.TABLE_ALREADY_EXISTS,
          tableName.getValue(),
          getDatabaseName(),
          getSchemaName());
    }

    TableCatalog.TableId viewId = new TableCatalog.TableId(id, getId());

    MaterializedViewCatalog view =
        new MaterializedViewCatalog(
            viewId,
            tableName,
            Collections.emptyList(),
            createMaterializedViewInfo.isStream(),
            ImmutableIntList.of(),
            DataDistributionType.ALL,
            createMaterializedViewInfo.getProperties(),
            createMaterializedViewInfo.getRowFormat(),
            createMaterializedViewInfo.getRowSchemaLocation(),
            createMaterializedViewInfo.getCollation(),
            createMaterializedViewInfo.getOffset(),
            createMaterializedViewInfo.getLimit());

    createMaterializedViewInfo
        .getColumns()
        .forEach(pair -> view.addColumn(pair.getKey(), pair.getValue()));

    registerTable(view);
    return view;
  }

  void createTable(CreateTableInfo createTableInfo) {
    var id = nextTableId.getAndIncrement();
    createTableWithId(createTableInfo, id);
  }

  TableCatalog createTableWithId(CreateTableInfo createTableInfo, Integer id) {
    TableCatalog.TableName tableName =
        new TableCatalog.TableName(createTableInfo.getName(), getEntityName());

    if (tableByName.containsKey(tableName)) {
      throw RisingWaveException.from(
          MetaServiceError.TABLE_ALREADY_EXISTS,
          tableName.getValue(),
          getDatabaseName(),
          getSchemaName());
    }

    TableCatalog.TableId tableId = new TableCatalog.TableId(id, getId());

    TableCatalog table =
        new TableCatalog(
            tableId,
            tableName,
            Collections.emptyList(),
            createTableInfo.isStream(),
            ImmutableIntList.of(),
            DataDistributionType.ALL,
            createTableInfo.getProperties(),
            createTableInfo.getRowFormat(),
            createTableInfo.getRowSchemaLocation());

    createTableInfo.getColumns().forEach(pair -> table.addColumn(pair.getKey(), pair.getValue()));

    registerTable(table);
    return table;
  }

  private void registerTable(TableCatalog table) {
    tableByName.put(table.getEntityName(), table);
    tableById.put(table.getId(), table);
  }

  void dropTable(String table) {
    TableCatalog.TableName tableName = new TableCatalog.TableName(table, getEntityName());
    TableCatalog tableCatalog = tableByName.remove(tableName);
    if (tableCatalog != null) {
      tableById.remove(tableCatalog.getId());
    }
  }

  private String getDatabaseName() {
    return getEntityName().getParent().getValue();
  }

  private String getSchemaName() {
    return getEntityName().getValue();
  }

  @Nullable
  public TableCatalog getTableCatalog(TableCatalog.TableName tableName) {
    return tableByName.get(tableName);
  }

  @Override
  @Nullable
  public Table getTable(String name) {
    return tableByName.get(new TableCatalog.TableName(name, getEntityName()));
  }

  @Override
  public Set<String> getTableNames() {
    return tableByName.keySet().stream()
        .map(TableCatalog.TableName::getValue)
        .collect(Collectors.toSet());
  }

  @Override
  @Nullable
  public RelProtoDataType getType(String name) {
    return null;
  }

  @Override
  public Set<String> getTypeNames() {
    return Collections.emptySet();
  }

  @Override
  public Collection<Function> getFunctions(String name) {
    return Collections.emptyList();
  }

  @Override
  public Set<String> getFunctionNames() {
    return Collections.emptySet();
  }

  @Override
  @Nullable
  public SchemaPlus getSubSchema(String name) {
    return null;
  }

  @Override
  public Set<String> getSubSchemaNames() {
    return Collections.emptySet();
  }

  @Override
  public Expression getExpression(@Nullable SchemaPlus parentSchema, String name) {
    return null;
  }

  @Override
  public boolean isMutable() {
    return false;
  }

  @Override
  public Schema snapshot(SchemaVersion version) {
    return this;
  }

  public CalciteSchemaWrapper toCalciteSchemaWrapper() {
    return new CalciteSchemaWrapper(this, getEntityName().getValue());
  }

  public SchemaPlus plus() {
    return toCalciteSchemaWrapper().plus();
  }

  /** Schema Id Definition */
  public static class SchemaId extends NonRootLikeBase<Integer, DatabaseCatalog.DatabaseId> {
    public SchemaId(Integer value, DatabaseCatalog.DatabaseId parent) {
      super(value, parent);
    }
  }

  /** Schema Name Definition */
  public static class SchemaName extends NonRootLikeBase<String, DatabaseCatalog.DatabaseName> {
    public SchemaName(String value, DatabaseCatalog.DatabaseName parent) {
      super(value, parent);
    }

    public static SchemaName of(String database, String schema) {
      return new SchemaName(schema, DatabaseCatalog.DatabaseName.of(database));
    }
  }
}
