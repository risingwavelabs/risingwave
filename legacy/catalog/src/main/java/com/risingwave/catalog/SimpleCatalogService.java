package com.risingwave.catalog;

import static com.google.common.base.Preconditions.checkNotNull;

import com.risingwave.common.error.MetaServiceError;
import com.risingwave.common.exception.RisingWaveException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import javax.inject.Singleton;

/** A naive in memory implementation of {@link CatalogService}. */
@Singleton
public class SimpleCatalogService implements CatalogService {

  private final AtomicInteger nextDatabaseId = new AtomicInteger(0);

  private final ConcurrentMap<DatabaseCatalog.DatabaseId, DatabaseCatalog> databaseById =
      new ConcurrentHashMap<>();
  private final ConcurrentMap<DatabaseCatalog.DatabaseName, DatabaseCatalog> databaseByName =
      new ConcurrentHashMap<>();

  @Override
  public synchronized DatabaseCatalog createDatabase(String dbName, String schemaName) {
    DatabaseCatalog.DatabaseName databaseName = DatabaseCatalog.DatabaseName.of(dbName);
    checkNotNull(databaseName, "database name can't be null!");
    if (databaseByName.containsKey(databaseName)) {
      throw RisingWaveException.from(MetaServiceError.DATABASE_ALREADY_EXISTS, databaseName);
    }

    DatabaseCatalog database =
        new DatabaseCatalog(
            new DatabaseCatalog.DatabaseId(nextDatabaseId.getAndIncrement()), databaseName);
    registerDatabase(database);

    database.createSchema(schemaName);

    return database;
  }

  private void registerDatabase(DatabaseCatalog database) {
    databaseByName.put(database.getEntityName(), database);
    databaseById.put(database.getId(), database);
  }

  @Override
  public DatabaseCatalog getDatabase(DatabaseCatalog.DatabaseName databaseName) {
    return databaseByName.get(databaseName);
  }

  @Override
  public synchronized SchemaCatalog createSchema(SchemaCatalog.SchemaName schemaName) {
    DatabaseCatalog databaseCatalog = getDatabaseChecked(schemaName.getParent());
    databaseCatalog.createSchema(schemaName.getValue());
    return databaseCatalog.getSchema(schemaName);
  }

  @Override
  public SchemaCatalog getSchema(SchemaCatalog.SchemaName schemaName) {
    return getDatabaseChecked(schemaName.getParent()).getSchema(schemaName);
  }

  @Override
  public synchronized TableCatalog createTable(
      SchemaCatalog.SchemaName schemaName, CreateTableInfo createTableInfo) {
    SchemaCatalog schema = getSchemaChecked(schemaName);
    schema.createTable(createTableInfo);
    return schema.getTableCatalog(
        new TableCatalog.TableName(createTableInfo.getName(), schemaName));
  }

  @Override
  public MaterializedViewCatalog createMaterializedView(
      SchemaCatalog.SchemaName schemaName, CreateMaterializedViewInfo createMaterializedViewInfo) {
    SchemaCatalog schema = getSchemaChecked(schemaName);
    schema.createMaterializedView(createMaterializedViewInfo);
    return (MaterializedViewCatalog)
        schema.getTableCatalog(
            new TableCatalog.TableName(createMaterializedViewInfo.getName(), schemaName));
  }

  @Override
  public TableCatalog getTable(TableCatalog.TableName tableName) {
    return getSchemaChecked(tableName.getParent()).getTableCatalog(tableName);
  }

  @Override
  public void dropTable(TableCatalog.TableName tableName) {
    getSchemaChecked(tableName.getParent()).dropTable(tableName.getValue());
  }
}
