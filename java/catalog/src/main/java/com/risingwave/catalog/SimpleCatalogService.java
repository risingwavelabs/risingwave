package com.risingwave.catalog;

import static com.google.common.base.Preconditions.checkNotNull;

import com.risingwave.common.error.MetaServiceError;
import com.risingwave.common.exception.RisingWaveException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/** A naive in memory implementation of {@link CatalogService}. */
public class SimpleCatalogService implements CatalogService {
  private final AtomicInteger nextDatabaseId = new AtomicInteger(0);

  private final ConcurrentMap<DatabaseCatalog.DatabaseId, DatabaseCatalog> databaseById =
      new ConcurrentHashMap<>();
  private final ConcurrentMap<DatabaseCatalog.DatabaseName, DatabaseCatalog> databaseByName =
      new ConcurrentHashMap<>();

  @Override
  public synchronized DatabaseCatalog createDatabase(DatabaseCatalog.DatabaseName databaseName) {
    checkNotNull(databaseName, "database name can't be null!");
    if (databaseByName.containsKey(databaseName)) {
      throw RisingWaveException.from(MetaServiceError.DATABASE_ALREADY_EXISTS, databaseName);
    }

    int nextDatabaseId = this.nextDatabaseId.incrementAndGet();

    DatabaseCatalog database =
        new DatabaseCatalog(new DatabaseCatalog.DatabaseId(nextDatabaseId), databaseName);
    registerDatabase(database);

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
      String database, String schemaName, CreateTableInfo createTableInfo) {
    SchemaCatalog schema = getSchemaChecked(SchemaCatalog.SchemaName.of(database, schemaName));
    schema.createTable(createTableInfo);
    return schema.getTableCatalog(
        TableCatalog.TableName.of(database, schemaName, createTableInfo.getName()));
  }
}
