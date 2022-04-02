package com.risingwave.catalog;

import static com.google.common.base.Preconditions.checkNotNull;

import com.risingwave.common.error.MetaServiceError;
import com.risingwave.common.exception.PgErrorCode;
import com.risingwave.common.exception.PgException;
import com.risingwave.common.exception.RisingWaveException;
import com.risingwave.proto.common.Status;
import com.risingwave.proto.metanode.Catalog;
import com.risingwave.proto.metanode.CreateRequest;
import com.risingwave.proto.metanode.CreateResponse;
import com.risingwave.proto.metanode.Database;
import com.risingwave.proto.metanode.DropRequest;
import com.risingwave.proto.metanode.DropResponse;
import com.risingwave.proto.metanode.GetCatalogRequest;
import com.risingwave.proto.metanode.GetCatalogResponse;
import com.risingwave.proto.metanode.Schema;
import com.risingwave.proto.metanode.Table;
import com.risingwave.rpc.MetaClient;
import com.risingwave.rpc.MetaMessages;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import javax.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A remote persistent implementation using meta service of {@link CatalogService}. */
@Singleton
public class RemoteCatalogService implements CatalogService {
  private static final Logger LOGGER = LoggerFactory.getLogger(RemoteCatalogService.class);
  private final MetaClient metaClient;
  private static final long startWaitInterval = 1000;

  private final ConcurrentMap<DatabaseCatalog.DatabaseId, DatabaseCatalog> databaseById;
  private final ConcurrentMap<DatabaseCatalog.DatabaseName, DatabaseCatalog> databaseByName;

  public RemoteCatalogService(MetaClient client) {
    this.metaClient = client;
    this.databaseById = new ConcurrentHashMap<>();
    this.databaseByName = new ConcurrentHashMap<>();
    initCatalog();
  }

  private void initCatalog() {
    GetCatalogRequest request = GetCatalogRequest.newBuilder().build();
    GetCatalogResponse response;
    while (true) {
      try {
        TimeUnit.MILLISECONDS.sleep(startWaitInterval);
        response = this.metaClient.getCatalog(request);
        if (response.getStatus().getCode() == Status.Code.OK) {
          break;
        }
      } catch (Exception e) {
        LOGGER.warn("meta service unreachable, wait for start.");
        // ignore and retry.
      }
    }
    Catalog catalog = response.getCatalog();
    LOGGER.debug("Init catalog from meta service: {} ", catalog);

    for (Database database : catalog.getDatabasesList()) {
      DatabaseCatalog.DatabaseId databaseId =
          DatabaseCatalog.DatabaseId.of(database.getDatabaseRefId().getDatabaseId());
      DatabaseCatalog.DatabaseName databaseName =
          DatabaseCatalog.DatabaseName.of(database.getDatabaseName());
      DatabaseCatalog databaseCatalog = new DatabaseCatalog(databaseId, databaseName);
      databaseCatalog.setVersion(database.getVersion());
      registerDatabase(databaseCatalog);
    }

    for (Schema schema : catalog.getSchemasList()) {
      DatabaseCatalog.DatabaseId databaseId =
          DatabaseCatalog.DatabaseId.of(schema.getSchemaRefId().getDatabaseRefId().getDatabaseId());
      DatabaseCatalog databaseCatalog = getDatabaseById(databaseId);
      if (databaseCatalog == null) {
        throw RisingWaveException.from(MetaServiceError.DATABASE_NOT_EXISTS, databaseId);
      }
      databaseCatalog
          .createSchemaWithId(schema.getSchemaName(), schema.getSchemaRefId().getSchemaId())
          .setVersion(schema.getVersion());
    }

    for (Table table : catalog.getTablesList()) {
      DatabaseCatalog.DatabaseId databaseId =
          DatabaseCatalog.DatabaseId.of(
              table.getTableRefId().getSchemaRefId().getDatabaseRefId().getDatabaseId());
      SchemaCatalog.SchemaId schemaId =
          new SchemaCatalog.SchemaId(
              table.getTableRefId().getSchemaRefId().getSchemaId(), databaseId);
      DatabaseCatalog databaseCatalog = getDatabaseById(databaseId);
      if (databaseCatalog == null) {
        throw RisingWaveException.from(MetaServiceError.DATABASE_NOT_EXISTS, databaseId);
      }
      SchemaCatalog schemaCatalog = databaseCatalog.getSchemaById(schemaId);
      if (schemaCatalog == null) {
        throw RisingWaveException.from(MetaServiceError.SCHEMA_NOT_EXISTS, schemaId);
      }
      CreateTableInfo createTableInfo = CatalogCast.tableFromProto(table);
      var isMaterializedView = table.getInfoCase() == Table.InfoCase.MATERIALIZED_VIEW;
      if (isMaterializedView) {
        schemaCatalog
            .createMaterializedViewWithId(
                (CreateMaterializedViewInfo) createTableInfo, table.getTableRefId().getTableId())
            .setVersion(table.getVersion());
      } else {
        schemaCatalog
            .createTableWithId(createTableInfo, table.getTableRefId().getTableId())
            .setVersion(table.getVersion());
      }
    }
  }

  @Override
  public synchronized DatabaseCatalog createDatabase(String dbName, String schemaName) {
    DatabaseCatalog.DatabaseName databaseName = DatabaseCatalog.DatabaseName.of(dbName);
    checkNotNull(databaseName, "database name can't be null!");
    if (databaseByName.containsKey(databaseName)) {
      throw RisingWaveException.from(MetaServiceError.DATABASE_ALREADY_EXISTS, databaseName);
    }
    LOGGER.debug("create database: {}:{}", dbName, schemaName);

    CreateRequest request =
        MetaMessages.buildCreateDatabaseRequest(CatalogCast.databaseToProto(dbName));
    CreateResponse response = this.metaClient.create(request);
    if (response.getStatus().getCode() != Status.Code.OK) {
      throw new PgException(PgErrorCode.INTERNAL_ERROR, "create database failed");
    }
    DatabaseCatalog database =
        new DatabaseCatalog(new DatabaseCatalog.DatabaseId(response.getId()), databaseName);
    database.setVersion(response.getVersion());

    registerDatabase(database);
    createSchema(new SchemaCatalog.SchemaName(schemaName, databaseName));

    return database;
  }

  private void registerDatabase(DatabaseCatalog database) {
    databaseByName.put(database.getEntityName(), database);
    databaseById.put(database.getId(), database);
  }

  private DatabaseCatalog getDatabaseById(DatabaseCatalog.DatabaseId databaseId) {
    return databaseById.get(databaseId);
  }

  @Override
  public DatabaseCatalog getDatabase(DatabaseCatalog.DatabaseName databaseName) {
    return databaseByName.get(databaseName);
  }

  @Override
  public SchemaCatalog getSchema(SchemaCatalog.SchemaName schemaName) {
    return getDatabaseChecked(schemaName.getParent()).getSchema(schemaName);
  }

  @Override
  public SchemaCatalog createSchema(SchemaCatalog.SchemaName schemaName) {
    LOGGER.debug("create schema: {}", schemaName);
    DatabaseCatalog databaseCatalog = getDatabaseChecked(schemaName.getParent());
    CreateRequest request =
        MetaMessages.buildCreateSchemaRequest(
            CatalogCast.schemaToProto(databaseCatalog, schemaName));
    CreateResponse response = this.metaClient.create(request);
    if (response.getStatus().getCode() != Status.Code.OK) {
      throw new PgException(PgErrorCode.INTERNAL_ERROR, "create schema failed");
    }
    SchemaCatalog schemaCatalog =
        databaseCatalog.createSchemaWithId(schemaName.getValue(), response.getId());
    schemaCatalog.setVersion(response.getVersion());

    return schemaCatalog;
  }

  @Override
  public synchronized TableCatalog createTable(
      SchemaCatalog.SchemaName schemaName, CreateTableInfo createTableInfo) {
    LOGGER.debug("create table: {}:{}", createTableInfo.getName(), schemaName);
    SchemaCatalog schema = getSchemaChecked(schemaName);
    TableCatalog.TableName tableName =
        new TableCatalog.TableName(createTableInfo.getName(), schemaName);
    CreateRequest request =
        MetaMessages.buildCreateTableRequest(
            CatalogCast.tableToProto(
                getDatabaseChecked(schemaName.getParent()),
                getSchemaChecked(tableName.getParent()),
                createTableInfo));
    CreateResponse response = this.metaClient.create(request);
    if (response.getStatus().getCode() != Status.Code.OK) {
      throw new PgException(PgErrorCode.INTERNAL_ERROR, "create table failed");
    }
    TableCatalog tableCatalog = schema.createTableWithId(createTableInfo, response.getId());
    tableCatalog.setVersion(response.getVersion());

    return tableCatalog;
  }

  @Override
  public MaterializedViewCatalog createMaterializedView(
      SchemaCatalog.SchemaName schemaName, CreateMaterializedViewInfo createMaterializedViewInfo) {
    LOGGER.debug(
        "create materialized view: {}:{}", createMaterializedViewInfo.getName(), schemaName);
    SchemaCatalog schema = getSchemaChecked(schemaName);
    TableCatalog.TableName viewName =
        new TableCatalog.TableName(createMaterializedViewInfo.getName(), schemaName);
    CreateRequest request =
        MetaMessages.buildCreateTableRequest(
            CatalogCast.tableToProto(
                getDatabaseChecked(schemaName.getParent()),
                getSchemaChecked(viewName.getParent()),
                createMaterializedViewInfo));
    CreateResponse response = this.metaClient.create(request);
    if (response.getStatus().getCode() != Status.Code.OK) {
      throw new PgException(PgErrorCode.INTERNAL_ERROR, "create materialized view failed");
    }
    MaterializedViewCatalog viewCatalog =
        schema.createMaterializedViewWithId(createMaterializedViewInfo, response.getId());
    viewCatalog.setVersion(response.getVersion());
    return viewCatalog;
  }

  @Override
  public TableCatalog getTable(TableCatalog.TableName tableName) {
    return getSchemaChecked(tableName.getParent()).getTableCatalog(tableName);
  }

  @Override
  public void dropTable(TableCatalog.TableName tableName) {
    SchemaCatalog schemaCatalog = getSchemaChecked(tableName.getParent());
    DatabaseCatalog databaseCatalog = getDatabaseChecked(schemaCatalog.getEntityName().getParent());
    DropRequest request =
        MetaMessages.buildDropTableRequest(
            CatalogCast.buildTableRefId(databaseCatalog, schemaCatalog, tableName));
    DropResponse response = this.metaClient.drop(request);
    if (response.getStatus().getCode() != Status.Code.OK) {
      throw new PgException(PgErrorCode.INTERNAL_ERROR, "drop table failed");
    }

    getSchemaChecked(tableName.getParent()).dropTable(tableName.getValue());
  }
}
