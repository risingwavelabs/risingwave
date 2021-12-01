package com.risingwave.pgserver;

import akka.actor.typed.ActorSystem;
import akka.actor.typed.SpawnProtocol;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.risingwave.catalog.CatalogService;
import com.risingwave.catalog.DatabaseCatalog;
import com.risingwave.catalog.RemoteCatalogService;
import com.risingwave.catalog.SimpleCatalogService;
import com.risingwave.common.config.Configuration;
import com.risingwave.common.config.FrontendServerConfigurations;
import com.risingwave.common.config.LeaderServerConfigurations;
import com.risingwave.execution.handler.DefaultSqlHandlerFactory;
import com.risingwave.execution.handler.SqlHandlerFactory;
import com.risingwave.node.DefaultWorkerNode;
import com.risingwave.node.DefaultWorkerNodeManager;
import com.risingwave.node.WorkerNodeManager;
import com.risingwave.pgserver.database.RisingWaveDatabaseManager;
import com.risingwave.pgwire.PgServer;
import com.risingwave.pgwire.database.DatabaseManager;
import com.risingwave.rpc.ComputeClientManager;
import com.risingwave.rpc.ComputeClientManagerImpl;
import com.risingwave.rpc.MetaClient;
import com.risingwave.rpc.MetaClientManager;
import com.risingwave.rpc.MetaClientManagerImpl;
import com.risingwave.scheduler.QueryManager;
import com.risingwave.scheduler.RemoteQueryManager;
import com.risingwave.scheduler.streaming.StreamManager;
import com.risingwave.scheduler.streaming.StreamManagerImpl;
import com.risingwave.scheduler.task.RemoteTaskManager;
import com.risingwave.scheduler.task.TaskManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This module produces server-level singletons, which can be referenced by other classes via
 * com.google.inject.Inject annotated methods.
 */
public class FrontendServerModule extends AbstractModule {
  private static final Logger LOGGER = LoggerFactory.getLogger(FrontendServerModule.class);
  private final FrontendServerOptions options;

  public FrontendServerModule(FrontendServerOptions options) {
    this.options = options;
  }

  @Override
  protected void configure() {
    bind(DatabaseManager.class).to(RisingWaveDatabaseManager.class).in(Singleton.class);
    bind(SqlHandlerFactory.class).to(DefaultSqlHandlerFactory.class).in(Singleton.class);
    bind(ComputeClientManager.class).to(ComputeClientManagerImpl.class).in(Singleton.class);
    bind(WorkerNodeManager.class).to(DefaultWorkerNodeManager.class).in(Singleton.class);
    if (options.combineLeader) {
      bind(TaskManager.class).to(RemoteTaskManager.class).in(Singleton.class);
      bind(QueryManager.class).to(RemoteQueryManager.class).in(Singleton.class);
    }
    bind(StreamManager.class).to(StreamManagerImpl.class).in(Singleton.class);
    bind(MetaClientManager.class).to(MetaClientManagerImpl.class).in(Singleton.class);
  }

  @Provides
  @Singleton
  Configuration systemConfig() {
    LOGGER.info("Loading server configuration at {}", options.configFile);
    if (!options.combineLeader) {
      return Configuration.load(options.configFile, FrontendServerConfigurations.class);
    }
    return Configuration.load(
        options.configFile, FrontendServerConfigurations.class, LeaderServerConfigurations.class);
  }

  @Provides
  @Singleton
  static PgServer createPgServer(Configuration config, DatabaseManager dbManager) {
    LOGGER.info("Creating pg server.");
    String ip = config.get(FrontendServerConfigurations.PG_WIRE_IP);
    int port = config.get(FrontendServerConfigurations.PG_WIRE_PORT);
    return new PgServer(ip, port, dbManager);
  }

  @Provides
  @Singleton
  static CatalogService createCatalogService(
      Configuration config, MetaClientManager metaClientManager) {
    LOGGER.info("Creating catalog service.");
    FrontendServerConfigurations.CatalogMode mode =
        config.get(FrontendServerConfigurations.CATALOG_MODE);
    CatalogService catalogService;
    if (mode == FrontendServerConfigurations.CatalogMode.Local) {
      catalogService = new SimpleCatalogService();

      catalogService.createDatabase(CatalogService.DEFAULT_DATABASE_NAME);
      LOGGER.info("Creating default database: {}.", CatalogService.DEFAULT_DATABASE_NAME);
    } else {
      String address = config.get(FrontendServerConfigurations.META_SERVICE_ADDRESS);
      DefaultWorkerNode node = DefaultWorkerNode.from(address);
      MetaClient client =
          metaClientManager.getOrCreate(
              node.getRpcEndPoint().getHost(), node.getRpcEndPoint().getPort());
      catalogService = new RemoteCatalogService(client);

      if (catalogService.getDatabase(
              new DatabaseCatalog.DatabaseName(CatalogService.DEFAULT_DATABASE_NAME))
          == null) {

        // For multi frontends, this may cause multi databases' creation when cluster start
        // at the first time. That's acceptable because only at most N(number of frontends)
        // versions of default database will be created in meta service, that doesn't cause any
        // inconsistency problem. The newest version will be synced eventually.
        catalogService.createDatabase(CatalogService.DEFAULT_DATABASE_NAME);
        LOGGER.info("Creating default database: {}.", CatalogService.DEFAULT_DATABASE_NAME);
      }
    }
    return catalogService;
  }

  // Required by QueryManager.
  @Singleton
  @Provides
  static ActorSystem<SpawnProtocol.Command> getActorSystem() {
    return ActorSystem.create(SpawnProtocol.create(), "FrontendServerModule");
  }
}
