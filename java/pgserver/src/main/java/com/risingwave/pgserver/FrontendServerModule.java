package com.risingwave.pgserver;

import akka.actor.typed.ActorSystem;
import akka.actor.typed.SpawnProtocol;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.risingwave.catalog.CatalogService;
import com.risingwave.catalog.SimpleCatalogService;
import com.risingwave.common.config.Configuration;
import com.risingwave.common.config.FrontendServerConfigurations;
import com.risingwave.common.config.LeaderServerConfigurations;
import com.risingwave.execution.handler.DefaultSqlHandlerFactory;
import com.risingwave.execution.handler.RpcExecutor;
import com.risingwave.execution.handler.RpcExecutorImpl;
import com.risingwave.execution.handler.SqlHandlerFactory;
import com.risingwave.node.DefaultWorkerNodeManager;
import com.risingwave.node.WorkerNodeManager;
import com.risingwave.pgserver.database.RisingWaveDatabaseManager;
import com.risingwave.pgwire.PgServer;
import com.risingwave.pgwire.database.DatabaseManager;
import com.risingwave.rpc.ManagedRpcClientFactory;
import com.risingwave.rpc.RpcClientFactory;
import com.risingwave.scheduler.QueryManager;
import com.risingwave.scheduler.RemoteQueryManager;
import com.risingwave.scheduler.actor.ActorFactory;
import com.risingwave.scheduler.actor.DefaultActorFactory;
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
    bind(RpcExecutor.class).to(RpcExecutorImpl.class).in(Singleton.class);
    bind(WorkerNodeManager.class).to(DefaultWorkerNodeManager.class).in(Singleton.class);
    if (options.combineLeader) {
      bind(ActorFactory.class).to(DefaultActorFactory.class).in(Singleton.class);
      bind(TaskManager.class).to(RemoteTaskManager.class).in(Singleton.class);
      bind(QueryManager.class).to(RemoteQueryManager.class).in(Singleton.class);
      bind(RpcClientFactory.class).to(ManagedRpcClientFactory.class).in(Singleton.class);
    }
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
    int port = config.get(FrontendServerConfigurations.PG_WIRE_PORT);
    return new PgServer(port, dbManager);
  }

  @Provides
  @Singleton
  static CatalogService createCatalogService() {
    LOGGER.info("Creating catalog service.");
    SimpleCatalogService catalogService = new SimpleCatalogService();
    LOGGER.info("Creating default database: {}.", CatalogService.DEFAULT_DATABASE_NAME);

    catalogService.createDatabase(CatalogService.DEFAULT_DATABASE_NAME);
    return catalogService;
  }

  // Required by QueryManager.
  @Singleton
  @Provides
  static ActorSystem<SpawnProtocol.Command> getActorSystem() {
    return ActorSystem.create(SpawnProtocol.create(), "FrontendServerModule");
  }
}
