package com.risingwave.planner;

import akka.actor.typed.ActorSystem;
import akka.actor.typed.SpawnProtocol;
import com.google.common.collect.Lists;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.risingwave.catalog.CatalogService;
import com.risingwave.catalog.SimpleCatalogService;
import com.risingwave.common.config.Configuration;
import com.risingwave.common.config.LeaderServerConfigurations;
import com.risingwave.execution.handler.DefaultSqlHandlerFactory;
import com.risingwave.execution.handler.SqlHandlerFactory;
import com.risingwave.node.LocalWorkerNodeManager;
import com.risingwave.node.WorkerNodeManager;
import com.risingwave.rpc.ComputeClientManager;
import com.risingwave.rpc.TestComputeClientManager;
import com.risingwave.scheduler.QueryManager;
import com.risingwave.scheduler.RemoteQueryManager;
import com.risingwave.scheduler.streaming.MockStreamManager;
import com.risingwave.scheduler.streaming.StreamManager;
import com.risingwave.scheduler.task.RemoteTaskManager;
import com.risingwave.scheduler.task.TaskManager;

/** Initialize the injection configuration for front end environments. */
public class TestPlannerModule extends AbstractModule {
  private final String dbName;
  private final String schemaName;

  public TestPlannerModule(String dbName, String schemaName) {
    this.dbName = dbName;
    this.schemaName = schemaName;
  }

  protected void configure() {
    bind(ComputeClientManager.class).to(TestComputeClientManager.class).in(Singleton.class);
    bind(TaskManager.class).to(RemoteTaskManager.class).in(Singleton.class);
    bind(QueryManager.class).to(RemoteQueryManager.class).in(Singleton.class);
    bind(WorkerNodeManager.class).to(LocalWorkerNodeManager.class).in(Singleton.class);
    bind(SqlHandlerFactory.class).to(DefaultSqlHandlerFactory.class).in(Singleton.class);
    bind(StreamManager.class).to(MockStreamManager.class).in(Singleton.class);
  }

  @Singleton
  @Provides
  CatalogService getCatalogService() {
    var catalogService = new SimpleCatalogService();
    catalogService.createDatabase(dbName, schemaName);
    return catalogService;
  }

  @Singleton
  @Provides
  static Configuration getConfiguration() {
    var cfg = new Configuration();
    cfg.set(LeaderServerConfigurations.COMPUTE_NODES, Lists.newArrayList("127.0.0.1:1234"));
    return cfg;
  }

  @Singleton
  @Provides
  static ActorSystem<SpawnProtocol.Command> getActorSystem() {
    return ActorSystem.create(SpawnProtocol.create(), "TestPlannerModule");
  }
}
