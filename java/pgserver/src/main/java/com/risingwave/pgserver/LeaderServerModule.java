package com.risingwave.pgserver;

import akka.actor.typed.ActorSystem;
import akka.actor.typed.SpawnProtocol;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.risingwave.common.config.Configuration;
import com.risingwave.node.DefaultWorkerNodeManager;
import com.risingwave.node.WorkerNodeManager;
import com.risingwave.rpc.ComputeClientManager;
import com.risingwave.rpc.ComputeClientManagerImpl;
import com.risingwave.scheduler.QueryManager;
import com.risingwave.scheduler.RemoteQueryManager;
import com.risingwave.scheduler.streaming.StreamManager;
import com.risingwave.scheduler.streaming.StreamManagerImpl;
import com.risingwave.scheduler.task.RemoteTaskManager;
import com.risingwave.scheduler.task.TaskManager;

/** Leader server singletons. */
public class LeaderServerModule extends AbstractModule {
  private final Configuration config;

  public LeaderServerModule(Configuration config) {
    this.config = config;
  }

  @Override
  protected void configure() {
    bind(WorkerNodeManager.class).to(DefaultWorkerNodeManager.class).in(Singleton.class);
    bind(TaskManager.class).to(RemoteTaskManager.class).in(Singleton.class);
    bind(QueryManager.class).to(RemoteQueryManager.class).in(Singleton.class);
    bind(ComputeClientManager.class).to(ComputeClientManagerImpl.class).in(Singleton.class);
    bind(StreamManager.class).to(StreamManagerImpl.class).in(Singleton.class);
  }

  @Provides
  @Singleton
  Configuration provideConfiguration() {
    return config;
  }

  @Singleton
  @Provides
  static ActorSystem<SpawnProtocol.Command> getActorSystem() {
    return ActorSystem.create(SpawnProtocol.create(), "LeaderServerModule");
  }
}
