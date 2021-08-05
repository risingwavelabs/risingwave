package com.risingwave.scheduler.task;

import static java.util.Objects.requireNonNull;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.Behaviors;
import com.risingwave.node.WorkerNodeManager;
import com.risingwave.rpc.RpcClientFactory;
import com.risingwave.scheduler.EventListener;
import com.risingwave.scheduler.actor.ActorFactory;
import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class RemoteTaskManager implements TaskManager {
  private final WorkerNodeManager nodeManager;
  private final RpcClientFactory clientFactory;

  private final ActorRef<TaskManagerEvent> actor;

  @Inject
  public RemoteTaskManager(
      WorkerNodeManager nodeManager, RpcClientFactory clientFactory, ActorFactory actorCreator) {
    this.nodeManager = requireNonNull(nodeManager, "nodeManager");
    this.clientFactory = requireNonNull(clientFactory, "clientFactory");

    Behavior<TaskManagerEvent> taskManagerActor =
        Behaviors.setup(ctx -> new TaskManagerActor(ctx, nodeManager, clientFactory));
    this.actor = actorCreator.createActor(taskManagerActor, "RemoteTaskManager");
  }

  @Override
  public void schedule(QueryTask task, EventListener<TaskEvent> taskEventListener) {
    actor.tell(new TaskManagerEvent.ScheduleTaskEvent(task, taskEventListener));
  }
}
