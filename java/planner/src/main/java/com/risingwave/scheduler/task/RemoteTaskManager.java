package com.risingwave.scheduler.task;

import static java.util.Objects.requireNonNull;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.Behaviors;
import com.risingwave.rpc.ComputeClientManager;
import com.risingwave.scheduler.EventListener;
import com.risingwave.scheduler.actor.ActorFactory;
import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class RemoteTaskManager implements TaskManager {
  private final ComputeClientManager clientManager;

  private final ActorRef<TaskManagerEvent> actor;

  @Inject
  public RemoteTaskManager(ComputeClientManager clientManager, ActorFactory actorCreator) {
    this.clientManager = requireNonNull(clientManager, "clientManager");

    Behavior<TaskManagerEvent> taskManagerActor =
        Behaviors.setup(ctx -> new TaskManagerActor(ctx, clientManager));
    this.actor = actorCreator.createActor(taskManagerActor, "RemoteTaskManager");
  }

  @Override
  public void schedule(QueryTask task, EventListener<TaskEvent> taskEventListener) {
    actor.tell(new TaskManagerEvent.ScheduleTaskEvent(task, taskEventListener));
  }
}
