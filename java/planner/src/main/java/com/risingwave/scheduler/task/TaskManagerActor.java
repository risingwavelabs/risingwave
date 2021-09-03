package com.risingwave.scheduler.task;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Objects.requireNonNull;

import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Receive;
import com.risingwave.node.WorkerNode;
import com.risingwave.node.WorkerNodeManager;
import com.risingwave.proto.common.Status;
import com.risingwave.proto.computenode.CreateTaskRequest;
import com.risingwave.proto.computenode.CreateTaskResponse;
import com.risingwave.rpc.ComputeClient;
import com.risingwave.rpc.ComputeClientManager;
import com.risingwave.rpc.Messages;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

class TaskManagerActor extends AbstractBehavior<TaskManagerEvent> {
  private final WorkerNodeManager nodeManager;
  private final ComputeClientManager clientManager;

  private final ConcurrentMap<TaskId, QueryTaskExecution> taskExecutions =
      new ConcurrentHashMap<>();

  public TaskManagerActor(
      ActorContext<TaskManagerEvent> context,
      WorkerNodeManager nodeManager,
      ComputeClientManager clientManager) {
    super(context);
    this.nodeManager = requireNonNull(nodeManager, "nodeManager");
    this.clientManager = requireNonNull(clientManager, "clientManager");
  }

  @Override
  public Receive<TaskManagerEvent> createReceive() {
    return newReceiveBuilder()
        .onMessage(TaskManagerEvent.ScheduleTaskEvent.class, this::scheduleTask)
        .build();
  }

  private Behavior<TaskManagerEvent> scheduleTask(TaskManagerEvent.ScheduleTaskEvent event) {
    QueryTaskExecution taskExecution = createTaskExecution(event);

    TaskId taskId = event.getTask().getTaskId();
    WorkerNode node = nodeManager.nextRandom();
    CreateTaskResponse response = sendCreateTaskRequest(event, node);

    if (response.getStatus().getCode() != Status.Code.OK) {
      getContext()
          .getLog()
          .error(
              "Failed to create query execution task: {}, {}",
              response.getStatus().getCode(),
              response.getStatus().getMessage());
      taskExecution.creationFailed(response.getStatus());
    } else {
      getContext().getLog().info("Succeeded to create query execution task: {}", taskId);
      taskExecution.taskCreated(node);
    }

    return this;
  }

  private QueryTaskExecution createTaskExecution(TaskManagerEvent.ScheduleTaskEvent event) {
    checkNotNull(event, "event");
    TaskId taskId = event.getTask().getTaskId();
    ;
    if (taskExecutions.containsKey(taskId)) {
      getContext().getLog().error("Task id {} already created.", taskId);
    }

    QueryTaskExecution taskExecution = QueryTaskExecution.from(event);
    taskExecutions.put(taskId, taskExecution);
    return taskExecution;
  }

  private CreateTaskResponse sendCreateTaskRequest(
      TaskManagerEvent.ScheduleTaskEvent event, WorkerNode node) {
    ComputeClient client = clientManager.getOrCreate(node);

    CreateTaskRequest request =
        CreateTaskRequest.newBuilder()
            .setTaskId(event.getTask().getTaskId().toTaskIdProto())
            .setPlan(event.getTask().getPlanFragment().toPlanFragmentProto())
            .build();

    getContext()
        .getLog()
        .debug(
            "Creating task {} on {}",
            Messages.jsonFormat(request.getPlan()),
            node.getRpcEndPoint());

    return client.createTask(request);
  }
}
