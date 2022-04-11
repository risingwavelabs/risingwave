package com.risingwave.execution.handler.util;

import com.risingwave.common.exception.PgErrorCode;
import com.risingwave.common.exception.PgException;
import com.risingwave.execution.context.ExecutionContext;
import com.risingwave.proto.common.Status;
import com.risingwave.proto.computenode.CreateTaskRequest;
import com.risingwave.proto.computenode.CreateTaskResponse;
import com.risingwave.proto.computenode.GetDataRequest;
import com.risingwave.proto.computenode.GetDataResponse;
import com.risingwave.proto.plan.PlanFragment;
import com.risingwave.proto.plan.TaskOutputId;
import com.risingwave.rpc.ComputeClient;
import com.risingwave.rpc.ComputeClientManager;
import com.risingwave.rpc.Messages;
import java.util.ArrayList;
import java.util.Iterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The shared logic of broadcasting a create task request to all workers. */
public class CreateTaskBroadcaster {
  private static final Logger log = LoggerFactory.getLogger(CreateTaskBroadcaster.class);

  /**
   * Build a <code>CreateTaskRequest</code> from the plan fragment, and broadcast it to all clients.
   *
   * @param planFragment The plan fragment to be broadcast.
   * @param context The <code>ExecutionContext</code> for execution.
   */
  public static void broadCastTaskFromPlanFragment(
      PlanFragment planFragment, ExecutionContext context) {
    ComputeClientManager clientManager = context.getComputeClientManager();
    var responseIterators = new ArrayList<Iterator<GetDataResponse>>();
    for (var node : context.getWorkerNodeManager().allNodes()) {
      ComputeClient client = clientManager.getOrCreate(node);
      CreateTaskRequest createTaskRequest = Messages.buildCreateTaskRequest(planFragment);
      log.info("Send request to:" + node.getRpcEndPoint().toString());
      log.debug("Create task request:\n" + Messages.jsonFormat(createTaskRequest));
      CreateTaskResponse createTaskResponse = client.createTask(createTaskRequest);
      if (createTaskResponse.getStatus().getCode() != Status.Code.OK) {
        throw new PgException(PgErrorCode.INTERNAL_ERROR, "Create Task failed");
      }
      TaskOutputId taskOutputId = Messages.buildTaskOutputId(createTaskRequest.getTaskId());
      var iterator =
          client.getData(GetDataRequest.newBuilder().setTaskOutputId(taskOutputId).build());
      responseIterators.add(iterator);
    }
    // Wait for create table task finished.
    for (var iterator : responseIterators) {
      iterator.forEachRemaining((r) -> {});
    }
  }
}
