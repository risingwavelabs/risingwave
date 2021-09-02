package com.risingwave.rpc;

import com.risingwave.node.WorkerNode;
import com.risingwave.proto.computenode.CreateTaskRequest;
import com.risingwave.proto.computenode.CreateTaskResponse;
import com.risingwave.proto.computenode.TaskData;
import com.risingwave.proto.computenode.TaskSinkId;
import java.util.ArrayList;
import java.util.Iterator;
import javax.inject.Singleton;

@Singleton
public class TestComputeClientManager implements ComputeClientManager {
  public static class TestClient implements ComputeClient {
    @Override
    public CreateTaskResponse createTask(CreateTaskRequest request) {
      return CreateTaskResponse.newBuilder().build();
    }

    @Override
    public Iterator<TaskData> getData(TaskSinkId taskSinkId) {
      ArrayList<TaskData> taskData = new ArrayList<>();
      return taskData.iterator();
    }
  }

  @Override
  public ComputeClient getOrCreate(WorkerNode node) {
    return new TestClient();
  }
}
