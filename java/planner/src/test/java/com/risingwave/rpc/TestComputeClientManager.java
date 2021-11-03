package com.risingwave.rpc;

import com.risingwave.node.WorkerNode;
import com.risingwave.proto.computenode.CreateTaskRequest;
import com.risingwave.proto.computenode.CreateTaskResponse;
import com.risingwave.proto.computenode.GetDataResponse;
import com.risingwave.proto.computenode.TaskSinkId;
import java.util.ArrayList;
import java.util.Iterator;
import javax.inject.Singleton;

/** A mocked ComputeClientManager. */
@Singleton
public class TestComputeClientManager implements ComputeClientManager {
  /** A mocked ComputeClient. */
  public static class TestClient implements ComputeClient {
    @Override
    public CreateTaskResponse createTask(CreateTaskRequest request) {
      return CreateTaskResponse.newBuilder().build();
    }

    @Override
    public Iterator<GetDataResponse> getData(TaskSinkId taskSinkId) {
      ArrayList<GetDataResponse> stream = new ArrayList<>();
      return stream.iterator();
    }
  }

  @Override
  public ComputeClient getOrCreate(WorkerNode node) {
    return new TestClient();
  }
}
