package com.risingwave.execution.handler;

import com.risingwave.common.config.Configuration;
import com.risingwave.common.config.FrontendServerConfigurations;
import com.risingwave.common.exception.PgErrorCode;
import com.risingwave.common.exception.PgException;
import com.risingwave.proto.computenode.CreateTaskRequest;
import com.risingwave.proto.computenode.CreateTaskResponse;
import com.risingwave.proto.computenode.ExchangeServiceGrpc;
import com.risingwave.proto.computenode.TaskData;
import com.risingwave.proto.computenode.TaskServiceGrpc;
import com.risingwave.proto.computenode.TaskSinkId;
import io.grpc.Channel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import java.util.Iterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RpcExecutorImpl implements RpcExecutor {
  private static final Logger LOGGER = LoggerFactory.getLogger(QueryHandler.class);
  private Channel channel;

  public RpcExecutorImpl(Configuration configuration) {
    // Prepare channel. FIXME: No TLS Support Yet.
    channel =
        ManagedChannelBuilder.forAddress(
                "localhost",
                configuration.get(FrontendServerConfigurations.COMPUTE_NODE_SERVER_PORT))
            .usePlaintext()
            .build();
  }

  @Override
  public CreateTaskResponse createTask(CreateTaskRequest request) {
    // Prepare Task service stub.
    TaskServiceGrpc.TaskServiceBlockingStub blockingTaskStub =
        TaskServiceGrpc.newBlockingStub(channel);
    CreateTaskResponse response;
    try {
      response = blockingTaskStub.create(request);
    } catch (StatusRuntimeException e) {
      LOGGER.atWarn().log("RPC failed: {0}", e.getStatus());
      throw new PgException(PgErrorCode.INTERNAL_ERROR, "Create task RPC failed");
    }
    return response;
  }

  @Override
  public Iterator<TaskData> getData(TaskSinkId taskSinkId) {
    // Prepare Exchange service stub.
    ExchangeServiceGrpc.ExchangeServiceBlockingStub blockingExchangeStub =
        ExchangeServiceGrpc.newBlockingStub(channel);
    Iterator<TaskData> taskDataIterator;
    try {
      taskDataIterator = blockingExchangeStub.getData(taskSinkId);
    } catch (StatusRuntimeException e) {
      LOGGER.atWarn().log("RPC failed: {0}", e.getStatus());
      throw new PgException(PgErrorCode.INTERNAL_ERROR, "Get data RPC failed");
    }
    return taskDataIterator;
  }
}
