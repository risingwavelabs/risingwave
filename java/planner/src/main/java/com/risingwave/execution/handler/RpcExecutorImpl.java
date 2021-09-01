package com.risingwave.execution.handler;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.risingwave.common.config.Configuration;
import com.risingwave.common.config.LeaderServerConfigurations;
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

@Singleton
public class RpcExecutorImpl implements RpcExecutor {
  private static final Logger LOGGER = LoggerFactory.getLogger(QueryHandler.class);
  private Channel channel;

  @Inject
  public RpcExecutorImpl(Configuration cfg) {
    // TODO: Choose a random gather node.
    String gatherNodeAddress = cfg.get(LeaderServerConfigurations.COMPUTE_NODES).get(0);

    // Prepare channel. FIXME: No TLS Support Yet.
    channel = ManagedChannelBuilder.forTarget(gatherNodeAddress).usePlaintext().build();
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
      LOGGER.warn("RPC failed: {}", e.getStatus());
      throw rpcException("createTask", e);
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
      LOGGER.warn("RPC failed: {}", e.getStatus());
      throw rpcException("getData", e);
    }
    return taskDataIterator;
  }

  private static PgException rpcException(String rpcName, StatusRuntimeException e) {
    throw new PgException(PgErrorCode.INTERNAL_ERROR, "%s RPC failed: %s", rpcName, e.toString());
  }
}
