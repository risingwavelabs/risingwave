package com.risingwave.scheduler.streaming;

import com.google.inject.Inject;
import com.risingwave.common.exception.PgErrorCode;
import com.risingwave.common.exception.PgException;
import com.risingwave.proto.common.Status;
import com.risingwave.proto.metanode.CreateMaterializedViewRequest;
import com.risingwave.proto.metanode.CreateMaterializedViewResponse;
import com.risingwave.proto.plan.TableRefId;
import com.risingwave.proto.streaming.plan.StreamNode;
import com.risingwave.rpc.MetaClient;
import com.risingwave.scheduler.streaming.graph.StreamGraph;
import java.util.List;

/** The implementation of a stream manager synchronized with meta service. */
public class RemoteStreamManager implements StreamManager {
  private final MetaClient metaClient;

  @Inject
  public RemoteStreamManager(MetaClient client) {
    this.metaClient = client;
  }

  @Override
  public int createFragment() {
    throw new UnsupportedOperationException("not supported, use createMaterializedView instead.");
  }

  @Override
  public List<StreamRequest> scheduleStreamGraph(StreamGraph graph) {
    throw new UnsupportedOperationException("not supported, use createMaterializedView instead.");
  }

  @Override
  public String nextScheduleId() {
    throw new UnsupportedOperationException("not supported, use createMaterializedView instead.");
  }

  @Override
  public ActorInfoTable getActorInfo(List<Integer> actorIdList) {
    throw new UnsupportedOperationException("not supported, use createMaterializedView instead.");
  }

  public void createMaterializedView(StreamNode streamNode, TableRefId tableRefId) {
    CreateMaterializedViewRequest.Builder builder = CreateMaterializedViewRequest.newBuilder();
    builder.setStreamNode(streamNode);
    builder.setTableRefId(tableRefId);
    CreateMaterializedViewResponse response = metaClient.createMaterializedView(builder.build());
    if (response.getStatus().getCode() != Status.Code.OK) {
      throw new PgException(PgErrorCode.INTERNAL_ERROR, "Create materialized view failed");
    }
  }
}
