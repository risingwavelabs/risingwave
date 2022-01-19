package com.risingwave.scheduler.stage;

import com.google.common.base.Objects;
import com.risingwave.scheduler.query.QueryId;

/** The id of Stage in a query */
public class StageId {
  private final QueryId queryId;
  private final int id;

  public StageId(QueryId queryId, int id) {
    this.queryId = queryId;
    this.id = id;
  }

  public QueryId getQueryId() {
    return queryId;
  }

  public int getId() {
    return id;
  }

  public com.risingwave.proto.plan.StageId toStageIdProto() {
    return com.risingwave.proto.plan.StageId.newBuilder()
        .setQueryId(queryId.toQueryIdProto())
        .setStageId(id)
        .build();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    StageId stageId = (StageId) o;
    return id == stageId.id && Objects.equal(queryId, stageId.queryId);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(queryId, id);
  }

  @Override
  public String toString() {
    return queryId.getId() + "." + id;
  }
}
