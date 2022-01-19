package com.risingwave.scheduler.query;

import com.google.common.base.Objects;
import java.util.UUID;

/** The id of Query */
public class QueryId {
  /** A random id. */
  private final String id;

  public QueryId(String id) {
    this.id = id;
  }

  public String getId() {
    return id;
  }

  public com.risingwave.proto.plan.QueryId toQueryIdProto() {
    return com.risingwave.proto.plan.QueryId.newBuilder().setTraceId(id).build();
  }

  public static QueryId next() {
    return new QueryId(UUID.randomUUID().toString());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    QueryId queryId = (QueryId) o;
    return Objects.equal(id, queryId.id);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(id);
  }

  @Override
  public String toString() {
    return id;
  }
}
