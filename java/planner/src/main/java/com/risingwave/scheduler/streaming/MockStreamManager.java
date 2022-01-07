package com.risingwave.scheduler.streaming;

import com.google.inject.Inject;
import com.risingwave.common.exception.PgErrorCode;
import com.risingwave.common.exception.PgException;
import com.risingwave.proto.plan.TableRefId;
import com.risingwave.proto.streaming.plan.StreamNode;

/** This mock implementation only work for test env init. */
public class MockStreamManager implements StreamManager {
  @Inject
  public MockStreamManager() {}

  @Override
  public void createMaterializedView(StreamNode streamNode, TableRefId tableRefId) {
    throw new PgException(PgErrorCode.INTERNAL_ERROR, "only used for test env config init.");
  }
}
