package com.risingwave.scheduler.streaming;

import com.google.inject.Inject;
import com.risingwave.proto.plan.TableRefId;
import com.risingwave.proto.streaming.plan.StreamNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** This mock implementation only work for test env init. */
public class MockStreamManager implements StreamManager {
  private static final Logger log = LoggerFactory.getLogger(MockStreamManager.class);

  @Inject
  public MockStreamManager() {}

  @Override
  public void createMaterializedView(StreamNode streamNode, TableRefId tableRefId) {
    log.debug("only used for test env config init.");
  }
}
