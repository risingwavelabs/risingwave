package com.risingwave.scheduler.streaming;

import com.risingwave.proto.plan_common.TableRefId;
import com.risingwave.proto.streaming.plan.StreamNode;

/** The interface for managing stream actors among distributed compute nodes. */
public interface StreamManager {
  void createMaterializedView(StreamNode streamNode, TableRefId tableRefId);
}
