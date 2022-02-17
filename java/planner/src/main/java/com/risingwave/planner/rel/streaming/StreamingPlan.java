package com.risingwave.planner.rel.streaming;

import com.risingwave.planner.rel.common.IdentityExtractor;
import org.apache.calcite.rel.RelNode;

/**
 * Plan for the stream execution. To be compatible with Calcite, a streaming plan is still a tree.
 * The root represents the result (sink, MV), and the leaf nodes represent the sources.
 *
 * <p>We remove the `serialize()` interface for StreamingPlan, as the serialization phase requires a
 * global id assigner. We defer the serialization implementation to later phases in the planning
 * procedure.
 */
public class StreamingPlan {
  private final RwStreamMaterializedView streamingPlan;

  public StreamingPlan(RwStreamMaterializedView streamingPlan) {
    this.streamingPlan = streamingPlan;
  }

  public RwStreamMaterializedView getStreamingPlan() {
    return streamingPlan;
  }

  public static String getCurrentNodeIdentity(RelNode node) {
    return IdentityExtractor.getCurrentNodeIdentity(node, "RwStream");
  }
}
