package com.risingwave.planner.planner.streaming;

import com.risingwave.execution.context.ExecutionContext;
import com.risingwave.planner.rel.physical.streaming.RwStreamMaterializedView;
import com.risingwave.planner.rel.physical.streaming.StreamingPlan;
import org.apache.calcite.rel.RelNode;

/** A `StreamFragmenter` generates the proto for interconnected actors for a streaming pipeline. */
public class StreamFragmenter {

  private StreamFragmenter() {}

  public static StreamingPlan generateGraph(StreamingPlan streamingPlan, ExecutionContext context) {
    RelNode root = streamingPlan.getStreamingPlan();
    StreamingPlan newPlan = new StreamingPlan((RwStreamMaterializedView) root);
    return newPlan;
  }
}
