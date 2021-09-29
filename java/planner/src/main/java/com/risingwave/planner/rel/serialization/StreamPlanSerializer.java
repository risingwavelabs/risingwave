package com.risingwave.planner.rel.serialization;

import com.risingwave.execution.context.ExecutionContext;
import com.risingwave.planner.rel.physical.streaming.RisingWaveStreamingRel;
import com.risingwave.planner.rel.physical.streaming.StreamingPlan;
import com.risingwave.proto.streaming.plan.StreamNode;

/** A StreamPlanSerializer serialize a StreamingPlan into a protobuf. */
public class StreamPlanSerializer {
  private final ExecutionContext context;

  public StreamPlanSerializer(ExecutionContext context) {
    this.context = context;
  }

  public StreamNode serialize(StreamingPlan plan) {
    // For now, we assume that each streaming plan is a chain.
    RisingWaveStreamingRel rel = plan.getStreamingPlan();
    StreamNode node = rel.serialize();
    while (rel.getInputs().size() > 0) {
      RisingWaveStreamingRel downStream = (RisingWaveStreamingRel) rel.getInput(0);
      StreamNode downStreamNode = downStream.serialize();
      StreamNode reviseNode = downStreamNode.toBuilder().setDownstreamNode(node).build();
      node = reviseNode;
      rel = downStream;
    }
    return node;
  }
}
