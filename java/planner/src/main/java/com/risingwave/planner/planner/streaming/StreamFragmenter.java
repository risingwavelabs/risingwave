package com.risingwave.planner.planner.streaming;

import static com.risingwave.planner.rules.streaming.StreamingParallelPipelineRules.STREAMING_PARALLEL_RULES;

import com.risingwave.execution.context.ExecutionContext;
import com.risingwave.planner.program.HepOptimizerProgram;
import com.risingwave.planner.program.OptimizerProgram;
import com.risingwave.planner.rel.physical.streaming.RwStreamMaterializedView;
import com.risingwave.planner.rel.physical.streaming.StreamingPlan;
import org.apache.calcite.rel.RelNode;

public class StreamFragmenter {

  private StreamFragmenter() {}

  public static StreamingPlan generateGraph(StreamingPlan streamingPlan, ExecutionContext context) {
    RelNode root = streamingPlan.getStreamingPlan();
    OptimizerProgram optimizerProgram = buildFragmenterProgram();
    RelNode planWithExchange = optimizerProgram.optimize(root, context);
    StreamingPlan newPlan = new StreamingPlan((RwStreamMaterializedView) planWithExchange);
    return newPlan;
  }

  private static OptimizerProgram buildFragmenterProgram() {
    var builder = HepOptimizerProgram.builder();
    return builder.addRules(STREAMING_PARALLEL_RULES).build();
  }
}
