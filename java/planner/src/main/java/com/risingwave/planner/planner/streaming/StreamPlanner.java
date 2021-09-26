package com.risingwave.planner.planner.streaming;

import static com.risingwave.planner.program.ChainedOptimizerProgram.OptimizerPhase.JOIN_REORDER;
import static com.risingwave.planner.program.ChainedOptimizerProgram.OptimizerPhase.LOGICAL_CBO;
import static com.risingwave.planner.program.ChainedOptimizerProgram.OptimizerPhase.LOGICAL_REWRITE;
import static com.risingwave.planner.program.ChainedOptimizerProgram.OptimizerPhase.STREAMING;
import static com.risingwave.planner.program.ChainedOptimizerProgram.OptimizerPhase.SUBQUERY_REWRITE;
import static com.risingwave.planner.rel.logical.RisingWaveLogicalRel.LOGICAL;
import static com.risingwave.planner.rules.BatchRuleSets.LOGICAL_CONVERTER_RULES;
import static com.risingwave.planner.rules.BatchRuleSets.LOGICAL_OPTIMIZATION_RULES;
import static com.risingwave.planner.rules.BatchRuleSets.LOGICAL_REWRITE_RULES;

import com.risingwave.execution.context.ExecutionContext;
import com.risingwave.planner.planner.Planner;
import com.risingwave.planner.program.ChainedOptimizerProgram;
import com.risingwave.planner.program.HepOptimizerProgram;
import com.risingwave.planner.program.JoinReorderProgram;
import com.risingwave.planner.program.OptimizerProgram;
import com.risingwave.planner.program.SubQueryRewriteProgram;
import com.risingwave.planner.program.VolcanoOptimizerProgram;
import com.risingwave.planner.rel.physical.streaming.RisingWaveStreamingRel;
import com.risingwave.planner.rel.physical.streaming.StreamingPlan;
import com.risingwave.planner.rules.BatchRuleSets;
import com.risingwave.planner.rules.streaming.StreamingConvertRules;
import com.risingwave.planner.sql.SqlConverter;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamPlanner implements Planner<StreamingPlan> {
  private static final Logger log = LoggerFactory.getLogger(StreamPlanner.class);

  public StreamPlanner() {}

  @Override
  public StreamingPlan plan(SqlNode ast, ExecutionContext context) {
    SqlConverter sqlConverter = SqlConverter.builder(context).build();
    RelNode rawPlan = sqlConverter.toRel(ast).rel;
    OptimizerProgram optimizerProgram = buildOptimizerProgram();

    RelNode result = optimizerProgram.optimize(rawPlan, context);
    RisingWaveStreamingRel root = (RisingWaveStreamingRel) result;

    // TODO: implement explain writer for streaming plan.
    log.info("Create streaming plan:\n");

    return new StreamingPlan(root);
  }

  private static OptimizerProgram buildOptimizerProgram() {
    ChainedOptimizerProgram.Builder builder = ChainedOptimizerProgram.builder();
    // We use partial rules from batch planner until getting a RisingWave logical plan.
    builder.addLast(SUBQUERY_REWRITE, SubQueryRewriteProgram.INSTANCE);

    builder.addLast(
        LOGICAL_REWRITE, HepOptimizerProgram.builder().addRules(LOGICAL_REWRITE_RULES).build());

    builder.addLast(JOIN_REORDER, JoinReorderProgram.INSTANCE);

    builder.addLast(
        LOGICAL_CBO,
        VolcanoOptimizerProgram.builder()
            .addRules(BatchRuleSets.LOGICAL_OPTIMIZE_RULES)
            .addRules(LOGICAL_CONVERTER_RULES)
            .addRules(LOGICAL_OPTIMIZATION_RULES)
            .addRequiredOutputTraits(LOGICAL)
            .build());
    // Next we transform RisingWave logical plan to streaming plan.
    builder.addLast(
        STREAMING,
        VolcanoOptimizerProgram.builder()
            .addRules(StreamingConvertRules.STREAMING_CONVERTER_RULES)
            .build());

    return builder.build();
  }
}
