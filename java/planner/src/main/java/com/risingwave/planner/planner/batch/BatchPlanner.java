package com.risingwave.planner.planner.batch;

import static com.risingwave.planner.planner.PlannerUtils.isSingleMode;
import static com.risingwave.planner.program.ChainedOptimizerProgram.OptimizerPhase.BASIC_LOGICAL_OPTIMIZATION;
import static com.risingwave.planner.program.ChainedOptimizerProgram.OptimizerPhase.JOIN_REORDER;
import static com.risingwave.planner.program.ChainedOptimizerProgram.OptimizerPhase.PHYSICAL;
import static com.risingwave.planner.program.ChainedOptimizerProgram.OptimizerPhase.SUBQUERY_REWRITE;
import static com.risingwave.planner.rel.logical.RisingWaveLogicalRel.LOGICAL;
import static com.risingwave.planner.rules.BatchRuleSets.LOGICAL_CONVERTER_RULES;
import static com.risingwave.planner.rules.BatchRuleSets.LOGICAL_OPTIMIZATION_RULES;
import static com.risingwave.planner.rules.BatchRuleSets.PHYSICAL_AGG_RULES;
import static com.risingwave.planner.rules.BatchRuleSets.PHYSICAL_CONVERTER_RULES;
import static com.risingwave.planner.rules.BatchRuleSets.PHYSICAL_JOIN_RULES;

import com.risingwave.execution.context.ExecutionContext;
import com.risingwave.planner.planner.Planner;
import com.risingwave.planner.program.ChainedOptimizerProgram;
import com.risingwave.planner.program.JoinReorderProgram;
import com.risingwave.planner.program.OptimizerProgram;
import com.risingwave.planner.program.SubQueryRewriteProgram;
import com.risingwave.planner.program.VolcanoOptimizerProgram;
import com.risingwave.planner.rel.logical.RwLogicalGather;
import com.risingwave.planner.rel.physical.batch.BatchPlan;
import com.risingwave.planner.rel.physical.batch.RisingWaveBatchPhyRel;
import com.risingwave.planner.rel.serialization.ExplainWriter;
import com.risingwave.planner.rules.BatchRuleSets;
import com.risingwave.planner.sql.SqlConverter;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BatchPlanner implements Planner<BatchPlan> {
  private static final Logger log = LoggerFactory.getLogger(BatchPlanner.class);

  public BatchPlanner() {}

  private static RelNode toRel(
      SqlConverter sqlConverter, SqlNode sqlNode, ExecutionContext context) {
    RelNode root = sqlConverter.toRel(sqlNode).rel;
    if (!isSingleMode(context)) {
      root = RwLogicalGather.create(root);
    }

    return root;
  }

  @Override
  public BatchPlan plan(SqlNode ast, ExecutionContext context) {
    SqlConverter sqlConverter = SqlConverter.builder(context).build();
    RelNode rawPlan = toRel(sqlConverter, ast, context);
    OptimizerProgram optimizerProgram = buildOptimizerProgram();

    RelNode result = optimizerProgram.optimize(rawPlan, context);
    RisingWaveBatchPhyRel root = (RisingWaveBatchPhyRel) result;
    log.info("Create physical plan: {}", ExplainWriter.explainPlan(root));

    return new BatchPlan(root);
  }

  private static OptimizerProgram buildOptimizerProgram() {
    ChainedOptimizerProgram.Builder builder = ChainedOptimizerProgram.builder();

    builder.addLast(SUBQUERY_REWRITE, SubQueryRewriteProgram.INSTANCE);

    builder.addLast(JOIN_REORDER, JoinReorderProgram.INSTANCE);

    builder.addLast(
        BASIC_LOGICAL_OPTIMIZATION,
        VolcanoOptimizerProgram.builder()
            .addRules(BatchRuleSets.BASIC_LOGICAL_OPTIMIZE_RULES)
            .addRules(LOGICAL_CONVERTER_RULES)
            .addRules(LOGICAL_OPTIMIZATION_RULES)
            .addRequiredOutputTraits(LOGICAL)
            .build());

    builder.addLast(
        PHYSICAL,
        VolcanoOptimizerProgram.builder()
            .addRules(PHYSICAL_CONVERTER_RULES)
            .addRules(PHYSICAL_AGG_RULES)
            .addRules(PHYSICAL_JOIN_RULES)
            .addRequiredOutputTraits(RisingWaveBatchPhyRel.BATCH_PHYSICAL)
            .build());

    return builder.build();
  }
}
