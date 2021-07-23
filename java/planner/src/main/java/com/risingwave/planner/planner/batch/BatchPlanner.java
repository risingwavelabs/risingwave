package com.risingwave.planner.planner.batch;

import static com.risingwave.planner.program.ChainedOptimizerProgram.OptimizerPhase.CALCITE_LOGICAL_OPTIMIZATION;
import static com.risingwave.planner.program.ChainedOptimizerProgram.OptimizerPhase.LOGICAL_CONVERSION;
import static com.risingwave.planner.program.ChainedOptimizerProgram.OptimizerPhase.PHYSICAL;
import static com.risingwave.planner.rules.BatchRuleSets.PHYSICAL_RULES;

import com.risingwave.planner.context.ExecutionContext;
import com.risingwave.planner.planner.Planner;
import com.risingwave.planner.program.ChainedOptimizerProgram;
import com.risingwave.planner.program.HepOptimizerProgram;
import com.risingwave.planner.program.OptimizerProgram;
import com.risingwave.planner.program.VolcanoOptimizerProgram;
import com.risingwave.planner.rel.physical.batch.BatchPlan;
import com.risingwave.planner.rel.physical.batch.RisingWaveBatchPhyRel;
import com.risingwave.planner.rules.BatchRuleSets;
import com.risingwave.planner.sql.SqlConverter;
import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNode;

public class BatchPlanner implements Planner<BatchPlan> {
  public BatchPlanner() {}

  private static RelNode toRel(SqlConverter sqlConverter, SqlNode sqlNode) {
    return sqlConverter.toRel(sqlNode).rel;
  }

  @Override
  public BatchPlan plan(SqlNode ast, ExecutionContext context) {
    SqlConverter sqlConverter = SqlConverter.builder(context).build();
    RelNode rawPlan = toRel(sqlConverter, ast);
    OptimizerProgram optimizerProgram = buildOptimizerProgram();

    RelNode result = optimizerProgram.optimize(rawPlan, context);

    return new BatchPlan(result);
  }

  private static OptimizerProgram buildOptimizerProgram() {
    ChainedOptimizerProgram.Builder builder = ChainedOptimizerProgram.builder();

    builder.addLast(
        CALCITE_LOGICAL_OPTIMIZATION,
        HepOptimizerProgram.builder()
            .withMatchOrder(HepMatchOrder.BOTTOM_UP)
            .withMatchLimit(10)
            .addRules(BatchRuleSets.CALCITE_LOGICAL_OPTIMIZE_RULES)
            .build());

    builder.addLast(
        LOGICAL_CONVERSION,
        HepOptimizerProgram.builder()
            .withMatchOrder(HepMatchOrder.BOTTOM_UP)
            .withMatchLimit(10)
            .addRules(BatchRuleSets.LOGICAL_CONVERSION_RULES)
            .build());

    builder.addLast(
        PHYSICAL,
        VolcanoOptimizerProgram.builder()
            .addRules(PHYSICAL_RULES)
            .addRequiredOutputTraits(RisingWaveBatchPhyRel.BATCH_PHYSICAL)
            .build());

    return builder.build();
  }
}
