package com.risingwave.planner.program;

import static com.risingwave.planner.rules.BatchRuleSets.JOIN_REORDER_PREPARE_RULES;

import com.risingwave.execution.context.ExecutionContext;
import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rules.CoreRules;

public class JoinReorderProgram implements OptimizerProgram {
  public static final JoinReorderProgram INSTANCE = new JoinReorderProgram();

  private static final HepProgram PREPARE_PROGRAM = createPrepareProgram();
  private static final HepProgram REORDER_PROGRAM = createReorderProgram();

  @Override
  public RelNode optimize(RelNode root, ExecutionContext context) {
    var preparedNode = runProgram(PREPARE_PROGRAM, root, context);

    return runProgram(REORDER_PROGRAM, preparedNode, context);
  }

  private static RelNode runProgram(HepProgram program, RelNode root, ExecutionContext context) {
    var planner = new HepPlanner(program, context);
    planner.setRoot(root);

    return planner.findBestExp();
  }

  private static HepProgram createPrepareProgram() {
    var builder = HepProgram.builder().addMatchOrder(HepMatchOrder.BOTTOM_UP);

    builder.addRuleCollection(JOIN_REORDER_PREPARE_RULES);
    return builder.build();
  }

  private static HepProgram createReorderProgram() {
    var builder = HepProgram.builder().addMatchOrder(HepMatchOrder.BOTTOM_UP);

    builder.addRuleInstance(CoreRules.MULTI_JOIN_OPTIMIZE_BUSHY);
    return builder.build();
  }
}
