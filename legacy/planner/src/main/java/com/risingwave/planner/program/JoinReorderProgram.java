package com.risingwave.planner.program;

import static org.apache.calcite.rel.rules.CoreRules.MULTI_JOIN_OPTIMIZE;

import com.risingwave.execution.context.ExecutionContext;
import com.risingwave.planner.rel.serialization.ExplainWriter;
import java.util.List;
import org.apache.calcite.plan.RelOptCostImpl;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rules.CoreRules;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JoinReorderProgram implements OptimizerProgram {
  private static final Logger LOG = LoggerFactory.getLogger(JoinReorderProgram.class);
  public static final JoinReorderProgram INSTANCE = new JoinReorderProgram();

  private static final List<RelOptRule> JOIN_REORDER_PREPARE_RULES =
      List.of(
          CoreRules.JOIN_TO_MULTI_JOIN,
          CoreRules.PROJECT_MULTI_JOIN_MERGE,
          CoreRules.FILTER_MULTI_JOIN_MERGE);

  private static final HepProgram PREPARE_PROGRAM = createPrepareProgram();
  private static final HepProgram REORDER_PROGRAM = createReorderProgram();

  @Override
  public RelNode optimize(RelNode root, ExecutionContext context) {
    var preparedNode = runProgram(PREPARE_PROGRAM, root, context);

    LOG.debug("Plan after preparing join reorder: \n{}", ExplainWriter.explainPlan(preparedNode));
    var joinReorderedPlan = runProgram(REORDER_PROGRAM, preparedNode, context);
    LOG.debug(
        "Plan after running join reorder program: \n{}",
        ExplainWriter.explainPlan(joinReorderedPlan));
    return joinReorderedPlan;
  }

  private static RelNode runProgram(HepProgram program, RelNode root, ExecutionContext context) {
    var planner = new HepPlanner(program, context, true, null, RelOptCostImpl.FACTORY);
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

    builder.addRuleInstance(MULTI_JOIN_OPTIMIZE);
    return builder.build();
  }
}
