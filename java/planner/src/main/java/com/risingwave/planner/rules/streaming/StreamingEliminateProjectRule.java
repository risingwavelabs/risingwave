package com.risingwave.planner.rules.streaming;

import com.risingwave.planner.rel.physical.streaming.RwStreamProject;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;

/**
 * Rule for eliminating the top project when there are two consecutive projects in streaming
 * convention
 */
public class StreamingEliminateProjectRule extends RelRule<StreamingEliminateProjectRule.Config> {

  public StreamingEliminateProjectRule(Config config) {
    super(config);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    RwStreamProject topProjectOperator = call.rel(0);
    RwStreamProject downProjectOperator = call.rel(1);

    // we first get all the projects/expressions in downProject,
    var downProjects = downProjectOperator.getProjects();

    // we then replace all the input ref in topProjects with downProject's expressions
    RexShuttle inputRefReplaceShuttle =
        new RexShuttle() {
          @Override
          public RexNode visitInputRef(RexInputRef inputRef) {
            var idx = inputRef.getIndex();
            return downProjects.get(idx);
          }
        };

    List<RexNode> newTopProjects =
        topProjectOperator.getProjects().stream()
            .map(inputRefReplaceShuttle::apply)
            .collect(Collectors.toList());

    RwStreamProject newTopProjectOperator =
        new RwStreamProject(
            topProjectOperator.getCluster(),
            topProjectOperator.getTraitSet(),
            topProjectOperator.getHints(),
            downProjectOperator.getInput(),
            newTopProjects,
            topProjectOperator.getRowType());

    call.transformTo(newTopProjectOperator);
  }

  /** Default config */
  public interface Config extends RelRule.Config {
    StreamingEliminateProjectRule.Config DEFAULT =
        RelRule.Config.EMPTY
            .withDescription("Eliminate the lower project of two consecutive projects")
            .withOperandSupplier(
                s ->
                    s.operand(RwStreamProject.class)
                        .oneInput(b -> b.operand(RwStreamProject.class).anyInputs()))
            .as(StreamingEliminateProjectRule.Config.class);

    @Override
    default RelOptRule toRule() {
      return new StreamingEliminateProjectRule(this);
    }
  }
}
