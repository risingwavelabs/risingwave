package com.risingwave.planner.program;

import com.google.common.collect.ImmutableList;
import com.risingwave.execution.context.ExecutionContext;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RuleSet;

/** Calcite's Volcano CBO Optimizer */
public class VolcanoOptimizerProgram implements OptimizerProgram {
  private final ImmutableList<RelOptRule> rules;
  private final RelTrait[] requitedOutputTraits;
  private boolean topDownOpt;

  private VolcanoOptimizerProgram(
      ImmutableList<RelOptRule> rules,
      ImmutableList<RelTrait> requitedOutputTraits,
      boolean topDownOpt) {
    this.rules = rules;
    this.requitedOutputTraits = requitedOutputTraits.toArray(new RelTrait[0]);
    this.topDownOpt = topDownOpt;
  }

  @Override
  public RelNode optimize(RelNode root, ExecutionContext context) {
    RelTraitSet targetTraits = root.getTraitSet().plusAll(requitedOutputTraits).simplify();

    // Ugly part of calcite...
    VolcanoPlanner planner = (VolcanoPlanner) root.getCluster().getPlanner();
    planner.setTopDownOpt(topDownOpt);

    Program optProgram = Programs.ofRules(rules);

    return optProgram.run(planner, root, targetTraits, ImmutableList.of(), ImmutableList.of());
  }

  public static Builder builder() {
    return new Builder();
  }

  /** Builder of the VolcanoOptimizerProgram */
  public static class Builder {
    private final List<RelOptRule> rules = new ArrayList<>();
    private final List<RelTrait> requiredOutputTraits = new ArrayList<>();
    private boolean topDownOpt = false;

    public Builder addRules(RuleSet ruleSet) {
      ruleSet.forEach(rules::add);
      return this;
    }

    public Builder addRequiredOutputTraits(RelTrait... traits) {
      this.requiredOutputTraits.addAll(Arrays.asList(traits));
      return this;
    }

    public Builder setTopDownOpt(boolean value) {
      topDownOpt = value;
      return this;
    }

    public VolcanoOptimizerProgram build() {
      return new VolcanoOptimizerProgram(
          ImmutableList.copyOf(rules), ImmutableList.copyOf(requiredOutputTraits), topDownOpt);
    }
  }
}
