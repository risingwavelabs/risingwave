package com.risingwave.planner.program;

import com.google.common.collect.ImmutableList;
import com.risingwave.planner.context.ExecutionContext;
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

public class VolcanoOptimizerProgram implements OptimizerProgram {
  private final ImmutableList<RelOptRule> rules;
  private final RelTrait[] requitedOutputTraits;

  private VolcanoOptimizerProgram(
      ImmutableList<RelOptRule> rules, ImmutableList<RelTrait> requitedOutputTraits) {
    this.rules = rules;
    this.requitedOutputTraits = requitedOutputTraits.toArray(new RelTrait[0]);
  }

  @Override
  public RelNode optimize(RelNode root, ExecutionContext context) {
    RelTraitSet targetTraits = root.getTraitSet().plusAll(requitedOutputTraits).simplify();

    // Ugly part of calcite...
    VolcanoPlanner planner = (VolcanoPlanner) root.getCluster().getPlanner();

    Program optProgram = Programs.ofRules(rules);

    return optProgram.run(planner, root, targetTraits, ImmutableList.of(), ImmutableList.of());
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private final List<RelOptRule> rules = new ArrayList<>();
    private final List<RelTrait> requiredOutputTraits = new ArrayList<>();

    public Builder addRules(RuleSet ruleSet) {
      ruleSet.forEach(rules::add);
      return this;
    }

    public Builder addRequiredOutputTraits(RelTrait... traits) {
      this.requiredOutputTraits.addAll(Arrays.asList(traits));
      return this;
    }

    public VolcanoOptimizerProgram build() {
      return new VolcanoOptimizerProgram(
          ImmutableList.copyOf(rules), ImmutableList.copyOf(requiredOutputTraits));
    }
  }
}
