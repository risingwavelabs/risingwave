package com.risingwave.planner.rel.logical;

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Customized LogicalProject * */
public class RwLogicalProject extends Project implements RisingWaveLogicalRel {
  public RwLogicalProject(
      RelOptCluster cluster,
      RelTraitSet traits,
      List<RelHint> hints,
      RelNode input,
      List<? extends RexNode> projects,
      RelDataType rowType) {
    super(cluster, traits, hints, input, projects, rowType);
    checkConvention();
  }

  @Override
  public RwLogicalProject copy(
      RelTraitSet traitSet, RelNode input, List<RexNode> projects, RelDataType rowType) {
    return new RwLogicalProject(getCluster(), traitSet, getHints(), input, projects, rowType);
  }

  /** ConverterRule from LogicalProject to RwLogicalProject. * */
  public static class RwProjectConverterRule extends ConverterRule {
    public static final RwLogicalProject.RwProjectConverterRule INSTANCE =
        Config.INSTANCE
            .withInTrait(Convention.NONE)
            .withOutTrait(LOGICAL)
            .withRuleFactory(RwProjectConverterRule::new)
            .withOperandSupplier(t -> t.operand(LogicalProject.class).anyInputs())
            .withDescription("RisingWaveLogicalProject")
            .as(Config.class)
            .toRule(RwProjectConverterRule.class);

    protected RwProjectConverterRule(Config config) {
      super(config);
    }

    @Override
    public @Nullable RelNode convert(RelNode rel) {
      LogicalProject logicalProject = (LogicalProject) rel;
      var input = logicalProject.getInput();
      var newInput = RelOptRule.convert(input, input.getTraitSet().plus(LOGICAL));
      var newProjects = new ArrayList<RexNode>();
      for (var project : logicalProject.getProjects()) {
        // Hack here to remove Sarg optimization of Calcite.
        var newProject = RexUtil.expandSearch(rel.getCluster().getRexBuilder(), null, project);
        newProjects.add(newProject);
      }
      return new RwLogicalProject(
          rel.getCluster(),
          rel.getTraitSet().plus(LOGICAL),
          logicalProject.getHints(),
          newInput,
          newProjects,
          logicalProject.getRowType());
    }
  }
}
