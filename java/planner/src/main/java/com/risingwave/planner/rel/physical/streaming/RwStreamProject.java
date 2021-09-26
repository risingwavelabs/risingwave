package com.risingwave.planner.rel.physical.streaming;

import static com.risingwave.planner.rel.logical.RisingWaveLogicalRel.LOGICAL;

import com.risingwave.planner.rel.logical.RwLogicalProject;
import com.risingwave.proto.streaming.plan.StreamNode;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.checkerframework.checker.nullness.qual.Nullable;

public class RwStreamProject extends Project implements RisingWaveStreamingRel {
  public RwStreamProject(
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
  public StreamNode serialize() {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public Project copy(
      RelTraitSet traitSet, RelNode input, List<RexNode> projects, RelDataType rowType) {
    return new RwStreamProject(getCluster(), traitSet, getHints(), input, projects, rowType);
  }

  public static class StreamProjectConverterRule extends ConverterRule {
    public static final RwStreamProject.StreamProjectConverterRule INSTANCE =
        ConverterRule.Config.INSTANCE
            .withInTrait(LOGICAL)
            .withOutTrait(STREAMING)
            .withRuleFactory(RwStreamProject.StreamProjectConverterRule::new)
            .withOperandSupplier(t -> t.operand(RwLogicalProject.class).anyInputs())
            .withDescription("Converting logical project to streaming project.")
            .as(Config.class)
            .toRule(RwStreamProject.StreamProjectConverterRule.class);

    protected StreamProjectConverterRule(Config config) {
      super(config);
    }

    @Override
    public @Nullable RelNode convert(RelNode rel) {
      RwLogicalProject rwLogicalProject = (RwLogicalProject) rel;
      RelTraitSet requiredInputTraits =
          rwLogicalProject.getInput().getTraitSet().replace(STREAMING);
      RelNode newInput = RelOptRule.convert(rwLogicalProject.getInput(), requiredInputTraits);
      return new RwStreamProject(
          rel.getCluster(),
          rwLogicalProject.getTraitSet().plus(STREAMING),
          rwLogicalProject.getHints(),
          newInput,
          rwLogicalProject.getProjects(),
          rwLogicalProject.getRowType());
    }
  }
}
