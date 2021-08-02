package com.risingwave.planner.rel.physical.batch;

import static com.google.common.base.Preconditions.checkArgument;
import static com.risingwave.planner.rel.logical.RisingWaveLogicalRel.LOGICAL;

import com.risingwave.planner.rel.logical.RwProject;
import com.risingwave.proto.plan.PlanNode;
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

public class BatchProject extends Project implements RisingWaveBatchPhyRel {
  protected BatchProject(
      RelOptCluster cluster,
      RelTraitSet traits,
      List<RelHint> hints,
      RelNode input,
      List<? extends RexNode> projects,
      RelDataType rowType) {
    super(cluster, traits, hints, input, projects, rowType);
    checkArgument(traitSet.contains(RisingWaveBatchPhyRel.BATCH_PHYSICAL));
  }

  @Override
  public Project copy(
      RelTraitSet traitSet, RelNode input, List<RexNode> projects, RelDataType rowType) {
    return new BatchProject(getCluster(), traitSet, getHints(), input, projects, rowType);
  }

  @Override
  public PlanNode serialize() {
    throw new UnsupportedOperationException("");
  }

  public static class BatchProjectConverterRule extends ConverterRule {
    public static final BatchProjectConverterRule INSTANCE =
        Config.INSTANCE
            .withInTrait(LOGICAL)
            .withOutTrait(BATCH_PHYSICAL)
            .withRuleFactory(BatchProjectConverterRule::new)
            .withOperandSupplier(t -> t.operand(RwProject.class).anyInputs())
            .withDescription("Converting logical project to batch physical.")
            .as(Config.class)
            .toRule(BatchProjectConverterRule.class);

    protected BatchProjectConverterRule(Config config) {
      super(config);
    }

    @Override
    public @Nullable RelNode convert(RelNode rel) {
      RwProject rwProject = (RwProject) rel;
      RelTraitSet newTraitSet = rwProject.getTraitSet().replace(BATCH_PHYSICAL);
      RelNode newInput = RelOptRule.convert(rwProject.getInput(), BATCH_PHYSICAL);
      return new BatchProject(
          rel.getCluster(),
          newTraitSet,
          rwProject.getHints(),
          newInput,
          rwProject.getProjects(),
          rwProject.getRowType());
    }
  }
}
