package com.risingwave.planner.rel.physical;

import static com.risingwave.planner.rel.logical.RisingWaveLogicalRel.LOGICAL;

import com.google.common.collect.ImmutableList;
import com.risingwave.planner.rel.common.dist.RwDistributionTrait;
import com.risingwave.planner.rel.common.dist.RwDistributionTraitDef;
import com.risingwave.planner.rel.common.dist.RwDistributions;
import com.risingwave.planner.rel.logical.RwLogicalProject;
import com.risingwave.planner.rel.serialization.RexToProtoSerializer;
import com.risingwave.proto.plan.PlanNode;
import com.risingwave.proto.plan.ProjectNode;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.PhysicalNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Pair;
import org.checkerframework.checker.nullness.qual.Nullable;

/** physical project operator */
public class RwBatchProject extends Project implements RisingWaveBatchPhyRel, PhysicalNode {
  public RwBatchProject(
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
  public Project copy(
      RelTraitSet traitSet, RelNode input, List<RexNode> projects, RelDataType rowType) {
    return new RwBatchProject(getCluster(), traitSet, getHints(), input, projects, rowType);
  }

  @Override
  public PlanNode serialize() {
    RexToProtoSerializer rexVisitor = new RexToProtoSerializer();
    ProjectNode.Builder projectNodeBuilder = ProjectNode.newBuilder();
    for (int i = 0; i < exps.size(); i++) {
      projectNodeBuilder.addSelectList(exps.get(i).accept(rexVisitor));
    }
    return PlanNode.newBuilder()
        .setProject(projectNodeBuilder.build())
        .addChildren(((RisingWaveBatchPhyRel) input).serialize())
        .build();
  }

  @Override
  public RelNode convertToDistributed() {
    return copy(
        getTraitSet().replace(BATCH_DISTRIBUTED),
        RelOptRule.convert(input, input.getTraitSet().replace(BATCH_DISTRIBUTED)),
        getProjects(),
        getRowType());
  }

  @Override
  public @Nullable Pair<RelTraitSet, List<RelTraitSet>> passThroughTraits(RelTraitSet required) {
    return null;
  }

  @Override
  public Pair<RelTraitSet, List<RelTraitSet>> deriveTraits(
      final RelTraitSet childTraits, final int childId) {
    if (childTraits.getConvention() != traitSet.getConvention()) {
      return null;
    }
    if (childTraits.getConvention() != BATCH_DISTRIBUTED) {
      return null;
    }

    var newTraits = traitSet;
    var dist = childTraits.getTrait(RwDistributionTraitDef.getInstance());
    if (dist != null) {
      // We remark that the project may:
      // 1. move a column into a different position. We also use the new position as the key.
      // 2. remove a column. We lose a key. This has implications depending on the type of
      // distribution:
      //    a. hash(a,b,c) => hash(a,b) invalid.
      //    b. range(a,b,c) => range(a,b) still valid.
      //    c. range(a,b,c) => range(b,c) invalid.
      // 3. use the column in some function. This is treated as (2).
      var keys = dist.getKeys();
      List<Integer> newKeys = new ArrayList<>();
      for (var key : keys) {
        var projects = this.getProjects();
        for (int i = 0; i < projects.size(); i++) {
          var project = projects.get(i);
          if (project instanceof RexInputRef) {
            var index = ((RexInputRef) project).getIndex();
            if (key == index) {
              newKeys.add(i);
              break;
            }
          }
        }
      }
      if (newKeys.size() != 0) {
        var newDistTrait =
            new RwDistributionTrait(dist.getType(), ImmutableIntList.copyOf(newKeys));
        // Therefore, we need to check whether the old distribution can satisfy the new one.
        // If yes, we plus it.
        if (dist.satisfies(newDistTrait)) {
          newTraits = newTraits.plus(newDistTrait);
        } else {
          newTraits = newTraits.plus(RwDistributions.RANDOM_DISTRIBUTED);
        }
      } else {
        if (dist == RwDistributions.SINGLETON) {
          newTraits = newTraits.plus(RwDistributions.SINGLETON);
        } else {
          newTraits = newTraits.plus(RwDistributions.RANDOM_DISTRIBUTED);
        }
      }
    }
    // Notice the default should be random_distributed instead of any. Any is a placeholder.
    return Pair.of(newTraits, ImmutableList.of(childTraits));
  }

  /** Project converter rule between logical and physical. */
  public static class BatchProjectConverterRule extends ConverterRule {
    public static final BatchProjectConverterRule INSTANCE =
        Config.INSTANCE
            .withInTrait(LOGICAL)
            .withOutTrait(BATCH_PHYSICAL)
            .withRuleFactory(BatchProjectConverterRule::new)
            .withOperandSupplier(t -> t.operand(RwLogicalProject.class).anyInputs())
            .withDescription("Converting logical project to batch physical.")
            .as(Config.class)
            .toRule(BatchProjectConverterRule.class);

    protected BatchProjectConverterRule(Config config) {
      super(config);
    }

    @Override
    public @Nullable RelNode convert(RelNode rel) {
      RwLogicalProject rwLogicalProject = (RwLogicalProject) rel;
      RelTraitSet requiredInputTraits =
          rwLogicalProject.getInput().getTraitSet().replace(BATCH_PHYSICAL);
      RelNode newInput = RelOptRule.convert(rwLogicalProject.getInput(), requiredInputTraits);
      return new RwBatchProject(
          rel.getCluster(),
          rwLogicalProject.getTraitSet().plus(BATCH_PHYSICAL),
          rwLogicalProject.getHints(),
          newInput,
          rwLogicalProject.getProjects(),
          rwLogicalProject.getRowType());
    }
  }
}
