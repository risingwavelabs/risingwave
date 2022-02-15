package com.risingwave.planner.rel.physical;

import static com.risingwave.planner.rel.logical.RisingWaveLogicalRel.LOGICAL;

import com.risingwave.planner.rel.logical.RwLogicalLimit;
import com.risingwave.proto.plan.LimitNode;
import com.risingwave.proto.plan.PlanNode;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.PhysicalNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Pair;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Limit in Batch convention */
public class RwBatchLimit extends SingleRel implements RisingWaveBatchPhyRel, PhysicalNode {

  public final @Nullable RexNode offset;
  public final @Nullable RexNode fetch;

  public RwBatchLimit(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode input,
      @Nullable RexNode offset,
      @Nullable RexNode fetch) {
    super(cluster, traitSet, input);
    this.offset = offset;
    this.fetch = fetch;
  }

  public RexNode getOffset() {
    return offset;
  }

  public RexNode getFetch() {
    return fetch;
  }

  @Override
  public @Nullable Pair<RelTraitSet, List<RelTraitSet>> passThroughTraits(RelTraitSet required) {
    return null;
  }

  @Override
  public Pair<RelTraitSet, List<RelTraitSet>> deriveTraits(
      final RelTraitSet childTraits, final int childId) {
    return null;
  }

  @Override
  public PlanNode serialize() {
    LimitNode.Builder builder = LimitNode.newBuilder();
    if (this.fetch != null) {
      builder.setLimit(RexLiteral.intValue(this.fetch));
    }
    if (this.offset != null) {
      builder.setOffset(RexLiteral.intValue(this.offset));
    }
    LimitNode limitNode = builder.build();
    return PlanNode.newBuilder()
        .setLimit(limitNode)
        .addChildren(((RisingWaveBatchPhyRel) input).serialize())
        .setIdentity(BatchPlan.getCurrentNodeIdentity(this))
        .build();
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new RwBatchLimit(this.getCluster(), traitSet, sole(inputs), offset, fetch);
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw)
        .itemIf("offset", offset, offset != null)
        .itemIf("fetch", fetch, fetch != null);
  }

  /** Rule for converting Limit in logical convention to batch convention */
  public static class BatchLimitConverterRule extends ConverterRule {
    public static final RwBatchLimit.BatchLimitConverterRule INSTANCE =
        Config.INSTANCE
            .withInTrait(LOGICAL)
            .withOutTrait(BATCH_PHYSICAL)
            .withRuleFactory(RwBatchLimit.BatchLimitConverterRule::new)
            .withOperandSupplier(t -> t.operand(RwLogicalLimit.class).anyInputs())
            .withDescription("Converting logical limit to batch limit.")
            .as(Config.class)
            .toRule(RwBatchLimit.BatchLimitConverterRule.class);

    protected BatchLimitConverterRule(Config config) {
      super(config);
    }

    @Override
    public @Nullable RelNode convert(RelNode rel) {
      var logicalLimit = (RwLogicalLimit) rel;

      var requiredInputTraits = logicalLimit.getInput().getTraitSet().plus(BATCH_PHYSICAL);
      var newTraits = logicalLimit.getTraitSet().plus(BATCH_PHYSICAL);
      var newInput = RelOptRule.convert(logicalLimit.getInput(), requiredInputTraits);

      return new RwBatchLimit(
          rel.getCluster(), newTraits, newInput, logicalLimit.offset, logicalLimit.fetch);
    }
  }
}
