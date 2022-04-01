package com.risingwave.planner.rules.distributed.agg;

import static com.risingwave.planner.rel.physical.RisingWaveBatchPhyRel.BATCH_DISTRIBUTED;
import static com.risingwave.planner.rel.physical.RisingWaveBatchPhyRel.BATCH_PHYSICAL;

import com.risingwave.planner.rel.common.dist.RwDistributions;
import com.risingwave.planner.rel.physical.RwBatchLimit;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Rule converting a RwBatchLimit to 2-phase limit */
public class TwoPhaseLimitRule extends ConverterRule {

  public static final TwoPhaseLimitRule INSTANCE =
      ConverterRule.Config.INSTANCE
          .withInTrait(BATCH_PHYSICAL)
          .withOutTrait(BATCH_DISTRIBUTED)
          .withRuleFactory(TwoPhaseLimitRule::new)
          .withOperandSupplier(t -> t.operand(RwBatchLimit.class).anyInputs())
          .withDescription("split limit to 2-phase limit.")
          .as(ConverterRule.Config.class)
          .toRule(TwoPhaseLimitRule.class);

  protected TwoPhaseLimitRule(Config config) {
    super(config);
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    RelNode rel = call.rel(0);
    RwBatchLimit limit = (RwBatchLimit) rel;
    RexNode fetch = limit.getFetch();
    return fetch != null;
  }

  @Override
  public @Nullable RelNode convert(RelNode rel) {
    RexBuilder rexBuilder = rel.getCluster().getRexBuilder();
    RwBatchLimit limit = (RwBatchLimit) rel;
    RexNode globalOffset = limit.getOffset();
    RexNode globalFetch = limit.getFetch();
    assert (globalFetch != null);
    int offsetValue = globalOffset == null ? 0 : RexLiteral.intValue(globalOffset);
    int fetchValue = RexLiteral.intValue(globalFetch);

    var requiredInputTraits = limit.getInput().getTraitSet().plus(BATCH_DISTRIBUTED);
    var newInput = RelOptRule.convert(limit.getInput(), requiredInputTraits);

    // For the local limit, we need to keep `offset + limit` number of rows
    var typeFactory = rexBuilder.getTypeFactory();
    RexNode zeroOffset = rexBuilder.makeZeroLiteral(typeFactory.createSqlType(SqlTypeName.INTEGER));
    RexNode localFetch = rexBuilder.makeLiteral(fetchValue + offsetValue, globalFetch.getType());
    var localLimit =
        new RwBatchLimit(
            limit.getCluster(),
            limit.getTraitSet().plus(BATCH_DISTRIBUTED),
            newInput,
            zeroOffset,
            localFetch);

    var requiredGlobalInputTraits =
        limit.getTraitSet().plus(BATCH_DISTRIBUTED).plus(RwDistributions.SINGLETON);

    var globalInput = RelOptRule.convert(localLimit, requiredGlobalInputTraits);

    // For the global limit, we use the same `offset` and `limit` as the original one to keep the
    // semantics unchanged.
    var globalLimit =
        new RwBatchLimit(
            limit.getCluster(), requiredGlobalInputTraits, globalInput, globalOffset, globalFetch);
    return globalLimit;
  }
}
