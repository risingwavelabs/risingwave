package com.risingwave.planner.rel.physical.join;

import static com.google.common.base.Verify.verify;
import static com.risingwave.common.config.BatchPlannerConfigurations.ENABLE_SORT_MERGE_JOIN;
import static com.risingwave.execution.context.ExecutionContext.contextOf;
import static com.risingwave.planner.rel.logical.RisingWaveLogicalRel.LOGICAL;
import static com.risingwave.planner.rel.physical.join.BatchJoinUtils.isEquiJoin;

import com.risingwave.planner.rel.logical.RwLogicalJoin;
import com.risingwave.planner.rel.physical.BatchPlan;
import com.risingwave.planner.rel.physical.RisingWaveBatchPhyRel;
import com.risingwave.proto.plan.OrderType;
import com.risingwave.proto.plan.PlanNode;
import com.risingwave.proto.plan.SortMergeJoinNode;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rex.RexNode;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Batch sort merge join plan node. */
public class RwBatchSortMergeJoin extends RwBufferJoinBase implements RisingWaveBatchPhyRel {
  private RelFieldCollation.Direction direction;

  public RwBatchSortMergeJoin(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      List<RelHint> hints,
      RelNode left,
      RelNode right,
      RexNode condition,
      JoinRelType joinType) {
    super(cluster, traitSet, hints, left, right, condition, Collections.emptySet(), joinType);
    checkConvention();
    verify(isEquiJoin(analyzeCondition()), "Sort merge join only support equi join!");
  }

  private OrderType getOrderType() {
    var fieldCollations = getTraitSet().getCollation().getFieldCollations();
    var direct = fieldCollations.get(0).getDirection();
    if (!fieldCollations.stream().allMatch(coll -> coll.getDirection() == direct)) {
      throw new UnsupportedOperationException("Sort Merge Join dont support different direction");
    }
    switch (direct) {
      case ASCENDING:
        return OrderType.ASCENDING;
      case DESCENDING:
        return OrderType.DESCENDING;
      default:
        throw new UnsupportedOperationException(String.format("Unsuported direction: %s", direct));
    }
  }

  @Override
  public PlanNode serialize() {
    var builder = SortMergeJoinNode.newBuilder();

    builder.setJoinType(BatchJoinUtils.getJoinTypeProto(getJoinType()));
    builder.setDirection(getOrderType());

    var joinInfo = analyzeCondition();
    joinInfo.leftKeys.forEach(builder::addLeftKeys);

    joinInfo.rightKeys.forEach(builder::addRightKeys);

    var node = builder.build();

    // TODO: Push project into join

    var leftChild = ((RisingWaveBatchPhyRel) left).serialize();
    var rightChild = ((RisingWaveBatchPhyRel) right).serialize();

    return PlanNode.newBuilder()
        .addChildren(leftChild)
        .addChildren(rightChild)
        .setSortMergeJoin(node)
        .setIdentity(BatchPlan.getCurrentNodeIdentity(this))
        .build();
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw);
  }

  @Override
  public Join copy(
      RelTraitSet traitSet,
      RexNode conditionExpr,
      RelNode left,
      RelNode right,
      JoinRelType joinType,
      boolean semiJoinDone) {
    return new RwBatchSortMergeJoin(
        getCluster(), traitSet, getHints(), left, right, conditionExpr, joinType);
  }

  /** Sort merge join converter rule between logical and physical. */
  public static class BatchSortMergeJoinConverterRule extends ConverterRule {
    public static final RwBatchSortMergeJoin.BatchSortMergeJoinConverterRule ASC =
        Config.INSTANCE
            .withInTrait(LOGICAL)
            .withOutTrait(BATCH_PHYSICAL)
            .withRuleFactory(
                RwBatchSortMergeJoin.BatchSortMergeJoinConverterRule
                    ::createAscSortMergeJoinConverterRule)
            .withOperandSupplier(t -> t.operand(RwLogicalJoin.class).anyInputs())
            .withDescription("Converting logical join to batch sort merge join(asc).")
            .as(Config.class)
            .toRule(RwBatchSortMergeJoin.BatchSortMergeJoinConverterRule.class);

    public static final RwBatchSortMergeJoin.BatchSortMergeJoinConverterRule DESC =
        Config.INSTANCE
            .withInTrait(LOGICAL)
            .withOutTrait(BATCH_PHYSICAL)
            .withRuleFactory(
                RwBatchSortMergeJoin.BatchSortMergeJoinConverterRule
                    ::createDescSortMergeJoinConverterRule)
            .withOperandSupplier(t -> t.operand(RwLogicalJoin.class).anyInputs())
            .withDescription("Converting logical join to batch sort merge join(desc).")
            .as(Config.class)
            .toRule(RwBatchSortMergeJoin.BatchSortMergeJoinConverterRule.class);

    static BatchSortMergeJoinConverterRule createAscSortMergeJoinConverterRule(Config config) {
      return new BatchSortMergeJoinConverterRule(config, RelFieldCollation.Direction.ASCENDING);
    }

    static BatchSortMergeJoinConverterRule createDescSortMergeJoinConverterRule(Config config) {
      return new BatchSortMergeJoinConverterRule(config, RelFieldCollation.Direction.DESCENDING);
    }

    RelFieldCollation.Direction direction;

    protected BatchSortMergeJoinConverterRule(
        Config config, RelFieldCollation.Direction direction) {
      super(config);
      this.direction = direction;
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
      var join = (RwLogicalJoin) call.rel(0);
      var joinInfo = join.analyzeCondition();
      return isEquiJoin(joinInfo)
          && contextOf(call).getSessionConfiguration().get(ENABLE_SORT_MERGE_JOIN);
    }

    @Override
    public @Nullable RelNode convert(RelNode rel) {
      var join = (RwLogicalJoin) rel;
      var joinInfo = join.analyzeCondition();
      // Add collation to both left input and right input.
      var left = join.getLeft();
      var leftTraits = left.getTraitSet().plus(BATCH_PHYSICAL);
      var leftCollations = new ArrayList<RelFieldCollation>();
      for (var key : joinInfo.leftKeys) {
        // Hard code order direction of sorted column.
        var collation = new RelFieldCollation(key, direction);
        leftCollations.add(collation);
      }
      var leftCollation = RelCollations.of(leftCollations);
      leftTraits = leftTraits.plus(leftCollation);

      leftTraits = leftTraits.simplify();
      var newLeft = convert(left, leftTraits);

      var right = join.getRight();
      var rightTraits = right.getTraitSet().plus(BATCH_PHYSICAL);
      var rightCollations = new ArrayList<RelFieldCollation>();
      for (var key : joinInfo.rightKeys) {
        var collation = new RelFieldCollation(key, direction);
        rightCollations.add(collation);
      }
      var rightCollation = RelCollations.of(rightCollations);
      rightTraits = rightTraits.plus(rightCollation);
      rightTraits = rightTraits.simplify();
      var newRight = convert(right, rightTraits);

      var newJoinTraits = newLeft.getTraitSet();

      return new RwBatchSortMergeJoin(
          join.getCluster(),
          newJoinTraits,
          join.getHints(),
          newLeft,
          newRight,
          join.getCondition(),
          join.getJoinType());
    }
  }
}
