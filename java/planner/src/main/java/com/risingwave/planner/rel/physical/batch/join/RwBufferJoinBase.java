package com.risingwave.planner.rel.physical.batch.join;

import com.google.common.collect.ImmutableList;
import com.risingwave.planner.rel.common.dist.RwDistributionTraitDef;
import com.risingwave.planner.rel.physical.batch.RisingWaveBatchPhyRel;
import java.util.List;
import java.util.Set;
import org.apache.calcite.plan.DeriveMode;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.PhysicalNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.mapping.MappingType;
import org.apache.calcite.util.mapping.Mappings;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Base class for join with build and probe side. */
public abstract class RwBufferJoinBase extends Join implements RisingWaveBatchPhyRel, PhysicalNode {

  public RwBufferJoinBase(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      List<RelHint> hints,
      RelNode left,
      RelNode right,
      RexNode condition,
      Set<CorrelationId> variablesSet,
      JoinRelType joinType) {
    super(cluster, traitSet, hints, left, right, condition, variablesSet, joinType);
    checkConvention();
  }

  @Override
  public @Nullable Pair<RelTraitSet, List<RelTraitSet>> passThroughTraits(RelTraitSet required) {
    return null;
  }

  @Override
  public @Nullable Pair<RelTraitSet, List<RelTraitSet>> deriveTraits(
      RelTraitSet childTraits, int childId) {
    if (childTraits.getConvention() != traitSet.getConvention()) {
      return null;
    }
    if (childTraits.getConvention() != BATCH_DISTRIBUTED) {
      return null;
    }

    var newTraits = traitSet;
    var dist = childTraits.getTrait(RwDistributionTraitDef.getInstance());
    if (dist != null) {
      if (childId == 0) {
        newTraits = newTraits.plus(dist);
      } else {
        int leftFieldCount = left.getRowType().getFieldCount();
        int rightFiledCount = right.getRowType().getFieldCount();
        Mappings.TargetMapping mapping =
            Mappings.create(
                MappingType.SURJECTION, rightFiledCount, leftFieldCount + rightFiledCount);
        for (int i = 0; i < rightFiledCount; i++) {
          mapping.set(i, i + leftFieldCount);
        }
        newTraits = newTraits.plus(dist.apply(mapping));
      }
    }
    return Pair.of(newTraits, ImmutableList.of(left.getTraitSet(), right.getTraitSet()));
  }

  /** see {@link com.risingwave.planner.rules.distributed.join.BroadcastJoinRule} */
  @Override
  public DeriveMode getDeriveMode() {
    if (joinType.generatesNullsOnLeft()) {
      return DeriveMode.RIGHT_FIRST;
    } else {
      return DeriveMode.LEFT_FIRST;
    }
  }
}
