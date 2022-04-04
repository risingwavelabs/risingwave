package com.risingwave.planner.rel.physical.join;

import com.risingwave.common.exception.PgErrorCode;
import com.risingwave.common.exception.PgException;
import com.risingwave.proto.plan.JoinType;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexNode;

/** Utilities for batch join planning. */
public class BatchJoinUtils {
  private BatchJoinUtils() {}

  public static boolean isEquiJoin(JoinInfo joinInfo) {
    return hasEquiCondition(joinInfo) && joinInfo.isEqui();
  }

  public static boolean hasEquiCondition(JoinInfo joinInfo) {
    return !joinInfo.leftKeys.isEmpty() && !joinInfo.rightKeys.isEmpty();
  }

  public static RexNode getNonequiCondition(RelNode left, RelNode right, RexNode condition) {
    final List<Integer> leftKeys = new ArrayList<>();
    final List<Integer> rightKeys = new ArrayList<>();
    final List<Boolean> filterNulls = new ArrayList<>();
    return RelOptUtil.splitJoinCondition(left, right, condition, leftKeys, rightKeys, filterNulls);
  }

  /**
   * Map from calcite join type to proto join type.
   *
   * @param joinType Calcite join type.
   * @return Protobuf join type.
   */
  public static JoinType getJoinTypeProto(JoinRelType joinType) {
    switch (joinType) {
      case INNER:
        return JoinType.INNER;
      case LEFT:
        return JoinType.LEFT_OUTER;
      case RIGHT:
        return JoinType.RIGHT_OUTER;
      case FULL:
        return JoinType.FULL_OUTER;
      case SEMI:
        return JoinType.LEFT_SEMI;
      case ANTI:
        return JoinType.LEFT_ANTI;
      default:
        throw new PgException(
            PgErrorCode.INTERNAL_ERROR, "unsupported join type: %s for nested loop join", joinType);
    }
  }
}
