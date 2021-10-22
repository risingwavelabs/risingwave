package com.risingwave.planner.rules.physical.batch.join;

import com.risingwave.common.exception.PgErrorCode;
import com.risingwave.common.exception.PgException;
import com.risingwave.proto.plan.JoinType;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;

/** Utilities for batch join planning. */
public class BatchJoinRules {
  private BatchJoinRules() {}

  public static boolean isEquiJoin(JoinInfo joinInfo) {
    return joinInfo.nonEquiConditions.isEmpty() && !joinInfo.leftKeys.isEmpty();
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
