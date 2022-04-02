package com.risingwave.planner.rel.physical.batch.join;

import static com.risingwave.planner.rel.physical.join.BatchJoinUtils.getJoinTypeProto;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.risingwave.proto.plan.JoinType;
import org.apache.calcite.rel.core.JoinRelType;
import org.junit.jupiter.api.Test;

/** To make code coverage happy. */
class BatchJoinUtilsTest {

  @Test
  void getJoinTypeProtoTest() {
    assertEquals(JoinType.INNER, getJoinTypeProto(JoinRelType.INNER));
    assertEquals(JoinType.LEFT_OUTER, getJoinTypeProto(JoinRelType.LEFT));
    assertEquals(JoinType.RIGHT_OUTER, getJoinTypeProto(JoinRelType.RIGHT));
    assertEquals(JoinType.FULL_OUTER, getJoinTypeProto(JoinRelType.FULL));
    assertEquals(JoinType.LEFT_ANTI, getJoinTypeProto(JoinRelType.ANTI));
    assertEquals(JoinType.LEFT_SEMI, getJoinTypeProto(JoinRelType.SEMI));
  }
}
