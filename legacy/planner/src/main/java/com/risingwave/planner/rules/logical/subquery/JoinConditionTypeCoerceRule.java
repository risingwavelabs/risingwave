/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.risingwave.planner.rules.logical.subquery;

import com.google.common.collect.Lists;
import com.risingwave.planner.planner.PlannerUtils;
import java.util.ArrayList;
import java.util.stream.Collectors;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeUtil;

/**
 * Planner rule that coerces the both sides of EQUALS(`=`) operator in Join condition to the same
 * type while sans nullability.
 *
 * <p>For most cases, we already did the type coercion during type validation by implicit type
 * coercion or during sqlNode to relNode conversion, this rule just does a rechecking to ensure a
 * strongly uniform equals type, so that during a HashJoin shuffle we can have the same hashcode of
 * the same value.
 *
 * <p>This file is adapted from flink.
 */
public class JoinConditionTypeCoerceRule extends RelOptRule {
  public static final JoinConditionTypeCoerceRule INSTANCE = new JoinConditionTypeCoerceRule();

  public JoinConditionTypeCoerceRule() {
    super(operand(Join.class, any()), "JoinConditionTypeCoerceRule");
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    Join join = call.rel(0);
    if (join.getCondition().isAlwaysTrue()) {
      return false;
    }
    var typeFactory = call.builder().getTypeFactory();
    return hasEqualsRefsOfDifferentTypes(typeFactory, join.getCondition());
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    Join join = call.rel(0);
    var builder = call.builder();
    var rexBuilder = builder.getRexBuilder();
    var typeFactory = builder.getTypeFactory();

    var newJoinFilters = new ArrayList<RexNode>();
    var joinFilters = RelOptUtil.conjunctions(join.getCondition());
    for (var filter : joinFilters) {
      if (filter instanceof RexCall && filter.isA(SqlKind.EQUALS)) {
        var c = (RexCall) filter;
        if (c.getOperands().stream().allMatch(o -> o instanceof RexInputRef)) {
          var ref1 = (RexInputRef) c.getOperands().get(0);
          var ref2 = (RexInputRef) c.getOperands().get(1);
          if (!SqlTypeUtil.equalSansNullability(typeFactory, ref1.getType(), ref2.getType())) {
            var refList = Lists.newArrayList(ref1, ref2);
            var targetType =
                typeFactory.leastRestrictive(
                    refList.stream().map(RexNode::getType).collect(Collectors.toList()));
            if (targetType == null) {
              return;
            }
            newJoinFilters.add(
                builder.equals(
                    rexBuilder.ensureType(targetType, ref1, true),
                    rexBuilder.ensureType(targetType, ref2, true)));
          } else {
            newJoinFilters.add(c);
          }
        } else {
          newJoinFilters.add(c);
        }
      } else {
        newJoinFilters.add(filter);
      }
    }

    var newCondExp =
        builder.and(
            PlannerUtils.simplify(
                rexBuilder,
                builder.and(newJoinFilters),
                join.getCluster().getPlanner().getExecutor()));

    var newJoin =
        join.copy(
            join.getTraitSet(),
            newCondExp,
            join.getLeft(),
            join.getRight(),
            join.getJoinType(),
            join.isSemiJoinDone());

    call.transformTo(newJoin);
  }

  /**
   * Returns true if two input refs of an equal call have different types in join condition, else
   * false.
   */
  private boolean hasEqualsRefsOfDifferentTypes(RelDataTypeFactory typeFactory, RexNode predicate) {
    var conjunctions = RelOptUtil.conjunctions(predicate);
    return conjunctions.stream()
        .anyMatch(
            a -> {
              if (a instanceof RexCall && a.isA(SqlKind.EQUALS)) {
                var c = (RexCall) a;
                if (c.getOperands().stream().allMatch(x -> x instanceof RexInputRef)) {
                  var ref1 = (RexInputRef) c.getOperands().get(0);
                  var ref2 = (RexInputRef) c.getOperands().get(1);
                  return !SqlTypeUtil.equalSansNullability(
                      typeFactory, ref1.getType(), ref2.getType());
                }
              }

              return false;
            });
  }
}
