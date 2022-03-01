/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.risingwave.planner.rules.logical.subquery;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;

/**
 * Planner rule that pushes down expressions in "equal" join condition.
 *
 * <p>This rules is copied from Calcite's {@link
 * org.apache.calcite.rel.rules.JoinPushExpressionsRule}. Modification: - Supports SEMI/ANTI join
 * using {@link org.apache.flink.table.planner.plan.utils.FlinkRelOptUtil#pushDownJoinConditions} -
 * Only push down calls on non-time-indicator field.
 *
 * <p>For example, given "emp JOIN dept ON emp.deptno + 1 = dept.deptno", adds a project above "emp"
 * that computes the expression "emp.deptno + 1". The resulting join condition is a simple
 * combination of AND, equals, and input fields, plus the remaining non-equal conditions.
 *
 * <p>This file is adapted from flink.
 */
public class JoinPushExpressionsRule extends RelOptRule {

  public static final JoinPushExpressionsRule INSTANCE =
      new JoinPushExpressionsRule(Join.class, RelFactories.LOGICAL_BUILDER);

  /** Creates a JoinPushExpressionsRule. */
  public JoinPushExpressionsRule(Class<? extends Join> clazz, RelBuilderFactory relBuilderFactory) {
    super(operand(clazz, any()), relBuilderFactory, null);
  }

  @Deprecated // to be removed before 2.0
  public JoinPushExpressionsRule(
      Class<? extends Join> clazz, RelFactories.ProjectFactory projectFactory) {
    this(clazz, RelBuilder.proto(projectFactory));
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    Join join = call.rel(0);

    // Push expression in join condition into Project below Join.
    RelNode newJoin = RelOptUtil.pushDownJoinConditions(join, call.builder());

    // If the join is the same, we bail out
    if (newJoin instanceof Join) {
      final RexNode newCondition = ((Join) newJoin).getCondition();
      if (join.getCondition().equals(newCondition)) {
        return;
      }
    }

    call.transformTo(newJoin);
  }
}
