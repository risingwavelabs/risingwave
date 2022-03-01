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

import com.risingwave.planner.planner.PlannerUtils;
import java.util.Optional;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexUtil;

/**
 * Planner rule that apply various simplifying transformations on filter condition.
 *
 * <p> if `simplifySubQuery` is true, this rule will also simplify the filter condition
 * in [[RexSubQuery]].
 *
 * <p> This file is adapted from flink.
 */
class SimplifyFilterConditionRule extends RelOptRule {

  private final boolean simplifySubQuery = true;

  public SimplifyFilterConditionRule() {
    super(operand(Filter.class, any()), "SimplifyFilterConditionRule:simplifySubQuery");
  }

  public void onMatch(RelOptRuleCall call) {
    Filter filter = call.rel(0);
    var changed = new boolean[] {false};
    var newFilter = simplify(filter, changed);
    newFilter.ifPresent(f -> {
      call.transformTo(f);
      call.getPlanner().prune(filter);
    });
  }

  private Optional<Filter> simplify(Filter filter, boolean[] changed) {
    var condition = simplifyFilterConditionInSubQuery(filter.getCondition(), changed);

    var rexBuilder = filter.getCluster().getRexBuilder();
    var simplifiedCondition = PlannerUtils.simplify(
        rexBuilder,
        condition,
        filter.getCluster().getPlanner().getExecutor());
    var newCondition = RexUtil.pullFactors(rexBuilder, simplifiedCondition);

    if (!changed[0] && !condition.equals(newCondition)) {
      changed[0] = true;
    }

    // just replace modified RexNode
    if (changed[0]) {
      return Optional.of(filter.copy(filter.getTraitSet(), filter.getInput(), newCondition));
    } else {
      return Optional.empty();
    }
  }

  private RexNode simplifyFilterConditionInSubQuery(RexNode condition, boolean[] changed) {
    return condition.accept(new RexShuttle() {
      @Override
      public RexNode visitSubQuery(RexSubQuery subQuery) {
        var newRel = subQuery.rel.accept(new RelShuttleImpl() {
          public RelNode visit(LogicalFilter filter) {
            return simplify(filter, changed).orElse(filter);
          }
        });
        if (changed[0]) {
          return subQuery.clone(newRel);
        } else {
          return subQuery;
        }
      }
    });
  }
}
