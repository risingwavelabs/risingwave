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

import java.util.Objects;
import java.util.Optional;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlCountAggFunction;
import org.apache.calcite.sql.type.SqlTypeFamily;

/**
 * Planner rule that rewrites scalar query in filter like:
 * `select * from T1 where (select count(*) from T2) > 0`
 * to
 * `select * from T1 where exists (select * from T2)`,
 * which could be converted to SEMI join by [[FlinkSubQueryRemoveRule]].
 * <p>
 * Without this rule, the original query will be rewritten to a filter on a join on an aggregate
 * by [[org.apache.calcite.rel.rules.SubQueryRemoveRule]]. the full logical plan is
 * {{{
 * LogicalProject(a=[$0], b=[$1], c=[$2])
 * +- LogicalJoin(condition=[$3], joinType=[semi])
 * :- LogicalTableScan(table=[[x, source: [TestTableSource(a, b, c)]]])
 * +- LogicalProject($f0=[IS NOT NULL($0)])
 * +- LogicalAggregate(group=[{}], m=[MIN($0)])
 * +- LogicalProject(i=[true])
 * +- LogicalTableScan(table=[[y, source: [TestTableSource(d, e, f)]]])
 * }}}
 *
 * <p> This file is adapted from flink.
 */
class RewriteSubQueryRule extends RelOptRule {
  public static final RewriteSubQueryRule INSTANCE = new RewriteSubQueryRule();

  public RewriteSubQueryRule() {
    super(operandJ(Filter.class, null, RexUtil.SubQueryFinder.FILTER_PREDICATE, any()),
        RelFactories.LOGICAL_BUILDER,
        "FlinkRewriteSubQueryRule:Filter");
  }


  @Override
  public void onMatch(RelOptRuleCall call) {
    Filter filter = call.rel(0);
    var condition = filter.getCondition();
    var newCondition = rewriteScalarQuery(condition);
    if (RexUtil.eq(condition, newCondition)) {
      return;
    }

    var newFilter = filter.copy(filter.getTraitSet(), filter.getInput(), newCondition);
    call.transformTo(newFilter);
  }

  // scalar query like: `(select count(*) from T) > 0` can be converted to `exists(select * from T)`
  RexNode rewriteScalarQuery(RexNode condition) {
    return condition.accept(new RexShuttle() {
      @Override
      public RexNode visitCall(RexCall call) {
        var subQuery = getSupportedScalarQuery(call);
        if (subQuery.isPresent()) {
          var sq = subQuery.get();
          var aggInput = sq.rel.getInput(0);
          return RexSubQuery.exists(aggInput);
        } else {
          return super.visitCall(call);
        }
      }
    });
  }

  private boolean isScalarQuery(RexNode n) {
    return n.isA(SqlKind.SCALAR_QUERY);
  }

  // check the RexNode is a RexLiteral which's value is between 0 and 1
  private static boolean isBetween0And1(RexNode n, boolean include0, boolean include1) {
    if (n instanceof RexLiteral) {
      var l = (RexLiteral) n;
      if (Objects.requireNonNull(l.getTypeName().getFamily()) == SqlTypeFamily.NUMERIC) {
        if (l.getValue() != null) {
          var v = Double.parseDouble(l.getValue().toString());
          return (0.0 < v && v < 1.0) || (include0 && v == 0.0) || (include1 && v == 1.0);
        }
      }
    }
    return false;
  }

  // check the RelNode is a Aggregate which has only count aggregate call with empty args
  private static boolean isCountStarAggWithoutGroupBy(RelNode n) {
    if (n instanceof Aggregate) {
      var agg = (Aggregate) n;
      if (agg.getGroupCount() == 0 && agg.getAggCallList().size() == 1) {
        var aggCall = agg.getAggCallList().get(0);
        return !aggCall.isDistinct() &&
            aggCall.filterArg < 0 &&
            aggCall.getArgList().isEmpty() &&
            (aggCall.getAggregation() instanceof SqlCountAggFunction);
      }
    }
    return false;
  }

  private Optional<RexSubQuery> getSupportedScalarQuery(RexCall call) {
    switch (call.getKind()) {
      // (select count(*) from T) > X (X is between 0 (inclusive) and 1 (exclusive))
      case GREATER_THAN: {
        if (isScalarQuery(call.operands.get(0))) {
          var subQuery = (RexSubQuery) call.operands.get(0);
          if (isCountStarAggWithoutGroupBy(subQuery.rel) &&
              isBetween0And1(call.operands.get(call.operands.size() - 1), true, false)) {
            return Optional.of(subQuery);
          } else {
            return Optional.empty();
          }
        }
        break;
      }

      // (select count(*) from T) >= X (X is between 0 (exclusive) and 1 (inclusive))
      case GREATER_THAN_OR_EQUAL: {
        if (isScalarQuery(call.operands.get(0))) {
          var subQuery = (RexSubQuery) call.operands.get(0);

          if (isCountStarAggWithoutGroupBy(subQuery.rel) &&
              isBetween0And1(call.operands.get(call.getOperands().size() - 1), false, true)) {
            return Optional.of(subQuery);
          } else {
            return Optional.empty();
          }
        }
        break;
      }
      // X < (select count(*) from T) (X is between 0 (inclusive) and 1 (exclusive))
      case LESS_THAN: {
        if (isScalarQuery(call.operands.get(call.getOperands().size() - 1))) {
          var subQuery = (RexSubQuery) call.operands.get(call.getOperands().size() - 1);
          if (isCountStarAggWithoutGroupBy(subQuery.rel) &&
              isBetween0And1(call.operands.get(0), true, false)) {
            return Optional.of(subQuery);
          } else {
            return Optional.empty();
          }
        }
        break;
      }
      // X <= (select count(*) from T) (X is between 0 (exclusive) and 1 (inclusive))
      case LESS_THAN_OR_EQUAL: {
        if (isScalarQuery(call.operands.get(call.operands.size() - 1))) {
          var subQuery = (RexSubQuery) call.operands.get(call.operands.size() - 1);
          if (isCountStarAggWithoutGroupBy(subQuery.rel) &&
              isBetween0And1(call.operands.get(0), false, true)) {
            return Optional.of(subQuery);
          } else {
            return Optional.empty();
          }
        }
        break;
      }
      default:
        break;
    }
    return Optional.empty();
  }
}
