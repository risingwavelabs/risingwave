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

import com.google.common.collect.ImmutableList;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.LogicVisitor;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

/**
 * Planner rule that converts IN and EXISTS into semi-join,
 * converts NOT IN and NOT EXISTS into anti-join.
 *
 * <p>Sub-queries are represented by [[RexSubQuery]] expressions.
 *
 * <p>A sub-query may or may not be correlated. If a sub-query is correlated,
 * the wrapped [[RelNode]] will contain a [[RexCorrelVariable]] before the rewrite,
 * and the product of the rewrite will be a [[org.apache.calcite.rel.core.Join]]
 * with SEMI or ANTI join type.
 *
 * <p>This file is adapted from flink.
 */
public class FlinkSubQueryRemoveRule extends RelOptRule {

  public FlinkSubQueryRemoveRule(
      RelOptRuleOperand operand,
      RelBuilderFactory relBuilderFactory,
      String description) {
    super(operand, relBuilderFactory, description);
  }

  public void onMatch(RelOptRuleCall call) {
    Filter filter = call.rel(0);
    var condition = filter.getCondition();

    if (hasUnsupportedSubQuery(condition)) {
      // has some unsupported subquery, such as: subquery connected with OR
      // select * from t1 where t1.a > 10 or t1.b in (select t2.c from t2)
      // TODO supports ExistenceJoin
      return;
    }

    var subQueryCall = findSubQuery(condition)
    if (subQueryCall.isEmpty) {
      // ignore scalar query
      return
    }

    var decorrelate = SubQueryDecorrelator.decorrelateQuery(filter)
    if (decorrelate == null) {
      // can't handle the query
      return
    }

    var relBuilder = call.builder.asInstanceOf[FlinkRelBuilder]
    relBuilder.push(filter.getInput) // push join left

    var newCondition = handleSubQuery(subQueryCall.get, condition, relBuilder, decorrelate)
    newCondition match {
      case Some(c) =>
        if (hasCorrelatedExpressions(c)) {
          // some correlated expressions can not be replaced in this rule,
          // so we must keep the VariablesSet for decorrelating later in new filter
          // RelBuilder.filter can not create Filter with VariablesSet arg
          var newFilter = filter.copy(filter.getTraitSet, relBuilder.build(), c)
          relBuilder.push(newFilter)
        } else {
          // all correlated expressions are replaced,
          // so we can create a new filter without any VariablesSet
          relBuilder.filter(c)
        }
        relBuilder.project(fields(relBuilder, filter.getRowType.getFieldCount))
        call.transformTo(relBuilder.build)
      case _ => // do nothing
    }
  }

  private Optional<RexNode> handleSubQuery(
      RexCall subQueryCall,
      RexNode condition,
      RelBuilder relBuilder,
      SubQueryDecorrelator.Result decorrelate) {
    var logic = LogicVisitor.find(RelOptUtil.Logic.TRUE, ImmutableList.of(condition), subQueryCall);
    if (logic != RelOptUtil.Logic.TRUE) {
      // this should not happen, none unsupported SubQuery could not reach here
      // this is just for double-check
      return Optional.empty();
    }

    var target = apply(subQueryCall, relBuilder, decorrelate)
    if (target.isEmpty) {
      return None
    }

    var newCondition = replaceSubQuery(condition, subQueryCall, target.get)
    var nextSubQueryCall = findSubQuery(newCondition)
    nextSubQueryCall match {
    case Some(subQuery) =>handleSubQuery(subQuery, newCondition, relBuilder, decorrelate)
    case _ =>Some(newCondition)
  }
  }

  private Optional<RexNode> apply(
      RexCall subQueryCall,
      RelBuilder relBuilder,
      SubQueryDecorrelator.Result decorrelate) {
    RexSubQuery subQuery;
    boolean withNot;
    if (subQueryCall instanceof RexSubQuery) {
      subQuery = (RexSubQuery) subQueryCall;
      withNot = false;
    } else {
      subQuery = (RexSubQuery) subQueryCall.getOperands().get(0);
      withNot = true;
    }

    var equivarent = decorrelate.getSubQueryEquivalent(subQuery);

    subQuery.getKind match {
    // IN and NOT IN
    //
    // NOT IN is a NULL-aware (left) anti join e.g. col NOT IN expr Construct the condition.
    // A NULL in one of the conditions is regarded as a positive result;
    // such a row will be filtered out by the Anti-Join operator.
    //
    // Rewrite logic for NOT IN:
    // Expand the NOT IN expression with the NULL-aware semantic to its full form.
    // That is from:
    //   (a1,a2,...) = (b1,b2,...)
    // to
    //   (a1=b1 OR isnull(a1=b1)) AND (a2=b2 OR isnull(a2=b2)) AND ...
    //
    // After that, add back the correlated join predicate(s) in the subquery
    // Example:
    // SELECT ... FROM A WHERE A.A1 NOT IN (SELECT B.B1 FROM B WHERE B.B2 = A.A2 AND B.B3 > 1)
    // will have the final conditions in the ANTI JOIN as
    // (A.A1 = B.B1 OR ISNULL(A.A1 = B.B1)) AND (B.B2 = A.A2)
    case SqlKind.IN =>
      // TODO:
      // Calcite does not support project with correlated expressions.
      // e.g.
      // SELECT b FROM l WHERE (
      // CASE WHEN a IN (SELECT i FROM t1 WHERE l.b = t1.j) THEN 1 ELSE 2 END)
      // IN (SELECT d FROM r)
      //
      // we can not create project with VariablesSet arg, and
      // the result of RelDecorrelator is also wrong.
      if (hasCorrelatedExpressions(subQuery.getOperands:_ *)){
      return None
    }

    var(newRight, joinCondition) = if (equivarent != null) {
      // IN has correlation variables
      (equivarent.getKey, Some(equivarent.getValue))
    } else {
      // IN has no correlation variables
      (subQuery.rel, None)
    }
    // adds projection if the operands of IN contains non-RexInputRef nodes
    // e.g. SELECT * FROM l WHERE a + 1 IN (SELECT c FROM r)
    var(newOperands, newJoinCondition) =
        handleSubQueryOperands(subQuery, joinCondition, relBuilder)
    var leftFieldCount = relBuilder.peek().getRowType.getFieldCount

    relBuilder.push(newRight) // push join right

    var joinConditions = newOperands
        .zip(relBuilder.fields())
        .map {
      case (op, f) =>
        var inCondition = relBuilder.equals(op, RexUtil.shift(f, leftFieldCount))
        if (withNot) {
          relBuilder.or(inCondition, relBuilder.isNull(inCondition))
        } else {
          inCondition
        }
    }.toBuffer

    newJoinCondition.foreach(joinConditions += _)

    if (withNot) {
      relBuilder.join(JoinRelType.ANTI, joinConditions)
    } else {
      relBuilder.join(JoinRelType.SEMI, joinConditions)
    }
    Some(relBuilder.literal(true))

    // EXISTS and NOT EXISTS
    case SqlKind.EXISTS =>
      var joinCondition = if (equivarent != null) {
      // EXISTS has correlation variables
      relBuilder.push(equivarent.getKey) // push join right
      require(equivarent.getValue != null)
      equivarent.getValue
    } else {
      // EXISTS has no correlation variables
      //
      // e.g. (table `l` has two columns: `a`, `b`, and table `r` has two columns: `c`, `d`)
      // SELECT * FROM l WHERE EXISTS (SELECT * FROM r)
      // which can be converted to:
      //
      // LogicalProject(a=[$0], b=[$1])
      //  LogicalJoin(condition=[$2], joinType=[semi])
      //    LogicalTableScan(table=[[builtin, default, l]])
      //    LogicalProject($f0=[IS NOT NULL($0)])
      //     LogicalAggregate(group=[{}], m=[MIN($0)])
      //       LogicalProject(i=[true])
      //         LogicalTableScan(table=[[builtin, default, r]])
      //
      // MIN($0) will return null when table `r` is empty,
      // so add LogicalProject($f0=[IS NOT NULL($0)]) to check null varue
      var leftFieldCount = relBuilder.peek().getRowType.getFieldCount
      relBuilder.push(subQuery.rel) // push join right
      // adds LogicalProject(i=[true]) to join right
      relBuilder.project(relBuilder.alias(relBuilder.literal(true), "i"))
      // adds LogicalAggregate(group=[{}], agg#0=[MIN($0)]) to join right
      relBuilder.aggregate(relBuilder.groupKey(), relBuilder.min("m", relBuilder.field(0)))
      // adds LogicalProject($f0=[IS NOT NULL($0)]) to check null varue
      relBuilder.project(relBuilder.isNotNull(relBuilder.field(0)))
      var fieldType = relBuilder.peek().getRowType.getFieldList.get(0).getType
      // join condition references project result directly
      new RexInputRef(leftFieldCount, fieldType)
    }

      if (withNot) {
        relBuilder.join(JoinRelType.ANTI, joinCondition)
      } else {
        relBuilder.join(JoinRelType.SEMI, joinCondition)
      }
      Some(relBuilder.literal(true))

    case _ =>None
  }
  }

  private List<RexNode> fields(RelBuilder builder, int fieldCount) {
    var projects = new ArrayList<RexNode>();
    IntStream.range(0, fieldCount)
        .forEachOrdered(i -> projects.add(builder.field(i)));
    return projects;
  }

  private static boolean isScalarQuery(RexNode n) {
    return n.isA(SqlKind.SCALAR_QUERY);
  }

  private Optional<RexCall> findSubQuery(RexNode node) {
    var subQueryFinder = new RexVisitorImpl<Void>(true) {
      @Override
      public Void visitSubQuery(RexSubQuery subQuery) {
        if (!isScalarQuery(subQuery)) {
          throw new Util.FoundOne(subQuery);
        }
        return null;
      }

      @Override
      public Void visitCall(RexCall call) {
        if (call.getKind() == SqlKind.NOT && call.getOperands().get(0) instanceof RexSubQuery) {
          if (!isScalarQuery(call.operands.get(0))) {
            throw new Util.FoundOne(call);
          }
        } else {
          return super.visitCall(call);
        }
        return null;
      }
    };

    try {
      node.accept(subQueryFinder);
      return Optional.empty();
    } catch (Util.FoundOne e) {
      return Optional.of((RexCall) Objects.requireNonNull(e.getNode()));
    }
  }

  private RexNode replaceSubQuery(
      RexNode condition,
      RexCall oldSubQueryCall,
      RexNode replacement) {
    return condition.accept(new RexShuttle() {
      @Override
      public RexNode visitSubQuery(RexSubQuery subQuery) {
        if (RexUtil.eq(subQuery, oldSubQueryCall)) {
          return replacement;
        } else {
          return subQuery;
        }
      }

      @Override
      public RexNode visitCall(RexCall call) {
        if (call.getKind() == SqlKind.NOT && call.getOperands().get(0) instanceof RexSubQuery) {
          if (RexUtil.eq(call, oldSubQueryCall)) {
            return replacement;
          } else {
            return call;
          }
        } else {
          return super.visitCall(call);
        }
      }
    });
  }

  /**
   * Adds projection if the operands of a SubQuery contains non-RexInputRef nodes,
   * and returns SubQuery's new operands and new join condition with new index.
   *
   * <p>e.g. SELECT * FROM l WHERE a + 1 IN (SELECT c FROM r)
   * We will add projection as SEMI join left input, the added projection will pass along fields
   * from the input, and add `a + 1` as new field.
   */
  private Pair<List<RexNode>, Optional<RexNode>> handleSubQueryOperands(
      RexSubQuery subQuery,
      Optional<RexNode> joinCondition,
      RelBuilder relBuilder) {
    var operands = subQuery.getOperands();
    // operands is empty or all operands are RexInputRef
    if (operands.isEmpty() || operands.stream().allMatch(x -> x instanceof RexInputRef) {
      return Pair.of(operands, joinCondition);
    }

    var rexBuilder = relBuilder.getRexBuilder();
    var oldLeftNode = relBuilder.peek();
    var oldLeftFieldCount = oldLeftNode.getRowType().getFieldCount();
    var newLeftProjects = new ArrayList<RexNode>();
    var newOperandIndices = new ArrayList<Integer>();
    for (int i = 0; i < oldLeftFieldCount; i++) {
      newLeftProjects.add(rexBuilder.makeInputRef(oldLeftNode, i));
    }
    for (var o : operands) {
      var index = newLeftProjects.indexOf(o);
      if (index < 0) {
        index = newLeftProjects.size();
        newLeftProjects.add(o);
      }
      newOperandIndices.add(index);
    }

    // adjust join condition after adds new projection

    var newJoinCondition = Optional.<RexNode>empty();
    if (joinCondition.isPresent()) {
      var offset = newLeftProjects.size() - oldLeftFieldCount;
      newJoinCondition = Optional.of(RexUtil.shift(joinCondition.get(), oldLeftFieldCount, offset));
    }

    relBuilder.project(newLeftProjects); // push new join left
    var newOperands = newOperandIndices.stream()
        .map(i -> (RexNode) rexBuilder.makeInputRef(relBuilder.peek(), i))
        .collect(Collectors.toList());
    return Pair.of(newOperands, newJoinCondition);
  }

  /**
   * Check the condition whether contains unsupported SubQuery.
   * <p>Now, we only support single SubQuery or SubQuery connected with AND.
   */
  private boolean hasUnsupportedSubQuery(RexNode condition) {
    var visitor = new RexVisitorImpl<Void>(true) {
      Deque<SqlKind> stack = new ArrayDeque<>();

      private void checkAndConjunctions(RexCall call) {
        if (stack.stream().anyMatch(x -> !x.equals(SqlKind.AND))) {
          throw new Util.FoundOne(call);
        }
      }

      @Override
      public Void visitSubQuery(RexSubQuery subQuery) {
        // ignore scalar query
        if (!isScalarQuery(subQuery)) {
          checkAndConjunctions(subQuery);
        }
        return null;
      }

      @Override
      public Void visitCall(RexCall call) {
        if (call.getKind() == SqlKind.NOT && call.operands.get(0) instanceof RexSubQuery) {
          if (!isScalarQuery(call.operands.get(0))) {
            checkAndConjunctions(call);
          }
        } else {
          stack.push(call.getKind());
          call.getOperands().forEach(r -> r.accept(this));
          stack.pop();
        }

        return null;
      }
    };

    try {
      condition.accept(visitor);
      return false;
    } catch (Util.FoundOne e) {
      return true;
    }
  }

  /**
   * Check nodes' SubQuery whether contains correlated expressions.
   */
  private boolean hasCorrelatedExpressions(List<RexNode> nodes) {
    var relShuttle = new RelShuttleImpl() {
      final RexVisitor<Void> corVarFinder = new RexVisitorImpl<Void>(true) {
        public Void visitCorrelVariable(RexCorrelVariable corVar) {
          throw new Util.FoundOne(corVar);
        }
      };

      @Override
      public RelNode visit(LogicalFilter filter) {
        filter.getCondition().accept(corVarFinder);
        return super.visit(filter);
      }

      @Override
      public RelNode visit(LogicalJoin join) {
        join.getCondition().accept(corVarFinder);
        return super.visit(join);
      }

      @Override
      public RelNode visit(LogicalProject project) {
        project.getProjects().forEach(x -> x.accept(corVarFinder));
        return super.visit(project);
      }
    };

    var subQueryFinder = new RexVisitorImpl<Void>(true) {
      @Override
      public Void visitSubQuery(RexSubQuery subQuery) {
        subQuery.rel.accept(relShuttle);
        return null;
      }
    };

    var found = false;
    for (var c : nodes) {
      if (!found) {
        try {
          c.accept(subQueryFinder);
          found = false;
        } catch (Util.FoundOne e) {
          found = true;
        }
      } else {
        found = true;
      }
    }
    return found;
  }
}

  object FlinkSubQueryRemoveRule {

    var FILTER=new FlinkSubQueryRemoveRule(
    operandJ(classOf[Filter],null,RexUtil.SubQueryFinder.FILTER_PREDICATE,any),
    FlinkRelFactories.FLINK_REL_BUILDER,
    "FlinkSubQueryRemoveRule:Filter")

    }
