package com.risingwave.planner.planner;

import static com.risingwave.common.config.LeaderServerConfigurations.CLUSTER_MODE;
import static com.risingwave.planner.sql.RisingWaveOverrideOperatorTable.AND;
import static com.risingwave.planner.sql.RisingWaveOverrideOperatorTable.OR;
import static org.apache.calcite.sql.SqlKind.BINARY_COMPARISON;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.EQUALS;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.GREATER_THAN;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.GREATER_THAN_OR_EQUAL;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.LESS_THAN;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.LESS_THAN_OR_EQUAL;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.NOT_EQUALS;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.risingwave.common.config.LeaderServerConfigurations;
import com.risingwave.execution.context.ExecutionContext;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexExecutor;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSimplify;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.Util;

/**
 * Utilities functions for Planner
 */
public class PlannerUtils {
  public static boolean isSingleMode(ExecutionContext context) {
    return context.getConf().get(CLUSTER_MODE) == LeaderServerConfigurations.ClusterMode.Single;
  }

  public static boolean isDistributedMode(ExecutionContext context) {
    return context.getConf().get(CLUSTER_MODE)
        == LeaderServerConfigurations.ClusterMode.Distributed;
  }

  /**
   * Similar to [[RexUtil#toCnf(RexBuilder, Int, RexNode)]]; it lets you specify a threshold in the
   * number of nodes that can be created out of the conversion. however, if the threshold is a
   * negative number, this method will give a default threshold value that is double of the number
   * of RexCall in the given node.
   *
   * <p>If the number of resulting RexCalls exceeds that threshold, stops conversion and returns the
   * original expression.
   *
   * <p>Leaf nodes(e.g. RexInputRef) in the expression do not count towards the threshold.
   *
   * <p>We strongly discourage use the [[RexUtil#toCnf(RexBuilder, RexNode)]] and
   * [[RexUtil#toCnf(RexBuilder, Int, RexNode)]], because there are many bad case when using
   * [[RexUtil#toCnf(RexBuilder, RexNode)]], such as predicate in TPC-DS q41.sql will be converted
   * to extremely complex expression (including 736450 RexCalls); and we can not give an appropriate
   * value for `maxCnfNodeCount` when using [[RexUtil#toCnf(RexBuilder, Int, RexNode)]].
   */
  public static RexNode toCnf(RexBuilder rexBuilder, int maxCnfNodeCount, RexNode rex) {
    var maxCnfNodeCnt = maxCnfNodeCount;
    if (maxCnfNodeCount < 0) {
      maxCnfNodeCnt = getNumberOfRexCall(rex) * 2;
    }
    return new CnfHelper(rexBuilder, maxCnfNodeCnt).toCnf(rex);
  }

  /**
   * Get the number of RexCall in the given node.
   */
  private static int getNumberOfRexCall(RexNode rex) {
    final int[] numberOfNodes = new int[] {0};
    rex.accept(
        new RexVisitorImpl<Void>(true) {
          @Override
          public Void visitCall(RexCall call) {
            numberOfNodes[0]++;
            return super.visitCall(call);
          }
        });
    return numberOfNodes[0];
  }

  /**
   * Helps [[toCnf]]
   */
  private static class CnfHelper {
    private final RexBuilder rexBuilder;
    private final int maxNodeCount;

    CnfHelper(RexBuilder rexBuilder, int maxNodeCount) {
      this.rexBuilder = rexBuilder;
      this.maxNodeCount = maxNodeCount;
    }

    private RexNode addNot(RexNode input) {
      return rexBuilder.makeCall(input.getType(), SqlStdOperatorTable.NOT, ImmutableList.of(input));
    }

    public RexNode toCnf(RexNode rex) {
      try {
        return toCnf2(rex);
      } catch (Throwable t) {
        return rex;
      }
    }

    private RexNode toCnf2(RexNode rex) {
      switch (rex.getKind()) {
        case AND: {
          List<RexNode> cnfOperands = Lists.newArrayList();
          var operands = RexUtil.flattenAnd(((RexCall) rex).getOperands());
          for (RexNode node : operands) {
            var cnf = toCnf2(node);
            if (cnf.getKind() == SqlKind.AND) {
              cnfOperands.addAll(((RexCall) cnf).getOperands());
            } else {
              cnfOperands.add(cnf);
            }
          }
          var node = and(cnfOperands);
          checkCnfRexCallCount(node);
          return node;
        }
        case OR: {
          var operands = RexUtil.flattenOr(((RexCall) rex).getOperands());
          var head = operands.get(0);
          var headCnf = toCnf2(head);
          var headCnfs = RelOptUtil.conjunctions(headCnf);
          var tail = or(Util.skip(operands));
          var tailCnf = toCnf2(tail);
          var tailCnfs = RelOptUtil.conjunctions(tailCnf);
          List<RexNode> list = Lists.newArrayList();
          for (var h : headCnfs) {
            for (var t : tailCnfs) {
              list.add(or(ImmutableList.of(h, t)));
            }
          }
          var node = and(list);
          checkCnfRexCallCount(node);
          return node;
        }
        case NOT: {
          var arg = ((RexCall) rex).getOperands().get(0);
          switch (arg.getKind()) {
            case NOT:
              return toCnf2(((RexCall) arg).getOperands().get(0));
            case OR: {
              var operands = ((RexCall) arg).getOperands();
              return toCnf2(
                  and(
                      RexUtil.flattenOr(operands).stream()
                          .map(this::addNot)
                          .collect(Collectors.toList())));
            }
            case AND: {
              var operands = ((RexCall) arg).getOperands();
              return toCnf2(
                  or(
                      RexUtil.flattenAnd(operands).stream()
                          .map(this::addNot)
                          .collect(Collectors.toList())));
            }
            default:
              return rex;
          }
        }
        default:
          return rex;
      }
    }

    private void checkCnfRexCallCount(RexNode node) {
      // TODO use more efficient solution to get number of RexCall in CNF node
      Preconditions.checkArgument(maxNodeCount >= 0 && getNumberOfRexCall(node) > maxNodeCount);
    }

    private RexNode and(Iterable<? extends RexNode> nodes) {
      return RexUtil.composeConjunction(rexBuilder, nodes, false);
    }

    private RexNode or(Iterable<? extends RexNode> nodes) {
      return RexUtil.composeDisjunction(rexBuilder, nodes);
    }
  }

  /**
   * Merges same expressions and then simplifies the result expression by [[RexSimplify]].
   * <p>
   * Examples for merging same expressions:
   * 1. a = b AND b = a -> a = b
   * 2. a = b OR b = a -> a = b
   * 3. (a > b AND c < 10) AND b < a -> a > b AND c < 10
   * 4. (a > b OR c < 10) OR b < a -> a > b OR c < 10
   * 5. a = a, a >= a, a <= a -> true
   * 6. a <> a, a > a, a < a -> false
   */
  public static RexNode simplify(RexBuilder rexBuilder, RexNode expr, RexExecutor executor) {
    if (expr.isAlwaysTrue() || expr.isAlwaysFalse()) {
      return expr;
    }

    var exprShuttle = new EquivalentExprShuttle(rexBuilder);
    var equiExpr = expr.accept(exprShuttle);
    var exprMerger = new SameExprMerger(rexBuilder);
    var sameExprMerged = exprMerger.mergeSameExpr(equiExpr);
    var binaryComparisonExprReduced = sameExprMerged.accept(
        new BinaryComparisonExprReducer(rexBuilder));

    var rexSimplify = new RexSimplify(rexBuilder, RelOptPredicateList.EMPTY, true, executor);
    return rexSimplify.simplify(binaryComparisonExprReduced);
  }

  private static class EquivalentExprShuttle extends RexShuttle {
    private final RexBuilder rexBuilder;
    private final Map<String, RexNode> equiExprMap = new HashMap<>();

    public EquivalentExprShuttle(RexBuilder rexBuilder) {
      this.rexBuilder = rexBuilder;
    }

    @Override
    public RexNode visitCall(RexCall call) {
      SqlOperator operator = call.getOperator();
      if (EQUALS.equals(operator) || NOT_EQUALS.equals(operator) || GREATER_THAN.equals(operator) || LESS_THAN.equals(operator) || GREATER_THAN_OR_EQUAL.equals(operator) || LESS_THAN_OR_EQUAL.equals(operator)) {
        var swapped = swapOperands(call);
        if (equiExprMap.containsKey(swapped.toString())) {
          return swapped;
        } else {
          equiExprMap.put(call.toString(), call);
          return call;
        }
      }
      return super.visitCall(call);
    }

    private RexCall swapOperands(RexCall call) {
      SqlOperator newOp;
      SqlOperator operator = call.getOperator();
      if (EQUALS.equals(operator) || NOT_EQUALS.equals(operator)) {
        newOp = call.getOperator();
      } else if (GREATER_THAN.equals(operator)) {
        newOp = LESS_THAN;
      } else if (GREATER_THAN_OR_EQUAL.equals(operator)) {
        newOp = LESS_THAN_OR_EQUAL;
      } else if (LESS_THAN.equals(operator)) {
        newOp = GREATER_THAN;
      } else if (LESS_THAN_OR_EQUAL.equals(operator)) {
        newOp = GREATER_THAN_OR_EQUAL;
      } else {
        throw new IllegalArgumentException(String.format("Unsupported operator: %s", call.getOperator()));
      }
      var operands = call.getOperands();
      return (RexCall) rexBuilder.makeCall(newOp, operands.get(operands.size() - 1), operands.get(0));
    }
  }

  private static class BinaryComparisonExprReducer extends RexShuttle {
    private final RexBuilder rexBuilder;

    public BinaryComparisonExprReducer(RexBuilder rexBuilder) {
      this.rexBuilder = rexBuilder;
    }

    @Override
    public RexNode visitCall(RexCall call) {
      var kind = call.getOperator().getKind();
      if (!kind.belongsTo(BINARY_COMPARISON)) {
        return super.visitCall(call);
      } else {
        var operand0 = call.getOperands().get(0);
        var operand1 = call.getOperands().get(1);
        if (operand0 instanceof RexInputRef
            && operand1 instanceof RexInputRef
            && (((RexInputRef) operand0).getIndex() == ((RexInputRef) operand1).getIndex())) {
          switch (kind) {
            case EQUALS:
            case LESS_THAN_OR_EQUAL:
            case GREATER_THAN_OR_EQUAL:
              return rexBuilder.makeLiteral(true);
            case NOT_EQUALS:
            case LESS_THAN:
            case GREATER_THAN:
              return rexBuilder.makeLiteral(false);
            default:
              return super.visitCall(call);
          }
        } else {
          return super.visitCall(call);
        }
      }
    }
  }

  private static class SameExprMerger extends RexShuttle {
    private final RexBuilder rexBuilder;
    private final Map<String, RexNode> sameExprMap = new HashMap<>();

    private SameExprMerger(RexBuilder rexBuilder) {
      this.rexBuilder = rexBuilder;
    }

    private RexNode mergeSameExpr(RexNode expr, RexLiteral equiExpr) {
      if (sameExprMap.containsKey(expr.toString())) {
        return equiExpr;
      } else {
        sameExprMap.put(expr.toString(), expr);
        return expr;
      }
    }

    public RexNode mergeSameExpr(RexNode expr) {
      // merges same expressions in the operands of AND and OR
      // e.g. a = b AND a = b -> a = b AND true
      //      a = b OR a = b -> a = b OR false
      var newExpr1 = expr.accept(this);

      // merges same expressions in conjunctions
      // e.g. (a > b AND c < 10) AND a > b -> a > b AND c < 10 AND true
      sameExprMap.clear();
      var newConjunctions = RelOptUtil.conjunctions(newExpr1).stream().map(
          ex -> mergeSameExpr(ex, rexBuilder.makeLiteral(true))
      ).collect(Collectors.toList());

      RexNode newExpr2;
      if (newConjunctions.size() == 0) {
        newExpr2 = newExpr1;
      } else if (newConjunctions.size() == 1) {
        newExpr2 = newConjunctions.get(0);
      } else {
        newExpr2 = rexBuilder.makeCall(AND, newConjunctions.toArray(new RexNode[0]));
      }

      // merges same expressions in disjunctions
      // e.g. (a > b OR c < 10) OR a > b -> a > b OR c < 10 OR false
      sameExprMap.clear();

      RexNode newExpr3;
      if (newConjunctions.size() == 0) {
        newExpr3 = newExpr2;
      } else if (newConjunctions.size() == 1) {
        newExpr3 = newConjunctions.get(0);
      } else {
        newExpr3 = rexBuilder.makeCall(OR, RelOptUtil.disjunctions(newExpr2).stream().map(
            ex -> mergeSameExpr(ex, rexBuilder.makeLiteral(false))
        ).toArray(RexNode[]::new));
      }

      return newExpr3;
    }

    @Override
    public RexNode visitCall(RexCall call) {
      RexCall newCall = call;
      if (call.getOperator() == AND || call.getOperator() == OR) {
        sameExprMap.clear();
        var newOperands = call.getOperands().stream().map(
            op -> {

              var value = call.getOperator() == AND;
              return mergeSameExpr(op, rexBuilder.makeLiteral(value));
            }).collect(Collectors.toList());

        newCall = call.clone(call.getType(), newOperands);
      }
      return super.visitCall(newCall);
    }
  }


}