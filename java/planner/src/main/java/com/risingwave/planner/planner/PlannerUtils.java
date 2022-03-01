package com.risingwave.planner.planner;

import static com.risingwave.common.config.LeaderServerConfigurations.CLUSTER_MODE;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.risingwave.common.config.LeaderServerConfigurations;
import com.risingwave.execution.context.ExecutionContext;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.Util;

/** Utilities functions for Planner */
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

  /** Get the number of RexCall in the given node. */
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

  /** Helps [[toCnf]] */
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
        case AND:
          {
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
        case OR:
          {
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
        case NOT:
          {
            var arg = ((RexCall) rex).getOperands().get(0);
            switch (arg.getKind()) {
              case NOT:
                return toCnf2(((RexCall) arg).getOperands().get(0));
              case OR:
                {
                  var operands = ((RexCall) arg).getOperands();
                  return toCnf2(
                      and(
                          RexUtil.flattenOr(operands).stream()
                              .map(this::addNot)
                              .collect(Collectors.toList())));
                }
              case AND:
                {
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
}
