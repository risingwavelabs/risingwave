package com.risingwave.planner.sql;

import static java.util.Objects.requireNonNull;
import static org.apache.calcite.rex.RexUtil.composeConjunction;
import static org.apache.calcite.rex.RexUtil.composeDisjunction;
import static org.apache.calcite.rex.RexUtil.sargRef;

import java.util.List;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexUnknownAs;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.Sarg;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Utility methods for RexNode.
 */
public class RisingWaveRexUtil {
  /**
   * Expands all the calls to {@link SqlStdOperatorTable#SEARCH} in an expression.
   */
  public static RexNode expandSearch(
      RexBuilder rexBuilder, @Nullable RexProgram program, RexNode node) {
    return expandSearch(rexBuilder, program, node, -1);
  }

  /**
   * Expands calls to {@link SqlStdOperatorTable#SEARCH} whose complexity is greater than {@code
   * maxComplexity} in an expression.
   */
  public static RexNode expandSearch(
      RexBuilder rexBuilder, @Nullable RexProgram program, RexNode node, int maxComplexity) {
    return node.accept(searchShuttle(rexBuilder, program, maxComplexity));
  }

  public static RexShuttle searchShuttle(
      RexBuilder rexBuilder, @Nullable RexProgram program, int maxComplexity) {
    return new SearchExpandingShuttle(program, rexBuilder, maxComplexity);
  }

  /**
   * Shuttle that expands calls to {@link org.apache.calcite.sql.fun.SqlStdOperatorTable#SEARCH}.
   *
   * <p>Calls whose complexity is greater than {@link #maxComplexity} are retained (not expanded).
   *
   * <p>This class enhances the functionality of {@link RexShuttle} by adding {@code SqlKind.CASE}
   * operator.
   */
  private static class SearchExpandingShuttle extends RexShuttle {
    private final RexBuilder rexBuilder;
    private final @Nullable RexProgram program;
    private final int maxComplexity;

    SearchExpandingShuttle(@Nullable RexProgram program, RexBuilder rexBuilder, int maxComplexity) {
      this.program = program;
      this.rexBuilder = rexBuilder;
      this.maxComplexity = maxComplexity;
    }

    @Override
    public RexNode visitCall(RexCall call) {
      final boolean[] update = {false};
      final List<RexNode> clonedOperands;
      switch (call.getKind()) {
        // Flatten AND/OR operands.
        case OR:
          clonedOperands = visitList(call.operands, update);
          if (update[0]) {
            return composeDisjunction(rexBuilder, clonedOperands);
          } else {
            return call;
          }
        case AND:
          clonedOperands = visitList(call.operands, update);
          if (update[0]) {
            return composeConjunction(rexBuilder, clonedOperands);
          } else {
            return call;
          }
        case CASE:
          clonedOperands = visitList(call.operands, update);
          if (update[0]) {
            return rexBuilder.makeCall(call.getOperator(), clonedOperands);
          } else {
            return call;
          }

        case SEARCH:
          final RexNode ref = call.operands.get(0);
          final RexLiteral literal = (RexLiteral) deref(program, call.operands.get(1));
          final Sarg sarg = requireNonNull(literal.getValueAs(Sarg.class), "Sarg");
          if (maxComplexity < 0 || sarg.complexity() < maxComplexity) {
            return sargRef(rexBuilder, ref, sarg, literal.getType(), RexUnknownAs.UNKNOWN);
          }
          // Sarg is complex (therefore useful); fall through
        default:
          return super.visitCall(call);
      }
    }
  }

  private static RexNode deref(@Nullable RexProgram program, RexNode node) {
    while (node instanceof RexLocalRef) {
      node = requireNonNull(program, "program").getExprList().get(((RexLocalRef) node).getIndex());
    }
    return node;
  }
}
