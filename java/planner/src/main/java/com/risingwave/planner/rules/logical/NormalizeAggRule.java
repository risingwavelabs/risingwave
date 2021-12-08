package com.risingwave.planner.rules.logical;

import static java.util.Collections.emptyList;

import com.risingwave.planner.rel.logical.RwLogicalAggregate;
import com.risingwave.planner.rel.logical.RwLogicalProject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.stream.Collectors;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.util.ImmutableBeans;
import org.apache.calcite.util.ImmutableIntList;

/**
 * Rule for normalizing aggregation.
 *
 * <p>This rule will do two conversions:
 *
 * <ol>
 *   <li>Rewrite some complex aggregation functions to combination of simple functions. e.g: avg(x)
 *       -> sum(x) / count(x)
 *   <li>Insert a projection after the aggregation, in the order of group keys and aggregation
 *       results.
 * </ol>
 */
public final class NormalizeAggRule extends RelRule<NormalizeAggRule.Config> {
  private NormalizeAggRule(Config config) {
    super(config);
  }

  // The reason we need `stream_null_by_row_count` function is that
  // in streaming workload, there are appends(updates) and retractions(deletions).
  // So when we decide the output of some aggregation function, we need to consider
  // two cases:
  // 1. output 0 because the aggregation function has non-zero rows of inputs and computes to be 0
  // 2. output NULL because the aggregation function has zero rows of inputs.
  public static final SqlFunction STREAM_NULL_BY_ROW_COUNT =
      new SqlFunction(
          "$STREAM_NULL_BY_ROW_COUNT",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.ARG1,
          null,
          null,
          SqlFunctionCategory.USER_DEFINED_SPECIFIC_FUNCTION);

  private interface ExprGen {
    RexNode gen(RelNode input, ImmutableIntList refs);
  }

  private static class ExprClosure {
    ImmutableIntList refs;
    ExprGen gen;

    public ExprClosure(ImmutableIntList refs, ExprGen gen) {
      this.refs = refs;
      this.gen = gen;
    }
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    LogicalAggregate logicalAgg = call.rel(0);
    final var rexBuilder = logicalAgg.getCluster().getRexBuilder();
    final var aggCalls = logicalAgg.getAggCallList();
    final var input = logicalAgg.getInput();
    final var groupCount = logicalAgg.getGroupCount();

    final var newAggCalls = new ArrayList<AggregateCall>();
    final var expressionClosures = new ArrayList<ExprClosure>();

    logicalAgg.getGroupSet().toList().stream()
        .map(
            idx ->
                new ExprClosure(
                    ImmutableIntList.of(idx),
                    ((rwAgg, refs) -> rexBuilder.makeInputRef(rwAgg, refs.get(0)))))
        .forEachOrdered(expressionClosures::add);

    if (config.isStreaming()) {
      final var rowCountAgg =
          AggregateCall.create(
              SqlStdOperatorTable.COUNT,
              false,
              false,
              false,
              new ArrayList<>(),
              -1,
              null,
              RelCollations.EMPTY,
              logicalAgg.getGroupCount(),
              logicalAgg.getInput(),
              null,
              null);
      newAggCalls.add(0, rowCountAgg);
    }

    for (var aggCall : aggCalls) {
      ExprClosure exprClosure;
      if (aggCall.getAggregation().getKind() == SqlKind.AVG) {
        final var idx = newAggCalls.size();
        final var sumAgg =
            AggregateCall.create(
                SqlStdOperatorTable.SUM,
                aggCall.isDistinct(),
                aggCall.isApproximate(),
                aggCall.ignoreNulls(),
                aggCall.getArgList(),
                aggCall.filterArg,
                aggCall.distinctKeys,
                aggCall.getCollation(),
                logicalAgg.getGroupCount(),
                logicalAgg.getInput(),
                null,
                null);
        final var countAgg =
            AggregateCall.create(
                SqlStdOperatorTable.COUNT,
                false,
                false,
                false,
                aggCall.getArgList(),
                aggCall.filterArg,
                aggCall.distinctKeys,
                aggCall.getCollation(),
                logicalAgg.getGroupCount(),
                logicalAgg.getInput(),
                null,
                null);
        newAggCalls.add(sumAgg);
        newAggCalls.add(countAgg);
        exprClosure =
            new ExprClosure(
                ImmutableIntList.of(idx + groupCount, idx + groupCount + 1),
                (rwAgg, refs) -> {
                  final var left = rexBuilder.makeInputRef(rwAgg, refs.get(0));
                  final var right = rexBuilder.makeInputRef(rwAgg, refs.get(1));
                  return rexBuilder.makeCall(SqlStdOperatorTable.DIVIDE, left, right);
                });
      } else {
        var idx = newAggCalls.size();
        newAggCalls.add(aggCall);
        exprClosure =
            new ExprClosure(
                ImmutableIntList.of(groupCount + idx),
                (rwAgg, refs) -> rexBuilder.makeInputRef(rwAgg, refs.get(0)));
      }

      var exprClosure2 = exprClosure;

      if (logicalAgg.groupSets.isEmpty()
          && aggCall.getAggregation().getKind() != SqlKind.COUNT
          && config.isStreaming()) {
        var originalRefsSize = exprClosure.refs.size();
        exprClosure2 =
            new ExprClosure(
                ImmutableIntList.of(groupCount).appendAll(exprClosure.refs),
                (rwAgg, refs) ->
                    rexBuilder.makeCall(
                        STREAM_NULL_BY_ROW_COUNT,
                        rexBuilder.makeInputRef(rwAgg, groupCount),
                        exprClosure.gen.gen(
                            rwAgg,
                            ImmutableIntList.copyOf(refs.subList(1, 1 + originalRefsSize)))));
      }
      expressionClosures.add(exprClosure2);
    }

    var aggCallSet = new HashMap<AggregateCall, Integer>();
    var refRewriteMapping = new HashMap<Integer, Integer>();
    for (int idx = 0; idx < aggCalls.size(); idx++) {
      var aggCall = newAggCalls.get(idx);
      if (aggCallSet.containsKey(aggCall)) {
        var prevIdx = aggCallSet.get(aggCall);
        refRewriteMapping.put(groupCount + idx, groupCount + prevIdx);
      } else {
        aggCallSet.put(aggCall, idx);
      }
    }

    var expressionClosures2 =
        expressionClosures.stream()
            .map(
                closure ->
                    new ExprClosure(
                        ImmutableIntList.copyOf(
                            closure.refs.stream()
                                .mapToInt(idx -> refRewriteMapping.getOrDefault(idx, idx))
                                .iterator()),
                        closure.gen));

    var rwAgg =
        new RwLogicalAggregate(
            logicalAgg.getCluster(),
            logicalAgg.getTraitSet(),
            logicalAgg.getHints(),
            input,
            logicalAgg.getGroupSet(),
            logicalAgg.getGroupSets(),
            newAggCalls);

    var expressions =
        expressionClosures2
            .map(closure -> closure.gen.gen(rwAgg, closure.refs))
            .collect(Collectors.toList());

    var project =
        new RwLogicalProject(
            logicalAgg.getCluster(),
            logicalAgg.getTraitSet(),
            emptyList(),
            rwAgg,
            expressions,
            // We remark that the final output should be the same as
            // the output of the original row type
            logicalAgg.getRowType());

    call.transformTo(project);
  }

  /** TODO */
  public interface Config extends RelRule.Config {
    Config DEFAULT =
        Config.EMPTY
            .withDescription("Normalize aggregation")
            .withOperandSupplier(s -> s.operand(LogicalAggregate.class).anyInputs())
            .as(Config.class);

    Config STREAMING = DEFAULT.withStreaming(true);

    Config BATCH = DEFAULT.withStreaming(false);

    default NormalizeAggRule toRule() {
      return new NormalizeAggRule(this);
    }

    @ImmutableBeans.Property
    @ImmutableBeans.BooleanDefault(true)
    boolean isStreaming();

    Config withStreaming(boolean isStreaming);
  }
}
