package com.risingwave.planner.rel.serialization;

import java.io.PrintWriter;
import java.util.List;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Exchange;
import org.apache.calcite.rel.externalize.RelWriterImpl;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.util.Pair;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A plan writer which skips inputs of {@link org.apache.calcite.rel.core.Exchange}, which is useful
 * in debug.
 */
public class FragmentWriter extends RelWriterImpl {
  public FragmentWriter(PrintWriter pw) {
    super(pw, SqlExplainLevel.EXPPLAN_ATTRIBUTES, false);
  }

  protected void explain_(RelNode rel, List<Pair<String, @Nullable Object>> values) {
    List<RelNode> inputs = rel.getInputs();
    final RelMetadataQuery mq = rel.getCluster().getMetadataQuery();
    if (!mq.isVisibleInExplain(rel, detailLevel)) {
      // render children in place of this, at same level
      explainInputs(inputs);
      return;
    }

    StringBuilder s = new StringBuilder();
    spacer.spaces(s);
    if (withIdPrefix) {
      s.append(rel.getId()).append(":");
    }
    s.append(rel.getRelTypeName());
    if (detailLevel != SqlExplainLevel.NO_ATTRIBUTES) {
      int j = 0;
      for (Pair<String, @Nullable Object> value : values) {
        if (value.right instanceof RelNode) {
          continue;
        }
        if (j++ == 0) {
          s.append("(");
        } else {
          s.append(", ");
        }
        s.append(value.left).append("=[").append(value.right).append("]");
      }
      if (j > 0) {
        s.append(")");
      }
    }
    switch (detailLevel) {
      case ALL_ATTRIBUTES:
        s.append(": rowcount = ")
            .append(mq.getRowCount(rel))
            .append(", cumulative cost = ")
            .append(mq.getCumulativeCost(rel));
        break;
      default:
        break;
    }
    switch (detailLevel) {
      case NON_COST_ATTRIBUTES:
      case ALL_ATTRIBUTES:
        if (!withIdPrefix) {
          // If we didn't print the rel id at the start of the line, print
          // it at the end.
          s.append(", id = ").append(rel.getId());
        }
        break;
      default:
        break;
    }
    pw.println(s);
    if (!skipInputs(rel)) {
      spacer.add(2);
      explainInputs(inputs);
      spacer.subtract(2);
    }
  }

  private void explainInputs(List<RelNode> inputs) {
    for (RelNode input : inputs) {
      input.explain(this);
    }
  }

  private static boolean skipInputs(RelNode node) {
    return node instanceof Exchange;
  }
}
