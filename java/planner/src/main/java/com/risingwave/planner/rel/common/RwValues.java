package com.risingwave.planner.rel.common;

import com.google.common.collect.ImmutableList;
import java.util.stream.Collectors;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlExplainLevel;

/**
 * Replacement of {@link org.apache.calcite.rel.core.Values} to hold {@link RexNode} rather than
 * {@link org.apache.calcite.rex.RexLiteral}.
 */
public abstract class RwValues extends AbstractRelNode {
  public final ImmutableList<ImmutableList<RexNode>> tuples;

  protected RwValues(
      RelOptCluster cluster,
      RelDataType rowType,
      ImmutableList<ImmutableList<RexNode>> tuples,
      RelTraitSet traits) {
    super(cluster, traits);
    this.rowType = rowType;
    this.tuples = tuples;
  }

  /** Returns the rows of literals represented by this Values relational expression. */
  public ImmutableList<ImmutableList<RexNode>> getTuples() {
    return tuples;
  }

  // implement RelNode
  @Override
  public double estimateRowCount(RelMetadataQuery mq) {
    return tuples.size();
  }

  // implement RelNode
  @Override
  public RelWriter explainTerms(RelWriter pw) {
    // A little adapter just to get the tuples to come out
    // with curly brackets instead of square brackets.  Plus
    // more whitespace for readability.
    RelDataType rowType = getRowType();
    RelWriter relWriter =
        super.explainTerms(pw)
            // For rel digest, include the row type since a rendered
            // literal may leave the type ambiguous (e.g. "null").
            .itemIf("type", rowType, pw.getDetailLevel() == SqlExplainLevel.DIGEST_ATTRIBUTES)
            .itemIf("type", rowType.getFieldList(), pw.nest());
    if (pw.nest()) {
      pw.item("tuples", tuples);
    } else {
      pw.item(
          "tuples",
          tuples.stream()
              .map(
                  row ->
                      row.stream()
                          .map(lit -> lit.toString())
                          .collect(Collectors.joining(", ", "{ ", " }")))
              .collect(Collectors.joining(", ", "[", "]")));
    }
    return relWriter;
  }
}
