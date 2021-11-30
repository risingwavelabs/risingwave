package com.risingwave.planner.planner.streaming;

import static com.risingwave.planner.program.ChainedOptimizerProgram.OptimizerPhase.JOIN_REORDER;
import static com.risingwave.planner.program.ChainedOptimizerProgram.OptimizerPhase.LOGICAL_CBO;
import static com.risingwave.planner.program.ChainedOptimizerProgram.OptimizerPhase.LOGICAL_REWRITE;
import static com.risingwave.planner.program.ChainedOptimizerProgram.OptimizerPhase.STREAMING;
import static com.risingwave.planner.program.ChainedOptimizerProgram.OptimizerPhase.SUBQUERY_REWRITE;
import static com.risingwave.planner.rel.logical.RisingWaveLogicalRel.LOGICAL;
import static com.risingwave.planner.rules.physical.BatchRuleSets.LOGICAL_CONVERTER_RULES;
import static com.risingwave.planner.rules.physical.BatchRuleSets.LOGICAL_OPTIMIZATION_RULES;
import static com.risingwave.planner.rules.physical.BatchRuleSets.LOGICAL_REWRITE_RULES;

import com.risingwave.execution.context.ExecutionContext;
import com.risingwave.planner.planner.Planner;
import com.risingwave.planner.program.ChainedOptimizerProgram;
import com.risingwave.planner.program.HepOptimizerProgram;
import com.risingwave.planner.program.JoinReorderProgram;
import com.risingwave.planner.program.OptimizerProgram;
import com.risingwave.planner.program.SubQueryRewriteProgram;
import com.risingwave.planner.program.VolcanoOptimizerProgram;
import com.risingwave.planner.rel.serialization.ExplainWriter;
import com.risingwave.planner.rel.streaming.PrimaryKeyDerivationVisitor;
import com.risingwave.planner.rel.streaming.RisingWaveStreamingRel;
import com.risingwave.planner.rel.streaming.RwStreamMaterializedView;
import com.risingwave.planner.rel.streaming.RwStreamSort;
import com.risingwave.planner.rel.streaming.StreamingPlan;
import com.risingwave.planner.rules.physical.BatchRuleSets;
import com.risingwave.planner.rules.streaming.StreamingRuleSets;
import com.risingwave.planner.sql.SqlConverter;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.ddl.SqlCreateMaterializedView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The planner for streaming job */
public class StreamPlanner implements Planner<StreamingPlan> {
  private static final Logger log = LoggerFactory.getLogger(StreamPlanner.class);

  // TODO: need finer-grained control to enable or disable certain rules
  // or optimizer programs for tests.
  private final boolean testMode;

  public StreamPlanner() {
    this(false);
  }

  public StreamPlanner(boolean testMode) {
    this.testMode = testMode;
  }

  /**
   * The stream planner takes the whole create materialized view AST as input.
   *
   * @param ast the AST of parsed create materialized view statements.
   * @param context the ExecutionContext.
   * @return a StreamingPlan to be later handled in CreateMaterializedViewHandler.
   */
  @Override
  public StreamingPlan plan(SqlNode ast, ExecutionContext context) {
    SqlCreateMaterializedView create = (SqlCreateMaterializedView) ast;
    SqlConverter sqlConverter = SqlConverter.builder(context).build();
    RelNode rawPlan = sqlConverter.toRel(create.query).rel;
    // Logical optimization.
    OptimizerProgram optimizerProgram = buildLogicalOptimizerProgram();
    RelNode logicalPlan = optimizerProgram.optimize(rawPlan, context);
    log.debug("Logical plan: \n" + ExplainWriter.explainPlan(logicalPlan));
    // Generate Streaming plan from logical plan.
    RwStreamMaterializedView root = generateStreamingPlan(logicalPlan, context, create.name);
    return new StreamingPlan(root);
  }

  private RwStreamMaterializedView generateStreamingPlan(
      RelNode logicalPlan, ExecutionContext context, SqlIdentifier name) {
    OptimizerProgram program = buildStreamingOptimizerProgram();
    RisingWaveStreamingRel rawPlan =
        (RisingWaveStreamingRel) program.optimize(logicalPlan, context);
    log.debug("Before adding Materialized View, the plan:\n" + ExplainWriter.explainPlan(rawPlan));
    RwStreamMaterializedView materializedViewPlan = addMaterializedViewNode(rawPlan, name);
    log.debug("Create streaming plan:\n" + ExplainWriter.explainPlan(materializedViewPlan));
    return materializedViewPlan;
  }

  private RwStreamMaterializedView addMaterializedViewNode(
      RisingWaveStreamingRel root, SqlIdentifier name) {
    var visitor = new PrimaryKeyDerivationVisitor();
    var p = root.accept(visitor);
    // There are two cases:
    // 1. Sort/Limit/TopN is the root. We merge them into the new RwStreamMaterializedView
    // 2. Otherwise, we just add a RwStreamMaterializedView.
    if (p.node instanceof RwStreamSort) {
      RwStreamSort sort = (RwStreamSort) p.node;
      return new RwStreamMaterializedView(
          sort.getCluster(),
          sort.getTraitSet(),
          sort.getInput(),
          name,
          p.info.getPrimaryKeyIndices(),
          sort.getCollation(),
          sort.offset,
          sort.fetch);
    } else {
      return new RwStreamMaterializedView(
          p.node.getCluster(), p.node.getTraitSet(), p.node, name, p.info.getPrimaryKeyIndices());
    }
  }

  private OptimizerProgram buildLogicalOptimizerProgram() {
    ChainedOptimizerProgram.Builder builder = ChainedOptimizerProgram.builder(STREAMING);
    // We use partial rules from batch planner until getting a RisingWave logical plan.
    builder.addLast(SUBQUERY_REWRITE, SubQueryRewriteProgram.INSTANCE);

    builder.addLast(
        LOGICAL_REWRITE, HepOptimizerProgram.builder().addRules(LOGICAL_REWRITE_RULES).build());

    if (!this.testMode) {
      builder.addLast(JOIN_REORDER, JoinReorderProgram.INSTANCE);
    }

    builder.addLast(
        LOGICAL_CBO,
        VolcanoOptimizerProgram.builder()
            .addRules(BatchRuleSets.LOGICAL_OPTIMIZE_RULES)
            .addRules(LOGICAL_CONVERTER_RULES)
            .addRules(LOGICAL_OPTIMIZATION_RULES)
            .addRequiredOutputTraits(LOGICAL)
            .build());

    return builder.build();
  }

  private static OptimizerProgram buildStreamingOptimizerProgram() {
    return VolcanoOptimizerProgram.builder()
        .addRules(StreamingRuleSets.STREAMING_CONVERTER_RULES)
        .addRules(StreamingRuleSets.STREAMING_AGG_RULES)
        .addRules(StreamingRuleSets.STREAMING_REMOVE_RULES)
        .addRequiredOutputTraits(RisingWaveStreamingRel.STREAMING)
        .build();
  }
}
