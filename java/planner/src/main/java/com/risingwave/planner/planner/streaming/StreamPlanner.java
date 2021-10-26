package com.risingwave.planner.planner.streaming;

import static com.risingwave.planner.program.ChainedOptimizerProgram.OptimizerPhase.JOIN_REORDER;
import static com.risingwave.planner.program.ChainedOptimizerProgram.OptimizerPhase.LOGICAL_CBO;
import static com.risingwave.planner.program.ChainedOptimizerProgram.OptimizerPhase.LOGICAL_REWRITE;
import static com.risingwave.planner.program.ChainedOptimizerProgram.OptimizerPhase.SUBQUERY_REWRITE;
import static com.risingwave.planner.rel.logical.RisingWaveLogicalRel.LOGICAL;
import static com.risingwave.planner.rules.BatchRuleSets.LOGICAL_CONVERTER_RULES;
import static com.risingwave.planner.rules.BatchRuleSets.LOGICAL_OPTIMIZATION_RULES;
import static com.risingwave.planner.rules.BatchRuleSets.LOGICAL_REWRITE_RULES;

import com.risingwave.execution.context.ExecutionContext;
import com.risingwave.planner.planner.Planner;
import com.risingwave.planner.program.ChainedOptimizerProgram;
import com.risingwave.planner.program.HepOptimizerProgram;
import com.risingwave.planner.program.JoinReorderProgram;
import com.risingwave.planner.program.OptimizerProgram;
import com.risingwave.planner.program.SubQueryRewriteProgram;
import com.risingwave.planner.program.VolcanoOptimizerProgram;
import com.risingwave.planner.rel.physical.streaming.RisingWaveStreamingRel;
import com.risingwave.planner.rel.physical.streaming.RwStreamMaterializedView;
import com.risingwave.planner.rel.physical.streaming.StreamingPlan;
import com.risingwave.planner.rel.serialization.ExplainWriter;
import com.risingwave.planner.rules.BatchRuleSets;
import com.risingwave.planner.rules.streaming.StreamingConvertRules;
import com.risingwave.planner.sql.SqlConverter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.ddl.SqlCreateMaterializedView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The planner for streaming job */
public class StreamPlanner implements Planner<StreamingPlan> {
  private static final Logger log = LoggerFactory.getLogger(StreamPlanner.class);

  public StreamPlanner() {}

  /**
   * The stream planner takes the whole create materialized view AST as input.
   *
   * @param ast the AST of parsed create materialized view statements.
   * @param context the ExecutionContext.
   * @return a StreamingPlan to be later handled in CreateMaterializedViewHandler.
   */
  @Override
  public StreamingPlan plan(SqlNode ast, ExecutionContext context) {
    SqlCreateMaterializedView createMaterializedView = (SqlCreateMaterializedView) ast;
    SqlNode query = createMaterializedView.query;
    SqlConverter sqlConverter = SqlConverter.builder(context).build();
    RelNode rawPlan = sqlConverter.toRel(query).rel;
    // Logical optimization.
    OptimizerProgram optimizerProgram = buildLogicalOptimizerProgram();
    RelNode logicalPlan = optimizerProgram.optimize(rawPlan, context);
    log.debug("Logical plan: \n" + ExplainWriter.explainPlan(logicalPlan));
    // Generate Streaming plan from logical plan.
    RwStreamMaterializedView root = generateStreamingPlan(logicalPlan, context);
    return new StreamingPlan(root);
  }

  private RwStreamMaterializedView generateStreamingPlan(
      RelNode logicalPlan, ExecutionContext context) {
    OptimizerProgram streamingOptimizerProgram = buildStreamingOptimizerProgram();
    RelNode rawStreamingPlan = streamingOptimizerProgram.optimize(logicalPlan, context);
    RwStreamMaterializedView materializedViewPlan = addMaterializedViewNode(rawStreamingPlan);
    log.debug("Create streaming plan:\n" + ExplainWriter.explainPlan(materializedViewPlan));
    return materializedViewPlan;
  }

  private RwStreamMaterializedView addMaterializedViewNode(RelNode root) {
    var rowType = root.getRowType();
    List<RexInputRef> inputRefList = new ArrayList();
    for (int i = 0; i < rowType.getFieldCount(); i++) {
      var field = rowType.getFieldList().get(i);
      RexInputRef inputRef = new RexInputRef(i, field.getType());
      inputRefList.add(inputRef);
    }
    return new RwStreamMaterializedView(
        root.getCluster(),
        root.getTraitSet(),
        Collections.emptyList(),
        root,
        inputRefList,
        root.getRowType());
  }

  private static OptimizerProgram buildLogicalOptimizerProgram() {
    ChainedOptimizerProgram.Builder builder = ChainedOptimizerProgram.builder();
    // We use partial rules from batch planner until getting a RisingWave logical plan.
    builder.addLast(SUBQUERY_REWRITE, SubQueryRewriteProgram.INSTANCE);

    builder.addLast(
        LOGICAL_REWRITE, HepOptimizerProgram.builder().addRules(LOGICAL_REWRITE_RULES).build());

    builder.addLast(JOIN_REORDER, JoinReorderProgram.INSTANCE);

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
        .addRules(StreamingConvertRules.STREAMING_CONVERTER_RULES)
        .addRules(StreamingConvertRules.STREAMING_REMOVE_RULES)
        .addRequiredOutputTraits(RisingWaveStreamingRel.STREAMING)
        .build();
  }
}
