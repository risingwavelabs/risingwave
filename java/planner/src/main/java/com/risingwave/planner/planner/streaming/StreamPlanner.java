package com.risingwave.planner.planner.streaming;

import static com.risingwave.planner.program.ChainedOptimizerProgram.OptimizerPhase.JOIN_REORDER;
import static com.risingwave.planner.program.ChainedOptimizerProgram.OptimizerPhase.LOGICAL_CBO;
import static com.risingwave.planner.program.ChainedOptimizerProgram.OptimizerPhase.LOGICAL_REWRITE;
import static com.risingwave.planner.program.ChainedOptimizerProgram.OptimizerPhase.STREAMING;
import static com.risingwave.planner.program.ChainedOptimizerProgram.OptimizerPhase.SUBQUERY_REWRITE;
import static com.risingwave.planner.rel.logical.RisingWaveLogicalRel.LOGICAL;
import static com.risingwave.planner.rules.physical.BatchRuleSets.LOGICAL_OPTIMIZATION_RULES;
import static com.risingwave.planner.rules.physical.BatchRuleSets.LOGICAL_REWRITE_RULES;

import com.risingwave.catalog.CatalogService;
import com.risingwave.catalog.TableCatalog;
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
import com.risingwave.planner.rel.streaming.RwStreamChain;
import com.risingwave.planner.rel.streaming.RwStreamMaterializedView;
import com.risingwave.planner.rel.streaming.RwStreamSort;
import com.risingwave.planner.rel.streaming.RwStreamTableSource;
import com.risingwave.planner.rel.streaming.StreamingPlan;
import com.risingwave.planner.rules.physical.BatchRuleSets;
import com.risingwave.planner.rules.streaming.StreamingRuleSets;
import com.risingwave.planner.sql.SqlConverter;
import java.util.List;
import java.util.Objects;
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
    resolveMaterializedViewOnMaterializedView(materializedViewPlan, null, -1, context);
    log.debug("Create streaming plan:\n" + ExplainWriter.explainPlan(materializedViewPlan));
    log.debug(
        "Primary key of Materialized View is:\n" + materializedViewPlan.getPrimaryKeyIndices());
    return materializedViewPlan;
  }

  private void resolveMaterializedViewOnMaterializedView(
      RisingWaveStreamingRel node,
      RisingWaveStreamingRel parent,
      int indexInParent,
      ExecutionContext context) {

    if (node == null) {
      return;
    }

    CatalogService catalogService = context.getCatalogService();

    if (node instanceof RwStreamTableSource) {
      String database = context.getDatabase();
      List<String> qualifiedName = Objects.requireNonNull(node.getTable()).getQualifiedName();
      assert qualifiedName.size() == 2;
      assert node.getInputs().isEmpty();
      TableCatalog source =
          catalogService.getTable(database, qualifiedName.get(0), qualifiedName.get(1));
      if (source != null && source.isMaterializedView()) {
        // source is a materialized view source
        assert parent != null;
        assert indexInParent >= 0;
        RwStreamChain chain =
            new RwStreamChain(
                node.getCluster(),
                node.getTraitSet(),
                ((RwStreamTableSource) node).getHints(),
                node.getTable(),
                ((RwStreamTableSource) node).getTableId());
        parent.replaceInput(indexInParent, chain);
      }
    }

    for (int i = 0; i < node.getInputs().size(); i++) {
      RisingWaveStreamingRel child = (RisingWaveStreamingRel) node.getInput(i);
      resolveMaterializedViewOnMaterializedView(child, node, i, context);
    }
  }

  private RwStreamMaterializedView addMaterializedViewNode(
      RisingWaveStreamingRel root, SqlIdentifier name) {
    var visitor = new PrimaryKeyDerivationVisitor();
    var p = root.accept(visitor);
    if (p.node instanceof RwStreamSort) {
      RwStreamSort sort = (RwStreamSort) p.node;
      // Here RwStreamSort only implements limit/fetch right now, we handle
      // ordering inside RwStreamMaterializedView. There is one case we could
      // do some optimize: when limit and fetch are not provided, we could
      // merge RwStreamSort into RwStreamMaterializedView.
      RelNode input = sort;
      if (sort.offset == null && sort.fetch == null) {
        input = sort.getInput();
      }
      return new RwStreamMaterializedView(
          sort.getCluster(),
          sort.getTraitSet(),
          input,
          name,
          p.info.getPrimaryKeyIndices(),
          sort.getCollation());
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
            .addRules(StreamingRuleSets.LOGICAL_CONVERTER_RULES)
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
