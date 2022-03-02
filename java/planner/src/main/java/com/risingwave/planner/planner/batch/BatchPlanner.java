package com.risingwave.planner.planner.batch;

import static com.risingwave.planner.program.ChainedOptimizerProgram.OptimizerPhase;
import static com.risingwave.planner.program.ChainedOptimizerProgram.OptimizerPhase.DISTRIBUTED;
import static com.risingwave.planner.program.ChainedOptimizerProgram.OptimizerPhase.JOIN_REORDER;
import static com.risingwave.planner.program.ChainedOptimizerProgram.OptimizerPhase.LOGICAL_CBO;
import static com.risingwave.planner.program.ChainedOptimizerProgram.OptimizerPhase.LOGICAL_REWRITE;
import static com.risingwave.planner.program.ChainedOptimizerProgram.OptimizerPhase.PHYSICAL;
import static com.risingwave.planner.program.ChainedOptimizerProgram.OptimizerPhase.SUBQUERY_REWRITE;
import static com.risingwave.planner.rel.logical.RisingWaveLogicalRel.LOGICAL;
import static com.risingwave.planner.rules.physical.BatchRuleSets.DISTRIBUTED_CONVERTER_RULES;
import static com.risingwave.planner.rules.physical.BatchRuleSets.DISTRIBUTION_RULES;
import static com.risingwave.planner.rules.physical.BatchRuleSets.LOGICAL_CONVERTER_RULES;
import static com.risingwave.planner.rules.physical.BatchRuleSets.LOGICAL_OPTIMIZATION_RULES;
import static com.risingwave.planner.rules.physical.BatchRuleSets.LOGICAL_REWRITE_RULES;
import static com.risingwave.planner.rules.physical.BatchRuleSets.PHYSICAL_CONVERTER_RULES;

import com.risingwave.execution.context.ExecutionContext;
import com.risingwave.planner.planner.Planner;
import com.risingwave.planner.program.ChainedOptimizerProgram;
import com.risingwave.planner.program.HepOptimizerProgram;
import com.risingwave.planner.program.JoinReorderProgram;
import com.risingwave.planner.program.OptimizerProgram;
import com.risingwave.planner.program.SubQueryRewriteProgram;
import com.risingwave.planner.program.VolcanoOptimizerProgram;
import com.risingwave.planner.rel.common.dist.RwDistributions;
import com.risingwave.planner.rel.physical.BatchPlan;
import com.risingwave.planner.rel.physical.RisingWaveBatchPhyRel;
import com.risingwave.planner.rel.physical.RwBatchProject;
import com.risingwave.planner.rel.serialization.ExplainWriter;
import com.risingwave.planner.rules.physical.BatchRuleSets;
import com.risingwave.planner.sql.SqlConverter;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.sql.SqlNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Planner for batch query */
public class BatchPlanner implements Planner<BatchPlan> {
  private static final Logger log = LoggerFactory.getLogger(BatchPlanner.class);

  public BatchPlanner() {}

  @Override
  public BatchPlan plan(SqlNode ast, ExecutionContext context) {
    return planDistributed(ast, context);
  }

  public RelNode plan(SqlNode ast, ExecutionContext context, Function<RelCollation, OptimizerProgram> optimizerProgramProvider) {
    SqlConverter sqlConverter = SqlConverter.builder(context).build();
    RelRoot rawRoot = sqlConverter.toRel(ast);
    OptimizerProgram optimizerProgram = optimizerProgramProvider.apply(rawRoot.collation);
    RelNode optimized = optimizerProgram.optimize(rawRoot.rel, context);
    if (!rawRoot.isRefTrivial()) {
      LogicalProject proj = (LogicalProject) rawRoot.withRel(optimized).project(true);
      optimized =
          new RwBatchProject(
              proj.getCluster(),
              proj.getTraitSet().plus(optimized.getConvention()),
              proj.getHints(),
              optimized,
              proj.getProjects(),
              proj.getRowType());
    }
    return optimized;
  }

  public RelNode planLogical(SqlNode ast, ExecutionContext context) {
    RelNode result = plan(ast, context, relCollation -> buildOptimizerProgram(LOGICAL_CBO, relCollation));
    log.info("Create logical plan:\n {}", ExplainWriter.explainPlan(result));
    return result;
  }

  public BatchPlan planPhysical(SqlNode ast, ExecutionContext context) {
    RelNode result = plan(ast, context, relCollation -> buildOptimizerProgram(PHYSICAL, relCollation));
    RisingWaveBatchPhyRel root = (RisingWaveBatchPhyRel) result;
    log.debug("Create physical plan:\n {}", ExplainWriter.explainPlan(root));
    return new BatchPlan(root);
  }

  public BatchPlan planDistributed(SqlNode ast, ExecutionContext context) {
    RelNode result = plan(ast, context, relCollation -> buildOptimizerProgram(DISTRIBUTED, relCollation));
    RisingWaveBatchPhyRel root = (RisingWaveBatchPhyRel) result;
    log.debug("Create distributed plan:\n {}", ExplainWriter.explainPlan(root));
    return new BatchPlan(root);
  }

  private static OptimizerProgram buildOptimizerProgram(
      OptimizerPhase optimizeLevel, RelCollation requiredCollation) {
    ChainedOptimizerProgram.Builder builder = ChainedOptimizerProgram.builder(optimizeLevel);

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

    var physical =
        VolcanoOptimizerProgram.builder()
            .addRules(PHYSICAL_CONVERTER_RULES)
            .addRequiredOutputTraits(RisingWaveBatchPhyRel.BATCH_PHYSICAL)
            .addRequiredOutputTraits(requiredCollation)
            .setTopDownOpt(true);
    builder.addLast(PHYSICAL, physical.build());

    var distributed =
        VolcanoOptimizerProgram.builder()
            .addRules(DISTRIBUTED_CONVERTER_RULES)
            .addRules(DISTRIBUTION_RULES)
            .addRequiredOutputTraits(requiredCollation)
            .addRequiredOutputTraits(RisingWaveBatchPhyRel.BATCH_DISTRIBUTED)
            .addRequiredOutputTraits(RwDistributions.SINGLETON)
            .setTopDownOpt(true);

    builder.addLast(DISTRIBUTED, distributed.build());

    return builder.build();
  }
}
