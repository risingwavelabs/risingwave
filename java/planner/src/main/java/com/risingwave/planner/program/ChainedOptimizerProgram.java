package com.risingwave.planner.program;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.risingwave.common.error.PlannerError;
import com.risingwave.common.exception.RisingWaveException;
import com.risingwave.common.util.Utils;
import com.risingwave.execution.context.ExecutionContext;
import com.risingwave.planner.rel.serialization.ExplainWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.rel.RelNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** chain OptimizerPrograms together, and do them one by one */
public class ChainedOptimizerProgram implements OptimizerProgram {
  private static final Logger LOGGER = LoggerFactory.getLogger(ChainedOptimizerProgram.class);
  private final ImmutableList<OptimizerPhase> phases;
  private final ImmutableMap<OptimizerPhase, OptimizerProgram> optimizers;
  private OptimizerPhase optimizeLevel;

  private ChainedOptimizerProgram(
      ImmutableList<OptimizerPhase> phases,
      ImmutableMap<OptimizerPhase, OptimizerProgram> optimizers,
      OptimizerPhase optimizeLevel) {
    this.phases = phases;
    this.optimizers = optimizers;
    this.optimizeLevel = optimizeLevel;
  }

  @Override
  public RelNode optimize(RelNode root, ExecutionContext context) {
    checkNotNull(root, "Root can't be null!");
    RelNode result = root;
    for (OptimizerPhase phase : phases) {
      OptimizerProgram program = optimizers.get(phase);
      RelNode current = result;
      LOGGER.debug("Begin to optimize in {} phase.", phase);
      try {
        result =
            Utils.runWithTime(
                () -> program.optimize(current, context),
                nanoSeconds ->
                    LOGGER.debug("Optimizer phase {} took {} nanoseconds.", phase, nanoSeconds));
        LOGGER.debug("Plan after phase {}: \n{}", phase, ExplainWriter.explainPlan(result));
      } catch (Exception e) {
        LOGGER.error("Failed to optimize plan at phase {}.", phase, e);
        throw RisingWaveException.from(PlannerError.INTERNAL, e);
      }
      if (this.optimizeLevel == phase) {
        return result;
      }
    }
    return result;
  }

  public static Builder builder(OptimizerPhase optimizeLevel) {
    return new Builder(optimizeLevel);
  }

  /** OptimizerPhase in the chain */
  public enum OptimizerPhase {
    SUBQUERY_REWRITE("Subquery rewrite"),
    LOGICAL_REWRITE("Logical rewrite"),
    JOIN_REORDER("Join reorder"),
    LOGICAL_CBO("CBO for logical optimization"),
    PHYSICAL("Physical planning"),
    DISTRIBUTED("distributed planning"),
    STREAMING("Streaming fragment generation");

    private final String description;

    OptimizerPhase(String description) {
      this.description = description;
    }
  }

  /** Builder of ChainedOptimizerProgram */
  public static class Builder {
    private List<OptimizerPhase> phases = new ArrayList<>();
    private Map<OptimizerPhase, OptimizerProgram> optimizers = new HashMap<>();
    private OptimizerPhase optimizeLevel;

    public Builder(OptimizerPhase optimizeLevel) {
      this.optimizeLevel = optimizeLevel;
    }

    public Builder addLast(OptimizerPhase phase, OptimizerProgram program) {
      phases.add(phase);
      optimizers.put(phase, program);
      return this;
    }

    public ChainedOptimizerProgram build() {
      return new ChainedOptimizerProgram(
          ImmutableList.copyOf(phases), ImmutableMap.copyOf(optimizers), this.optimizeLevel);
    }
  }
}
