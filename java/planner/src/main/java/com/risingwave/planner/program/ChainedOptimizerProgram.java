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

public class ChainedOptimizerProgram implements OptimizerProgram {
  private static final Logger LOGGER = LoggerFactory.getLogger(ChainedOptimizerProgram.class);
  private final ImmutableList<OptimizerPhase> phases;
  private final ImmutableMap<OptimizerPhase, OptimizerProgram> optimizers;

  private ChainedOptimizerProgram(
      ImmutableList<OptimizerPhase> phases,
      ImmutableMap<OptimizerPhase, OptimizerProgram> optimizers) {
    this.phases = phases;
    this.optimizers = optimizers;
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
    }

    return result;
  }

  public static Builder builder() {
    return new Builder();
  }

  public enum OptimizerPhase {
    SUBQUERY_REWRITE("Subquery rewrite"),
    BASIC_LOGICAL_OPTIMIZATION("Basic optimization rules"),
    JOIN_REORDER("Join reorder"),
    LOGICAL_CONVERSION("Converting calcite logical plan to rising wave logical plan."),
    LOGICAL_OPTIMIZATION("Optimising logical plan"),
    PHYSICAL("Physical planning");

    private final String description;

    OptimizerPhase(String description) {
      this.description = description;
    }
  }

  public static class Builder {
    private List<OptimizerPhase> phases = new ArrayList<>();
    private Map<OptimizerPhase, OptimizerProgram> optimizers = new HashMap<>();

    public Builder addLast(OptimizerPhase phase, OptimizerProgram program) {
      phases.add(phase);
      optimizers.put(phase, program);
      return this;
    }

    public ChainedOptimizerProgram build() {
      return new ChainedOptimizerProgram(
          ImmutableList.copyOf(phases), ImmutableMap.copyOf(optimizers));
    }
  }
}
