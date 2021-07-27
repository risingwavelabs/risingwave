package com.risingwave.planner.program;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.risingwave.common.error.PlannerError;
import com.risingwave.common.exception.RisingWaveException;
import com.risingwave.common.util.Utils;
import com.risingwave.execution.context.ExecutionContext;
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
      LOGGER.atDebug().addArgument(phase).log("Begin to optimize in {} phase.");
      try {
        result =
            Utils.runWithTime(
                () -> program.optimize(current, context),
                nanoSeconds ->
                    LOGGER
                        .atDebug()
                        .addArgument(phase)
                        .addArgument(nanoSeconds)
                        .log("Optimizer phase {} took {} nanoseconds."));
      } catch (Exception e) {
        LOGGER.atError().addArgument(phase).log("Failed to optimize plan at phase {}.", e);
        throw RisingWaveException.from(PlannerError.INTERNAL, e);
      }
    }

    return result;
  }

  public static Builder builder() {
    return new Builder();
  }

  public enum OptimizerPhase {
    CALCITE_LOGICAL_OPTIMIZATION("Logical optimization rules from calcite"),
    LOGICAL_CONVERSION("Converting calcite logical plan to rising wave logical plan."),
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
