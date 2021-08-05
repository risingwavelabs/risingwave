package com.risingwave.scheduler.query;

import static java.util.Objects.requireNonNull;

import com.risingwave.scheduler.stage.StageEvent;

public interface QueryExecutionEvent {
  class StartEvent implements QueryExecutionEvent {}

  class StageStatusChangeEvent implements QueryExecutionEvent {
    private final StageEvent stageEvent;

    public StageStatusChangeEvent(StageEvent stageEvent) {
      this.stageEvent = requireNonNull(stageEvent, "stageEvent");
    }

    public StageEvent getStageEvent() {
      return stageEvent;
    }
  }
}
