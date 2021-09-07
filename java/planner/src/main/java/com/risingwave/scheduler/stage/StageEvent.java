package com.risingwave.scheduler.stage;

public interface StageEvent {
  StageId getStageId();

  abstract class StageEventBase implements StageEvent {
    private final StageId stageId;

    protected StageEventBase(StageId stageId) {
      this.stageId = stageId;
    }

    @Override
    public StageId getStageId() {
      return stageId;
    }
  }

  class StageScheduledEvent extends StageEventBase {
    private final ScheduledStage stageInfo;

    protected StageScheduledEvent(StageId stageId, ScheduledStage stage) {
      super(stageId);
      this.stageInfo = stage;
    }

    public ScheduledStage getStageInfo() {
      return stageInfo;
    }
  }
}
