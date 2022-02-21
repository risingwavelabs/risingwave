package com.risingwave.execution.handler.cache;

/** A snapshot wrapper for try-with-resources */
public class ScopedSnapshot implements AutoCloseable {
  private final Long epoch;
  private final HummockSnapshotManager hummockSnapshotManager;

  public ScopedSnapshot(HummockSnapshotManager hummockSnapshotManager) {
    this.epoch = hummockSnapshotManager.pinAndGetSnapshot();
    this.hummockSnapshotManager = hummockSnapshotManager;
  }

  public long getEpoch() {
    return epoch;
  }

  @Override
  public void close() throws Exception {
    if (this.epoch == null) {
      return;
    }
    this.hummockSnapshotManager.unpinSnapshot(this.epoch);
  }
}
