package com.risingwave.execution.handler.cache;

/** HummockSnapshotManager maintains hummock snapshots. */
public interface HummockSnapshotManager {
  long pinAndGetSnapshot();

  void unpinSnapshot(long epoch);

  void force_update();

  default ScopedSnapshot getScopedSnapshot() {
    return new ScopedSnapshot(this);
  }
}
