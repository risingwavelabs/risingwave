package com.risingwave.execution.handler.cache;

import com.google.inject.Inject;

/** A mock implementation for HummockSnapshotManager. */
public class MockHummockSnapshotManager implements HummockSnapshotManager {
  @Inject
  public MockHummockSnapshotManager() {}

  @Override
  public long pinAndGetSnapshot() {
    return 0;
  }

  @Override
  public void unpinSnapshot(long epoch) {}

  @Override
  public void forceUpdate() {}
}
