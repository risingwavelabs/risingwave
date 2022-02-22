package com.risingwave.execution.handler.cache;

import com.google.inject.Inject;
import com.risingwave.common.exception.PgErrorCode;
import com.risingwave.common.exception.PgException;
import com.risingwave.proto.hummock.HummockSnapshot;
import com.risingwave.proto.hummock.PinSnapshotRequest;
import com.risingwave.proto.hummock.UnpinSnapshotRequest;
import com.risingwave.rpc.MetaClient;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A simple manager that caches hummock snapshots and sync them with meta server periodically. */
@Singleton
public class HummockSnapshotManagerImpl implements HummockSnapshotManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(HummockSnapshotManagerImpl.class);

  private final MetaClient metaClient;
  private final TreeMap<Long, Integer> referenceCounts;
  private final ExecutorService unpinWorker;
  private final int workerNodeId;

  @Inject
  public HummockSnapshotManagerImpl(MetaClient metaClient) {
    this.metaClient = metaClient;
    this.referenceCounts = new TreeMap<>(new EpochComparator().reversed());
    // TODO: use correct worker_node_id
    this.workerNodeId = 0;
    // Start a background worker to fetch the latest snapshot from meta periodically.
    ScheduledExecutorService pinWorker = Executors.newSingleThreadScheduledExecutor();
    pinWorker.scheduleWithFixedDelay(
        () -> {
          try {
            this.force_update();
          } catch (Exception e) {
            LOGGER.warn("Likely meta service is unreachable, retry after a while.");
            try {
              TimeUnit.MILLISECONDS.sleep(1000);
            } catch (InterruptedException ex) {
              Thread.currentThread().interrupt();
            }
          }
        },
        100,
        100,
        TimeUnit.MILLISECONDS);
    // Start a background worker to release snapshots in meta, which are neither referenced nor the
    // latest locally.
    this.unpinWorker = Executors.newSingleThreadExecutor();
  }

  @Override
  public long pinAndGetSnapshot() {
    synchronized (referenceCounts) {
      if (referenceCounts.isEmpty()) {
        // The background worker hasn't fetched any snapshot yet.
        throw new PgException(PgErrorCode.INTERNAL_ERROR, "Get a hummock snapshot failed.");
      }
      return referenceCounts.firstKey();
    }
  }

  @Override
  public void unpinSnapshot(long epoch) {
    synchronized (referenceCounts) {
      referenceCounts.computeIfPresent(
          epoch,
          (keyEpoch, refCount) -> {
            refCount = Math.max(refCount - 1, 0);
            if (refCount == 0 && !keyEpoch.equals(referenceCounts.firstKey())) {
              this.unpinWorker.submit(
                  () -> {
                    var snapshot = HummockSnapshot.newBuilder().setEpoch(keyEpoch).build();
                    var request = UnpinSnapshotRequest.newBuilder().setSnapshot(snapshot).build();
                    this.metaClient.unpinSnapshot(request);
                  });
              return null;
            }
            return refCount;
          });
    }
  }

  @Override
  public void force_update() {
    // The fetched epoch is guaranteed to be GE than the cached ones.
    long epoch =
        this.metaClient
            .pinSnapshot(PinSnapshotRequest.newBuilder().setContextId(this.workerNodeId).build())
            .getSnapshot()
            .getEpoch();
    synchronized (referenceCounts) {
      referenceCounts.putIfAbsent(epoch, 0);
    }
  }
}
