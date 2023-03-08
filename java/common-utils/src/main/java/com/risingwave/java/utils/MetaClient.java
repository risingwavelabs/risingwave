package com.risingwave.java.utils;

import com.risingwave.proto.*;
import com.risingwave.proto.Catalog.Table;
import com.risingwave.proto.ClusterServiceGrpc.ClusterServiceBlockingStub;
import com.risingwave.proto.Common.HostAddress;
import com.risingwave.proto.Common.WorkerType;
import com.risingwave.proto.DdlServiceGrpc.DdlServiceBlockingStub;
import com.risingwave.proto.DdlServiceOuterClass.GetTableRequest;
import com.risingwave.proto.DdlServiceOuterClass.GetTableResponse;
import com.risingwave.proto.HeartbeatServiceGrpc.HeartbeatServiceBlockingStub;
import com.risingwave.proto.Hummock.HummockVersion;
import com.risingwave.proto.Hummock.PinVersionRequest;
import com.risingwave.proto.Hummock.PinVersionResponse;
import com.risingwave.proto.Hummock.UnpinVersionBeforeRequest;
import com.risingwave.proto.HummockManagerServiceGrpc.HummockManagerServiceBlockingStub;
import com.risingwave.proto.Meta.AddWorkerNodeRequest;
import com.risingwave.proto.Meta.AddWorkerNodeResponse;
import com.risingwave.proto.Meta.HeartbeatRequest;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import java.time.Duration;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class MetaClient implements AutoCloseable {
    private final int workerId;

    private final ManagedChannel channel;

    // Scheduler for background tasks.
    private final ScheduledExecutorService scheduler;

    // RPC stubs.
    private final ClusterServiceBlockingStub clusterStub;
    private final DdlServiceBlockingStub ddlStub;
    private final HeartbeatServiceBlockingStub heartbeatStub;
    private final HummockManagerServiceBlockingStub hummockStub;

    private boolean isClosed;

    // A heart beat task that sends a heartbeat to the meta service when run.
    private class HeartbeatTask implements Runnable {
        Duration timeout;

        HeartbeatTask(Duration timeout) {
            this.timeout = timeout;
        }

        @Override
        public void run() {
            HeartbeatRequest req = HeartbeatRequest.newBuilder().setNodeId(workerId).build();

            try {
                heartbeatStub
                        .withDeadlineAfter(timeout.toMillis(), TimeUnit.MILLISECONDS)
                        .heartbeat(req);
            } catch (Exception e) {
                Logger.getGlobal().warning(String.format("Failed to send heartbeat: %s", e));
            }
        }
    }

    public MetaClient(String metaAddr, ScheduledExecutorService scheduler) {
        this.channel =
                Grpc.newChannelBuilder(metaAddr, InsecureChannelCredentials.create()).build();
        this.scheduler = scheduler;

        this.clusterStub = ClusterServiceGrpc.newBlockingStub(channel);
        this.ddlStub = DdlServiceGrpc.newBlockingStub(channel);
        this.hummockStub = HummockManagerServiceGrpc.newBlockingStub(channel);
        this.heartbeatStub = HeartbeatServiceGrpc.newBlockingStub(channel);

        this.isClosed = false;

        AddWorkerNodeRequest req =
                AddWorkerNodeRequest.newBuilder()
                        .setWorkerType(WorkerType.RISE_CTL)
                        .setHost(
                                HostAddress.newBuilder().setHost("127.0.0.1").setPort(8880).build())
                        .setWorkerNodeParallelism(0)
                        .build();
        AddWorkerNodeResponse resp = clusterStub.addWorkerNode(req);

        this.workerId = resp.getNode().getId();
    }

    public HummockVersion pinVersion() {
        PinVersionRequest req = PinVersionRequest.newBuilder().setContextId(workerId).build();
        PinVersionResponse resp = hummockStub.pinVersion(req);
        return resp.getPinnedVersion();
    }

    public void unpinVersion(HummockVersion version) {
        // TODO: we are calling UnpinBefore in this method. If there are multiple versions being
        // used, unpin using UnpinBefore may accidentally unpin the version used by other thread. We
        // may introduce reference counting in the meta client.
        UnpinVersionBeforeRequest req =
                UnpinVersionBeforeRequest.newBuilder()
                        .setContextId(workerId)
                        .setUnpinVersionBefore(version.getId())
                        .build();
        hummockStub.unpinVersionBefore(req);
    }

    public Table getTable(String databaseName, String tableName) {
        GetTableRequest req =
                GetTableRequest.newBuilder()
                        .setDatabaseName(databaseName)
                        .setTableName(tableName)
                        .build();
        GetTableResponse resp = ddlStub.getTable(req);
        if (resp.hasTable()) {
            return resp.getTable();
        } else {
            return null;
        }
    }

    public ScheduledFuture<?> startHeartbeatLoop(Duration interval) {
        Runnable heartbeatTask = new HeartbeatTask(interval.multipliedBy(3));
        return scheduler.scheduleWithFixedDelay(
                heartbeatTask, interval.toMillis(), interval.toMillis(), TimeUnit.MILLISECONDS);
    }

    @Override
    public void close() {
        if (!isClosed) {
            isClosed = true;
            this.channel.shutdown();
        }
    }
}
