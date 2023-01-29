package com.risingwave.java.binding.rpc;

import com.risingwave.proto.Catalog.Table;
import com.risingwave.proto.ClusterServiceGrpc;
import com.risingwave.proto.ClusterServiceGrpc.ClusterServiceBlockingStub;
import com.risingwave.proto.Common.HostAddress;
import com.risingwave.proto.Common.WorkerType;
import com.risingwave.proto.DdlServiceGrpc;
import com.risingwave.proto.DdlServiceGrpc.DdlServiceBlockingStub;
import com.risingwave.proto.DdlServiceOuterClass.JavaGetTableRequest;
import com.risingwave.proto.DdlServiceOuterClass.JavaGetTableResponse;
import com.risingwave.proto.HeartbeatServiceGrpc;
import com.risingwave.proto.HeartbeatServiceGrpc.HeartbeatServiceBlockingStub;
import com.risingwave.proto.Hummock.HummockVersion;
import com.risingwave.proto.Hummock.PinVersionRequest;
import com.risingwave.proto.Hummock.PinVersionResponse;
import com.risingwave.proto.HummockManagerServiceGrpc;
import com.risingwave.proto.HummockManagerServiceGrpc.HummockManagerServiceBlockingStub;
import com.risingwave.proto.Meta.AddWorkerNodeRequest;
import com.risingwave.proto.Meta.AddWorkerNodeResponse;
import com.risingwave.proto.Meta.HeartbeatRequest;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import java.time.Duration;
import java.time.Instant;
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
        Instant lastHeartbeatSent;
        Duration timeout;

        HeartbeatTask(Duration timeout) {
            this.lastHeartbeatSent = Instant.now();
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

            Instant now = Instant.now();
            if (Duration.between(lastHeartbeatSent, Instant.now()).compareTo(timeout) > 0) {
                Logger.getGlobal().warning("Heartbeat timeout, exiting...");
                System.exit(1);
            }
            lastHeartbeatSent = now;
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

    public Table getTable(String databaseName, String tableName) {
        JavaGetTableRequest req =
                JavaGetTableRequest.newBuilder()
                        .setDatabaseName(databaseName)
                        .setTableName(tableName)
                        .build();
        JavaGetTableResponse resp = ddlStub.javaGetTable(req);
        return resp.getTable();
    }

    public ScheduledFuture<?> startHeartbeatLoop(Duration minInterval, Duration maxInterval) {
        Runnable heartbeatTask = new HeartbeatTask(maxInterval);
        return scheduler.scheduleWithFixedDelay(
                heartbeatTask,
                minInterval.toMillis(),
                minInterval.toMillis(),
                TimeUnit.MILLISECONDS);
    }

    @Override
    public void close() {
        if (!isClosed) {
            isClosed = true;
            this.channel.shutdown();
        }
    }
}
