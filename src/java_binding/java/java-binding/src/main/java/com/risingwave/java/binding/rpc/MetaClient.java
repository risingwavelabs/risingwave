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
import com.risingwave.proto.Hummock.HummockVersion;
import com.risingwave.proto.Hummock.JavaPinVersionRequest;
import com.risingwave.proto.Hummock.JavaPinVersionResponse;
import com.risingwave.proto.HummockManagerServiceGrpc;
import com.risingwave.proto.HummockManagerServiceGrpc.HummockManagerServiceBlockingStub;
import com.risingwave.proto.Meta.AddWorkerNodeRequest;
import com.risingwave.proto.Meta.AddWorkerNodeResponse;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;

public class MetaClient implements AutoCloseable {
    final int workerId;

    final ManagedChannel channel;

    final ClusterServiceBlockingStub clusterStub;
    final DdlServiceBlockingStub ddlStub;
    final HummockManagerServiceBlockingStub hummockStub;

    public MetaClient(String metaAddr) {
        this.channel =
                Grpc.newChannelBuilder(metaAddr, InsecureChannelCredentials.create()).build();

        this.clusterStub = ClusterServiceGrpc.newBlockingStub(channel);
        this.ddlStub = DdlServiceGrpc.newBlockingStub(channel);
        this.hummockStub = HummockManagerServiceGrpc.newBlockingStub(channel);

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
        JavaPinVersionRequest req =
                JavaPinVersionRequest.newBuilder().setContextId(workerId).build();
        JavaPinVersionResponse resp = hummockStub.javaPinVersion(req);
        return resp.getPinnedVersion();
    }

    public Table getTable(int tableId) {
        JavaGetTableRequest req = JavaGetTableRequest.newBuilder().setTableId(tableId).build();
        JavaGetTableResponse resp = ddlStub.javaGetTable(req);
        return resp.getTable();
    }

    @Override
    public void close() {
        this.channel.shutdown();
    }
}
