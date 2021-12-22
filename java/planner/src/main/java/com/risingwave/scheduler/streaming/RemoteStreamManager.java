package com.risingwave.scheduler.streaming;

import com.google.inject.Inject;
import com.risingwave.common.exception.PgErrorCode;
import com.risingwave.common.exception.PgException;
import com.risingwave.node.EndPoint;
import com.risingwave.node.WorkerNode;
import com.risingwave.node.WorkerNodeManager;
import com.risingwave.proto.common.HostAddress;
import com.risingwave.proto.common.Status;
import com.risingwave.proto.metanode.AddFragmentsToNodeRequest;
import com.risingwave.proto.metanode.AddFragmentsToNodeResponse;
import com.risingwave.proto.metanode.FragmentLocation;
import com.risingwave.proto.metanode.GetIdRequest;
import com.risingwave.proto.metanode.GetIdResponse;
import com.risingwave.proto.metanode.LoadAllFragmentsRequest;
import com.risingwave.proto.metanode.LoadAllFragmentsResponse;
import com.risingwave.rpc.MetaClient;
import com.risingwave.scheduler.streaming.graph.StreamFragment;
import com.risingwave.scheduler.streaming.graph.StreamGraph;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The implementation of a stream manager synchronized with meta service. */
public class RemoteStreamManager implements StreamManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(RemoteStreamManager.class);
  private final MetaClient metaClient;
  private final WorkerNodeManager workerNodeManager;
  private final Map<WorkerNode, Set<com.risingwave.proto.streaming.plan.StreamFragment>>
      fragmentAllocation = new HashMap<>();

  // Used for id generation.
  private final int idInterval = 100;
  private int fragmentId = 1;
  private int fragmentIdCap = 1;

  @Inject
  public RemoteStreamManager(MetaClient client, WorkerNodeManager workerNodeManager) {
    this.metaClient = client;
    this.workerNodeManager = workerNodeManager;
    initManager();
  }

  /// Currently, history fragments are not used in stream manager. Keep it for further usage.
  private void initManager() {
    LoadAllFragmentsResponse response =
        metaClient.loadAllFragments(LoadAllFragmentsRequest.newBuilder().build());
    if (response.getStatus().getCode() != Status.Code.OK) {
      throw new PgException(PgErrorCode.INTERNAL_ERROR, "Load all fragment info failed");
    }

    List<WorkerNode> nodeList = workerNodeManager.allNodes();
    Map<HostAddress, Integer> nodes =
        IntStream.range(0, nodeList.size())
            .boxed()
            .collect(
                Collectors.toMap(
                    n -> {
                      EndPoint endPoint = nodeList.get(n).getRpcEndPoint();
                      return HostAddress.newBuilder()
                          .setHost(endPoint.getHost())
                          .setPort(endPoint.getPort())
                          .build();
                    },
                    Function.identity()));
    for (var location : response.getLocationsList()) {
      HostAddress host = location.getNode().getHost();
      if (!nodes.containsKey(host)) {
        LOGGER.warn("invalid host exist in meta service: {}", host);
        continue;
      }

      for (var fragment : location.getFragmentsList()) {
        addFragmentToWorker(nodeList.get(nodes.get(host)), fragment);
      }
    }

    // Init Id allocation variables.
    GetIdRequest idRequest =
        GetIdRequest.newBuilder()
            .setCategory(GetIdRequest.IdCategory.Fragment)
            .setInterval(idInterval)
            .build();
    GetIdResponse idResponse = metaClient.getId(idRequest);
    if (response.getStatus().getCode() != Status.Code.OK) {
      throw new PgException(PgErrorCode.INTERNAL_ERROR, "Get fragment Id failed");
    }
    fragmentId = idResponse.getId();
    fragmentIdCap = fragmentId + idInterval - 1;
  }

  @Override
  public int createFragment() {
    if (fragmentId <= fragmentIdCap) {
      return fragmentId++;
    } else {
      GetIdRequest idRequest =
          GetIdRequest.newBuilder()
              .setCategory(GetIdRequest.IdCategory.Fragment)
              .setInterval(idInterval)
              .build();
      GetIdResponse idResponse = metaClient.getId(idRequest);
      fragmentId = idResponse.getId();
      fragmentIdCap = fragmentId + idInterval - 1;
      return fragmentId;
    }
  }

  @Override
  public List<StreamRequest> scheduleStreamGraph(StreamGraph graph) {
    List<StreamFragment> fragmentList = graph.getAllFragments();

    List<WorkerNode> nodeList = workerNodeManager.allNodes();
    if (nodeList.size() != 1) {
      throw new PgException(
          PgErrorCode.INTERNAL_ERROR, "Support only one worker node for streaming now.");
    }
    // An ugly scheduling algorithm for 1 worker: put everything on the node0.
    WorkerNode node0 = nodeList.get(0);
    for (var fragment : fragmentList) {
      addFragmentToWorker(node0, fragment.serialize());
    }

    // Build returning list.
    List<StreamRequest> requestList = new ArrayList<>();
    Map<Integer, StreamFragment> relevantIdMap =
        fragmentList.stream().collect(Collectors.toMap(StreamFragment::getId, f -> f));
    for (WorkerNode node : fragmentAllocation.keySet()) {
      FragmentLocation.Builder locationBuilder = FragmentLocation.newBuilder();
      HostAddress.Builder addressBuilder = HostAddress.newBuilder();
      addressBuilder.setHost(node.getRpcEndPoint().getHost());
      addressBuilder.setPort(node.getRpcEndPoint().getPort());
      com.risingwave.proto.common.WorkerNode.Builder workerNodeBuilder =
          com.risingwave.proto.common.WorkerNode.newBuilder();
      workerNodeBuilder.setHost(addressBuilder);
      locationBuilder.setNode(workerNodeBuilder);

      StreamRequest.Builder builder = StreamRequest.newBuilder();
      builder.setWorkerNode(node);
      for (var fragment : fragmentAllocation.get(node)) {
        if (relevantIdMap.containsKey(fragment.getFragmentId())) {
          builder.addStreamFragment(relevantIdMap.get(fragment.getFragmentId()));
          locationBuilder.addFragments(fragment);
        }
      }
      requestList.add(builder.build());

      /* FIXME: if failed, some dirty data will left in memory. The situation is same with catalog.
       *   This be will handled together in the future (or keep it until rewrite frontend in Rust, 23333). */
      AddFragmentsToNodeResponse response =
          metaClient.addFragmentsToNode(
              AddFragmentsToNodeRequest.newBuilder().setLocation(locationBuilder).build());
      if (response.getStatus().getCode() != Status.Code.OK) {
        throw new PgException(PgErrorCode.INTERNAL_ERROR, "Add fragment to worker failed");
      }
    }
    return requestList;
  }

  private void addFragmentToWorker(
      WorkerNode node, com.risingwave.proto.streaming.plan.StreamFragment fragment) {
    LOGGER.debug(
        "add fragment {} to worker {}", fragment.getFragmentId(), node.getRpcEndPoint().toString());
    if (fragmentAllocation.containsKey(node)) {
      fragmentAllocation.get(node).add(fragment);
    } else {
      Set<com.risingwave.proto.streaming.plan.StreamFragment> fragmentSet = new HashSet<>();
      fragmentSet.add(fragment);
      fragmentAllocation.put(node, fragmentSet);
    }
  }

  @Override
  public String nextScheduleId() {
    return UUID.randomUUID().toString();
  }

  @Override
  public ActorInfoTable getActorInfo(List<Integer> actorIdList) {
    HashSet<Integer> relevantIdSet = new HashSet<>();
    relevantIdSet.addAll(actorIdList);

    ActorInfoTable.Builder builder = ActorInfoTable.newBuilder();
    for (var node : fragmentAllocation.keySet()) {
      Set<Integer> fragmentIdSet = new HashSet<Integer>();
      for (var fragment : fragmentAllocation.get(node)) {
        if (relevantIdSet.contains(fragment.getFragmentId())) {
          fragmentIdSet.add(fragment.getFragmentId());
        }
      }
      builder.addWorkerNode(node, fragmentIdSet);
    }
    return builder.build();
  }
}
