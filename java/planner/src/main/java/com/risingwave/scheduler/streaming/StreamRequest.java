package com.risingwave.scheduler.streaming;

import com.google.common.collect.ImmutableList;
import com.risingwave.node.WorkerNode;
import com.risingwave.proto.streaming.streamnode.BuildFragmentRequest;
import com.risingwave.proto.streaming.streamnode.UpdateFragmentRequest;
import com.risingwave.scheduler.streaming.graph.StreamFragment;
import java.util.ArrayList;
import java.util.List;

/**
 * A collection of requests (currently UpdateFragmentRequest and BuildFragmentRequest) to be sent to
 * one worker when creating fragments.
 */
public class StreamRequest {
  private final WorkerNode workerNode;
  private final ImmutableList<StreamFragment> fragmentList;

  public StreamRequest(WorkerNode workerNode, List<StreamFragment> fragmentList) {
    this.workerNode = workerNode;
    this.fragmentList = ImmutableList.copyOf(fragmentList);
  }

  public WorkerNode getWorkerNode() {
    return workerNode;
  }

  public List<StreamFragment> getFragmentList() {
    return fragmentList;
  }

  public List<Integer> getFragmentIdList() {
    List<Integer> idList = new ArrayList<>();
    for (var fragment : fragmentList) {
      idList.add(fragment.getId());
    }
    return idList;
  }

  public UpdateFragmentRequest serialize(String requestId) {
    UpdateFragmentRequest.Builder builder = UpdateFragmentRequest.newBuilder();
    builder.setRequestId(requestId);
    for (var fragment : fragmentList) {
      builder.addFragment(fragment.serialize());
    }
    return builder.build();
  }

  public BuildFragmentRequest buildRequest(String requestId) {
    BuildFragmentRequest.Builder builder = BuildFragmentRequest.newBuilder();
    builder.setRequestId(requestId);
    for (int fragmentId : getFragmentIdList()) {
      builder.addFragmentId(fragmentId);
    }
    return builder.build();
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  /** The builder class of a <code>StreamRequest</code>. */
  public static class Builder {
    private WorkerNode workerNode;
    private final List<StreamFragment> fragmentList = new ArrayList<>();

    public Builder() {}

    public void setWorkerNode(WorkerNode workerNode) {
      this.workerNode = workerNode;
    }

    public void addStreamFragment(StreamFragment streamFragment) {
      fragmentList.add(streamFragment);
    }

    public StreamRequest build() {
      return new StreamRequest(workerNode, fragmentList);
    }
  }
}
