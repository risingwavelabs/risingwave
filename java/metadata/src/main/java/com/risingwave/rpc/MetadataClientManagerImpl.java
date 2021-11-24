package com.risingwave.rpc;

import com.google.inject.Inject;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import javax.inject.Singleton;

/** A simple metadata client manager implementation of {@link MetadataClientManager}; */
@Singleton
public class MetadataClientManagerImpl implements MetadataClientManager {
  private static MetaClient client;

  @Inject
  public MetadataClientManagerImpl() {}

  @Override
  public MetaClient getOrCreate(String host, int port) {
    if (client == null) {
      ManagedChannel channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
      client = new GrpcMetaClient(channel);
    }
    return client;
  }
}
