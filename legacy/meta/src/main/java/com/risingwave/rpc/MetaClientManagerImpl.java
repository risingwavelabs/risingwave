package com.risingwave.rpc;

import com.google.inject.Inject;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import javax.inject.Singleton;

/** A simple meta client manager implementation of {@link MetaClientManager}; */
@Singleton
public class MetaClientManagerImpl implements MetaClientManager {
  private static MetaClient client;

  @Inject
  public MetaClientManagerImpl() {}

  @Override
  public MetaClient getOrCreate(String host, int port) {
    if (client == null) {
      ManagedChannel channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
      client = new GrpcMetaClient(channel);
    }
    return client;
  }
}
