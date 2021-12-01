package com.risingwave.rpc;

/**
 * This class manages a pool of RPC client. <br>
 * Implementation should be able to reuse the channel if exists.
 */
public interface MetaClientManager {
  MetaClient getOrCreate(String host, int port);
}
