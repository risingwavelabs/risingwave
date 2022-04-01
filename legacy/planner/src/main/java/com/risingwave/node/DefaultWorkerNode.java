package com.risingwave.node;

import com.risingwave.common.exception.PgErrorCode;
import com.risingwave.common.exception.PgException;

public class DefaultWorkerNode implements WorkerNode {
  private final String host;
  private final int port;

  public DefaultWorkerNode(String host, int port) {
    this.host = host;
    this.port = port;
  }

  @Override
  public EndPoint getRpcEndPoint() {
    return new EndPoint(host, port);
  }

  public static DefaultWorkerNode from(String address) {
    String[] components = address.split(":");
    if (components.length != 2) {
      throw new PgException(
          PgErrorCode.INTERNAL_ERROR, "Invalid computer node address: %s", address);
    }

    int port = Integer.parseInt(components[1]);
    return new DefaultWorkerNode(components[0], port);
  }
}
