package com.risingwave.node;

import static java.util.Objects.requireNonNull;

import com.google.common.base.Objects;

public class EndPoint {
  private final String host;
  private final int port;

  public EndPoint(String host, int port) {
    this.host = requireNonNull(host, "host");
    this.port = port;
  }

  public String getHost() {
    return host;
  }

  public int getPort() {
    return port;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    EndPoint endPoint = (EndPoint) o;
    return port == endPoint.port && Objects.equal(host, endPoint.host);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(host, port);
  }

  @Override
  public String toString() {
    return "EndPoint{" + "host='" + host + '\'' + ", port=" + port + '}';
  }
}
