package com.risingwave.pgwire.types;

public interface PgValue {
  /**
   * Encode the value in binary format.
   *
   * @return the binary in big-endian.
   */
  byte[] encodeInBinary();

  String encodeInText();
}
