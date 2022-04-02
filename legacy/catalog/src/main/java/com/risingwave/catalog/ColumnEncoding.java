package com.risingwave.catalog;

public enum ColumnEncoding {
  INVALID,
  RAW,
  LZO,
  LZ4,
  ZSTD,
  RUN_LENGTH,
  BYTE_DICT,
  DELTA
}
