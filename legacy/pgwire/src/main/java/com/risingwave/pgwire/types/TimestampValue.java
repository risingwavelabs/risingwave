/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package com.risingwave.pgwire.types;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.ResolverStyle;
import java.util.Locale;
import org.jetbrains.annotations.NotNull;

public class TimestampValue implements PgValue {

  // 1st msec where BC date becomes AD date
  private static final long FIRST_MSEC_AFTER_CHRIST = -62135596800000L;

  private static final DateTimeFormatter ISO_FORMATTER =
      new DateTimeFormatterBuilder()
          .parseCaseInsensitive()
          .appendPattern("yyyy-MM-dd HH:mm:ss.SSS+00")
          .toFormatter(Locale.ENGLISH)
          .withResolverStyle(ResolverStyle.STRICT);

  private static final DateTimeFormatter ISO_FORMATTER_WITH_ERA =
      new DateTimeFormatterBuilder()
          .parseCaseInsensitive()
          .append(ISO_FORMATTER)
          .appendLiteral(' ')
          .appendPattern("G")
          .toFormatter(Locale.ENGLISH)
          .withResolverStyle(ResolverStyle.STRICT);

  private final Timestamp ts;

  @Override
  public byte[] encodeInBinary() {
    // Milliseconds since 1970.1.1.
    return ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN).putLong(ts.getTime()).array();
  }

  TimestampValue(@NotNull Timestamp ts) {
    this.ts = ts;
  }

  @Override
  public String encodeInText() {
    long millis = ts.getTime();
    LocalDateTime ts = this.ts.toLocalDateTime();
    if (millis >= FIRST_MSEC_AFTER_CHRIST) {
      return ts.format(ISO_FORMATTER);
    } else {
      return ts.format(ISO_FORMATTER_WITH_ERA);
    }
  }
}
