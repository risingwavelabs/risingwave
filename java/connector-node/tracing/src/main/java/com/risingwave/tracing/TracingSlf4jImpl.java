// Copyright 2024 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Ported from https://github.com/MrFriendly-B-V/tracing-slf4j,
// which is licensed under the Apache License, Version 2.0.

package com.risingwave.tracing;

import com.risingwave.java.binding.Binding;

public class TracingSlf4jImpl {
  public static final int ERROR = 0;
  public static final int WARN = 1;
  public static final int INFO = 2;
  public static final int DEBUG = 3;
  public static final int TRACE = 4;

  // TODO: We may support changing the log level at runtime in the future.
  private static final boolean isErrorEnabled = Binding.tracingSlf4jEventEnabled(ERROR);
  private static final boolean isWarnEnabled = Binding.tracingSlf4jEventEnabled(WARN);
  private static final boolean isInfoEnabled = Binding.tracingSlf4jEventEnabled(INFO);
  private static final boolean isDebugEnabled = Binding.tracingSlf4jEventEnabled(DEBUG);
  private static final boolean isTraceEnabled = Binding.tracingSlf4jEventEnabled(TRACE);

  public static void event(String name, int level, String message) {
    Binding.tracingSlf4jEvent(Thread.currentThread().getName(), name, level, message);
  }

  public static boolean isEnabled(int level) {
    switch (level) {
      case ERROR:
        return isErrorEnabled;
      case WARN:
        return isWarnEnabled;
      case INFO:
        return isInfoEnabled;
      case DEBUG:
        return isDebugEnabled;
      case TRACE:
        return isTraceEnabled;
      default:
        return false;
    }
  }
}
