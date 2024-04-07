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

package com.risingwave.connector.source.core;

import java.util.concurrent.ConcurrentHashMap;

/** Global registry for all JNI Debezium source handlers. */
public class JniDbzSourceRegistry {
    private static final ConcurrentHashMap<Long, JniDbzSourceHandler> sourceHandlers =
            new ConcurrentHashMap<>();

    public static void register(long sourceId, JniDbzSourceHandler handler) {
        sourceHandlers.put(sourceId, handler);
    }

    public static JniDbzSourceHandler getSourceHandler(long sourceId) {
        return sourceHandlers.get(sourceId);
    }

    public static void unregister(long sourceId) {
        sourceHandlers.remove(sourceId);
    }
}
