// Copyright 2025 RisingWave Labs
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Global registry for all JNI Debezium source handlers. */
public class JniDbzSourceRegistry {
    static final Logger LOG = LoggerFactory.getLogger(JniDbzSourceRegistry.class);

    private static final ConcurrentHashMap<Long, JniDbzSourceHandler> sourceHandlers =
            new ConcurrentHashMap<>();

    public static void register(JniDbzSourceHandler handler) {
        var sourceId = handler.getSourceId();
        var prev = sourceHandlers.put(sourceId, handler);
        if (prev != null) {
            LOG.info("Replacing existing source handler for source ID {}", sourceId);
        }
    }

    public static JniDbzSourceHandler getSourceHandler(long sourceId) {
        return sourceHandlers.get(sourceId);
    }

    public static void unregister(JniDbzSourceHandler handler) {
        // Only unregister if the handler is the same object as the one in the registry.
        // This is crucial because there could be a race condition where multiple engines
        // operate with the same source ID during recovery. We don't want the cleanup
        // process of the stale one to remove the new one.
        // TODO: use a more robust way to handle this, e.g., may include the current term
        // ID of the cluster in the key.
        var sourceId = handler.getSourceId();
        var removed = sourceHandlers.remove(sourceId, handler);
        if (!removed) {
            LOG.info(
                    "Source handler for source ID {} does not match the one in the registry when unregistering (it may have been replaced)",
                    sourceId);
        }
    }
}
