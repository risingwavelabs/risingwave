/*
 * Copyright 2026 RisingWave Labs
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package com.risingwave.connector.cdc.debezium.internal;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/** Delivers the initial Oracle mining position to the matching embedded-engine consumer. */
public final class OracleMiningInitialization {
    @FunctionalInterface
    public interface Listener {
        void onMiningStarted(String scn, Map<String, ?> partition, Map<String, ?> offset)
                throws InterruptedException;
    }

    private static final ConcurrentMap<String, Listener> LISTENERS = new ConcurrentHashMap<>();

    private OracleMiningInitialization() {}

    public static void register(String logicalName, Listener listener) {
        if (LISTENERS.putIfAbsent(logicalName, listener) != null) {
            throw new IllegalStateException(
                    "Oracle mining listener already registered: " + logicalName);
        }
    }

    public static void unregister(String logicalName, Listener listener) {
        LISTENERS.remove(logicalName, listener);
    }

    public static boolean hasListener(String logicalName) {
        return LISTENERS.containsKey(logicalName);
    }

    public static void report(
            String logicalName, String scn, Map<String, ?> partition, Map<String, ?> offset)
            throws InterruptedException {
        Listener listener = LISTENERS.get(logicalName);
        if (listener == null) {
            throw new IllegalStateException("Oracle mining listener is missing: " + logicalName);
        }
        listener.onMiningStarted(scn, partition, offset);
    }
}
