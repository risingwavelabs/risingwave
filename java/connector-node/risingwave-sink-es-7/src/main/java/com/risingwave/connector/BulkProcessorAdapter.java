/*
 * Copyright 2024 RisingWave Labs
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.risingwave.connector;

import java.util.concurrent.TimeUnit;

public interface BulkProcessorAdapter {
    public void addRow(String index, String key, String doc, String routing)
            throws InterruptedException;

    public void deleteRow(String index, String key, String routing) throws InterruptedException;

    public void flush();

    public void awaitClose(long timeout, TimeUnit unit) throws InterruptedException;
}
