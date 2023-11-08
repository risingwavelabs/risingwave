// Copyright 2023 RisingWave Labs
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

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.json.JsonConverter;

/**
 * Customize the JSON converter to avoid outputting the `schema` field but retian `payload` field in
 * the JSON output. e.g.
 *
 * <pre>
 * {
 *     "schema": null,
 *     "payload": {
 *     	...
 *     }
 * }
 * </pre>
 */
public class DbzJsonConverter extends JsonConverter {
    @Override
    public ObjectNode asJsonSchema(Schema schema) {
        return null;
    }
}
