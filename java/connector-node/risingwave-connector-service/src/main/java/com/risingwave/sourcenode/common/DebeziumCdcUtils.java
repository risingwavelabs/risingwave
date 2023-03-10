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

package com.risingwave.sourcenode.common;

import java.util.Properties;

public class DebeziumCdcUtils {

    /** Common config properties for Debeizum CDC connectors */
    public static Properties createCommonConfig() {
        var props = new Properties();
        // capture decimal type in doule values, which may result in a loss of precision but is
        // easier to use
        // https://debezium.io/documentation/reference/stable/connectors/mysql.html#mysql-property-decimal-handling-mode
        props.setProperty("decimal.handling.mode", "double");

        // Add a converter for `Date` data type, which convert `Date` into a string
        props.setProperty("converters", "datetime");
        props.setProperty(
                "datetime.type",
                "com.risingwave.connector.cdc.debezium.converters.DatetimeTypeConverter");
        props.setProperty("max.batch.size", "1024");
        props.setProperty("max.queue.size", "8192");
        return props;
    }
}
