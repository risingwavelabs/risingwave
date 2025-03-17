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

package com.risingwave.connector.source.common;

public abstract class DatabaseValidator {

    public void validateAll() {
        validateDbConfig();
        validateUserPrivilege();
        // If the source connector is created by share source, it will capture events from
        // multiple tables, skip validate its schema
        if (!isCdcSourceJob()) {
            validateTable();
        }
    }

    /** Validate the config of the upstream database */
    abstract void validateDbConfig();

    /** Validate the required privileges to start the connector */
    abstract void validateUserPrivilege();

    /** Validate the properties of the source table */
    abstract void validateTable();

    /** Check if the validation is for CDC source job */
    abstract boolean isCdcSourceJob();
}
