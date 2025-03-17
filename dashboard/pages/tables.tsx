/*
 * Copyright 2025 RisingWave Labs
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
 *
 */

import {
  Column,
  Relations,
  streamingJobColumns,
  tableColumns,
} from "../components/Relations"
import { getTables } from "../lib/api/streaming"
import { Table } from "../proto/gen/catalog"

export default function Tables() {
  const associatedSourceColumn: Column<Table> = {
    name: "Source",
    width: 3,
    content: (t) => t.optionalAssociatedSourceId?.associatedSourceId ?? "-",
  }

  return Relations("Tables", getTables, [
    associatedSourceColumn,
    ...streamingJobColumns,
    ...tableColumns,
  ])
}
