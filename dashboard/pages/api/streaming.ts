/*
 * Copyright 2022 Singularity Data
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
import sortBy from "lodash/sortBy"
import { Source, Table } from "../../proto/gen/catalog"
import { ActorLocation } from "../../proto/gen/meta"
import { StreamActor } from "../../proto/gen/stream_plan"
import api from "./api"

export async function getActors(): Promise<ActorLocation[]> {
  return (await api.get("/api/actors")).map(ActorLocation.fromJSON)
}

export async function getFragments(): Promise<[number, StreamActor]> {
  let fragmentList = (await api.get("/api/fragments")).map(
    ([tableId, tableActor]: [any, any]) => [
      tableId as number,
      StreamActor.fromJSON(tableActor),
    ]
  )
  fragmentList = sortBy(fragmentList, "id")
  return fragmentList
}

export async function getMaterializedViews(): Promise<Table[]> {
  let mvList = (await api.get("/api/materialized_views")).map(Table.fromJSON)
  mvList = sortBy(mvList, "id")
  return mvList
}

export async function getDataSources(): Promise<Source[]> {
  let sourceList = (await api.get("/api/sources")).map(Source.fromJSON)
  sourceList = sortBy(sourceList, "id")
  return sourceList
}
