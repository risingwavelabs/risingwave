/*
 * Copyright 2023 Singularity Data
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

import sortBy from "lodash/sortBy"
import { Sink, Source, Table } from "../../proto/gen/catalog"
import { ActorLocation, TableFragments } from "../../proto/gen/meta"
import { ColumnCatalog } from "../../proto/gen/plan_common"
import api from "./api"

export async function getActors(): Promise<ActorLocation[]> {
  return (await api.get("/api/actors")).map(ActorLocation.fromJSON)
}

export async function getFragments(): Promise<TableFragments[]> {
  let fragmentList: TableFragments[] = (await api.get("/api/fragments2")).map(
    TableFragments.fromJSON
  )
  fragmentList = sortBy(fragmentList, (x) => x.tableId)
  return fragmentList
}

export interface Relation {
  id: number
  name: string
  owner: number
  dependentRelations: number[]
  columns: ColumnCatalog[]
}

export async function getRelations(): Promise<Relation[]> {
  const materialized_views: Relation[] = await getMaterializedViews()
  const sinks: Relation[] = await getSinks()
  let relations = materialized_views.concat(sinks)
  relations = sortBy(relations, (x) => x.id)
  return relations
}

export async function getMaterializedViews(withInternal: boolean = false) {
  let mvList: Table[] = (await api.get("/api/materialized_views")).map(
    Table.fromJSON
  )
  mvList = mvList.filter((mv) => withInternal || !mv.name.startsWith("__"))
  mvList = sortBy(mvList, (x) => x.id)
  return mvList
}

export async function getDataSources() {
  let sourceList: Source[] = (await api.get("/api/sources")).map(
    Source.fromJSON
  )
  sourceList = sortBy(sourceList, (x) => x.id)
  return sourceList
}

export async function getSinks() {
  let sinkList: Sink[] = (await api.get("/api/sinks")).map(Sink.fromJSON)
  sinkList = sortBy(sinkList, (x) => x.id)
  return sinkList
}
