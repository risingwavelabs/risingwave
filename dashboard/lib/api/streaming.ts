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
 *
 */

import _ from "lodash"
import sortBy from "lodash/sortBy"
import { Sink, Source, Table, View } from "../../proto/gen/catalog"
import { Table as TableV2 } from "../../proto/gen/dashboard"
import {
  ListObjectDependenciesResponse_ObjectDependencies as ObjectDependencies,
  TableFragments,
} from "../../proto/gen/meta"
import { ColumnCatalog, Field } from "../../proto/gen/plan_common"
import api from "./api"

export async function getFragments(): Promise<TableFragments[]> {
  let fragmentList: TableFragments[] = (await api.get("/fragments2")).map(
    TableFragments.fromJSON
  )
  fragmentList = sortBy(fragmentList, (x) => x.tableId)
  return fragmentList
}

export interface Relation {
  id: number
  name: string
  owner: number
  columns: (ColumnCatalog | Field)[]
}

export interface StreamingJob extends Relation {
  dependentRelations: number[]
}

export function relationType(x: Relation) {
  if ((x as Table).tableType !== undefined) {
    return (x as Table).tableType
  } else if ((x as Sink).sinkFromName !== undefined) {
    return "SINK"
  } else if ((x as Source).info !== undefined) {
    return "SOURCE"
  } else {
    return "UNKNOWN"
  }
}

export type RelationType = ReturnType<typeof relationType>

export function relationTypeTitleCase(x: Relation) {
  return _.startCase(_.toLower(relationType(x)))
}

export function relationIsStreamingJob(x: Relation): x is StreamingJob {
  const type = relationType(x)
  return type !== "UNKNOWN" && type !== "SOURCE" && type !== "INTERNAL"
}

export async function getStreamingJobs() {
  let jobs = _.concat<StreamingJob>(
    await getMaterializedViews(),
    await getTables(),
    await getIndexes(),
    await getSinks()
  )
  jobs = sortBy(jobs, (x) => x.id)
  return jobs
}

export async function getRelations() {
  let relations = _.concat<Relation>(
    await getMaterializedViews(),
    await getTables(),
    await getIndexes(),
    await getSinks(),
    await getSources()
  )
  relations = sortBy(relations, (x) => x.id)
  return relations
}

export async function getRelationDependencies() {
  return await getObjectDependencies()
}

async function getTableCatalogsInner(
  path: "tables" | "materialized_views" | "indexes" | "internal_tables"
) {
  let list: Table[] = (await api.get(`/${path}`)).map(Table.fromJSON)
  list = sortBy(list, (x) => x.id)
  return list
}

export async function getMaterializedViews() {
  return await getTableCatalogsInner("materialized_views")
}

export async function getTables() {
  let list: TableV2[] = (await api.get(`/v2/tables`)).map(TableV2.fromJSON)
  list = sortBy(list, (x) => x.id)
  return list
}

export async function getIndexes() {
  return await getTableCatalogsInner("indexes")
}

export async function getInternalTables() {
  return await getTableCatalogsInner("internal_tables")
}

export async function getSinks() {
  let sinkList: Sink[] = (await api.get("/sinks")).map(Sink.fromJSON)
  sinkList = sortBy(sinkList, (x) => x.id)
  return sinkList
}

export async function getSources() {
  let sourceList: Source[] = (await api.get("/sources")).map(Source.fromJSON)
  sourceList = sortBy(sourceList, (x) => x.id)
  return sourceList
}

export async function getViews() {
  let views: View[] = (await api.get("/views")).map(View.fromJSON)
  views = sortBy(views, (x) => x.id)
  return views
}

// export async function getDatabases() {
//   let views: View[] = (await api.get("/views")).map(View.fromJSON)
//   views = sortBy(views, (x) => x.id)
//   return views
// }

export async function getObjectDependencies() {
  let objDependencies: ObjectDependencies[] = (
    await api.get("/object_dependencies")
  ).map(ObjectDependencies.fromJSON)
  const objDependencyGroup = new Map<number, number[]>()
  objDependencies.forEach((x) => {
    if (!objDependencyGroup.has(x.objectId)) {
      objDependencyGroup.set(x.objectId, new Array<number>())
    }
    objDependencyGroup.get(x.objectId)?.push(x.referencedObjectId)
  })

  return objDependencyGroup
}
