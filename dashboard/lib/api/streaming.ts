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

import { Expose, plainToInstance } from "class-transformer"
import _ from "lodash"
import sortBy from "lodash/sortBy"
import {
  Database,
  Index,
  Schema,
  Sink,
  Source,
  Subscription,
  Table,
  View,
} from "../../proto/gen/catalog"
import {
  FragmentToRelationMap,
  ListObjectDependenciesResponse_ObjectDependencies as ObjectDependencies,
  RelationIdInfos,
  TableFragments,
} from "../../proto/gen/meta"
import { ColumnCatalog, Field } from "../../proto/gen/plan_common"
import { UserInfo } from "../../proto/gen/user"
import api from "./api"

// NOTE(kwannoel): This can be optimized further, instead of fetching the entire TableFragments struct,
// We can fetch the fields we need from TableFragments, in a truncated struct.
export async function getFragmentsByJobId(
  jobId: number
): Promise<TableFragments> {
  let route = "/fragments/job_id/" + jobId.toString()
  let tableFragments: TableFragments = TableFragments.fromJSON(
    await api.get(route)
  )
  return tableFragments
}

export async function getRelationIdInfos(): Promise<RelationIdInfos> {
  let fragmentIds: RelationIdInfos = await api.get("/relation_id_infos")
  return fragmentIds
}

export interface Relation {
  id: number
  name: string
  owner: number
  schemaId: number
  databaseId: number

  // For display
  columns?: (ColumnCatalog | Field)[]
  ownerName?: string
  schemaName?: string
  databaseName?: string
  totalSizeBytes?: number
}

export class StreamingJob {
  @Expose({ name: "jobId" })
  id!: number
  @Expose({ name: "objType" })
  _objType!: string
  name!: string
  jobStatus!: string
  @Expose({ name: "parallelism" })
  _parallelism!: any
  maxParallelism!: number

  get parallelism() {
    const parallelism = this._parallelism
    if (typeof parallelism === "string") {
      // `Adaptive`
      return parallelism
    } else if (typeof parallelism === "object") {
      // `Fixed (64)`
      let key = Object.keys(parallelism)[0]
      let value = parallelism[key]
      return `${key} (${value})`
    } else {
      // fallback
      return JSON.stringify(parallelism)
    }
  }

  get type() {
    if (this._objType == "Table") {
      return "Table / MV"
    } else {
      return this._objType
    }
  }
}

export interface StreamingRelation extends Relation {
  dependentRelations: number[]
}

export function relationType(x: Relation) {
  if ((x as Table).tableType !== undefined) {
    return (x as Table).tableType
  } else if ((x as Sink).sinkFromName !== undefined) {
    return "SINK"
  } else if ((x as Source).info !== undefined) {
    return "SOURCE"
  } else if ((x as Subscription).dependentTableId !== undefined) {
    return "SUBSCRIPTION"
  } else {
    return "UNKNOWN"
  }
}
export type RelationType = ReturnType<typeof relationType>

export function relationTypeTitleCase(x: Relation) {
  return _.startCase(_.toLower(relationType(x)))
}

export function relationIsStreamingJob(x: Relation): x is StreamingRelation {
  const type = relationType(x)
  return type !== "UNKNOWN" && type !== "SOURCE" && type !== "INTERNAL"
}

export async function getStreamingJobs() {
  let jobs = plainToInstance(
    StreamingJob,
    (await api.get("/streaming_jobs")) as any[]
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
    await getSources(),
    await getSubscriptions()
  )
  relations = sortBy(relations, (x) => x.id)
  return relations
}

export async function getRelationDependencies() {
  return await getObjectDependencies()
}

export async function getFragmentToRelationMap() {
  let res = await api.get("/fragment_to_relation_map")
  let fragmentVertexToRelationMap: FragmentToRelationMap =
    FragmentToRelationMap.fromJSON(res)
  return fragmentVertexToRelationMap
}

interface ExtendedTable extends Table {
  totalSizeBytes?: number
}

// Extended conversion function for Table with extra fields
function extendedTableFromJSON(json: any): ExtendedTable {
  const table = Table.fromJSON(json)
  return {
    ...table,
    totalSizeBytes: json.total_size_bytes,
  }
}

async function getTableCatalogsInner(
  path: "tables" | "materialized_views" | "indexes" | "internal_tables"
): Promise<ExtendedTable[]> {
  let list = (await api.get(`/${path}`)).map(extendedTableFromJSON)
  list = sortBy(list, (x) => x.id)
  return list
}

export async function getMaterializedViews() {
  return await getTableCatalogsInner("materialized_views")
}

export async function getTables() {
  return await getTableCatalogsInner("tables")
}

export async function getIndexes(): Promise<(Table & Index)[]> {
  let index_tables = await getTableCatalogsInner("indexes")
  let index_items: Index[] = (await api.get("/index_items")).map(Index.fromJSON)

  // Join them by id.
  return index_tables.flatMap((x) => {
    let index = index_items.find((y) => y.id === x.id)
    if (index === undefined) {
      return []
    } else {
      return [{ ...x, ...index }]
    }
  })
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

export async function getSubscriptions() {
  let subscriptions: Subscription[] = (await api.get("/subscriptions")).map(
    Subscription.fromJSON
  )
  subscriptions = sortBy(subscriptions, (x) => x.id)
  return subscriptions
}

export async function getUsers() {
  let users: UserInfo[] = (await api.get("/users")).map(UserInfo.fromJSON)
  users = sortBy(users, (x) => x.id)
  return users
}

export async function getDatabases() {
  let databases: Database[] = (await api.get("/databases")).map(
    Database.fromJSON
  )
  databases = sortBy(databases, (x) => x.id)
  return databases
}

export async function getSchemas() {
  let schemas: Schema[] = (await api.get("/schemas")).map(Schema.fromJSON)
  schemas = sortBy(schemas, (x) => x.id)
  return schemas
}

// Returns a map of object id to a list of object ids that it depends on
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
