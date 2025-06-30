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
  Box,
  Button,
  Table,
  TableContainer,
  Tbody,
  Td,
  Th,
  Thead,
  Tr,
} from "@chakra-ui/react"
import loadable from "@loadable/component"
import Head from "next/head"

import Link from "next/link"
import { Fragment } from "react"
import Title from "../components/Title"
import useFetch from "../lib/api/fetch"
import {
  Relation,
  StreamingRelation,
  getDatabases,
  getSchemas,
  getUsers,
} from "../lib/api/streaming"
import extractColumnInfo from "../lib/extractInfo"
import {
  Sink as RwSink,
  Source as RwSource,
  Table as RwTable,
} from "../proto/gen/catalog"
import { CatalogModal, useCatalogModal } from "./CatalogModal"

export const ReactJson = loadable(() => import("react-json-view"))

export type Column<R> = {
  name: string
  width: number
  content: (r: R) => React.ReactNode
}

export const dependentsColumn: Column<Relation> = {
  name: "Depends",
  width: 1,
  content: (r) => (
    <Link href={`/relation_graph/?id=${r.id}`}>
      <Button
        size="sm"
        aria-label="view dependents"
        colorScheme="blue"
        variant="link"
      >
        D
      </Button>
    </Link>
  ),
}

export const fragmentsColumn: Column<StreamingRelation> = {
  name: "Fragments",
  width: 1,
  content: (r) => (
    <Link href={`/fragment_graph/?id=${r.id}`}>
      <Button
        size="sm"
        aria-label="view fragments"
        colorScheme="blue"
        variant="link"
      >
        F
      </Button>
    </Link>
  ),
}

export const primaryKeyColumn: Column<RwTable> = {
  name: "Primary Key",
  width: 1,
  content: (r) =>
    r.pk
      .map((order) => order.columnIndex)
      .map((i) => r.columns[i])
      .map((col) => extractColumnInfo(col))
      .join(", "),
}

export const vnodeCountColumn: Column<RwTable> = {
  name: "Vnode Count",
  width: 1,
  // The table catalogs retrieved here are constructed from SQL models,
  // where the `vnode_count` column has already been populated during migration.
  // Therefore, it should always be present and no need to specify a fallback.
  content: (r) => r.maybeVnodeCount ?? "?",
}

// Helper function to format bytes into human readable format
function formatBytes(bytes: number | undefined): string {
  if (bytes === undefined) return "unknown"
  if (bytes === 0) return "0 B"
  const k = 1024
  const sizes = ["B", "KB", "MB", "GB", "TB"]
  const i = Math.floor(Math.log(bytes) / Math.log(k))
  return Math.round((bytes / Math.pow(k, i)) * 100) / 100 + " " + sizes[i]
}

export const dataSizeColumn: Column<Relation> = {
  name: "Data Size",
  width: 2,
  content: (r) => formatBytes(r.totalSizeBytes),
}

export const tableColumns = [primaryKeyColumn, vnodeCountColumn, dataSizeColumn]

export const connectorColumnSource: Column<RwSource> = {
  name: "Connector",
  width: 3,
  content: (r) => r.withProperties.connector ?? "unknown",
}

export const connectorColumnSink: Column<RwSink> = {
  name: "Connector",
  width: 3,
  content: (r) => r.properties.connector ?? "unknown",
}

export const streamingJobColumns = [dependentsColumn, fragmentsColumn]

export function Relations<R extends Relation>(
  title: string,
  getRelations: () => Promise<R[]>,
  extraColumns: Column<R>[]
) {
  const { response: relationList } = useFetch(async () => {
    const relations = await getRelations()
    const users = await getUsers()
    const databases = await getDatabases()
    const schemas = await getSchemas()
    return relations.map((r) => {
      // Add owner, schema, and database names. It's linear search but the list is small.
      const owner = users.find((u) => u.id === r.owner)
      const ownerName = owner?.name
      const schema = schemas.find((s) => s.id === r.schemaId)
      const schemaName = schema?.name
      const database = databases.find((d) => d.id === r.databaseId)
      const databaseName = database?.name
      return { ...r, ownerName, schemaName, databaseName }
    })
  })
  const [modalData, setModalId] = useCatalogModal(relationList)

  const modal = (
    <CatalogModal modalData={modalData} onClose={() => setModalId(null)} />
  )

  const table = (
    <Box p={3}>
      <Title>{title}</Title>
      {relationList && (
        <Box mb={3} fontSize="sm" color="gray.600">
          Total: {relationList.length}{" "}
          {relationList.length === 1 ? "item" : "items"}
        </Box>
      )}
      <TableContainer>
        <Table variant="simple" size="sm" maxWidth="full">
          <Thead>
            <Tr>
              <Th width={3}>Id</Th>
              <Th width={5}>Database</Th>
              <Th width={5}>Schema</Th>
              <Th width={5}>Name</Th>
              <Th width={3}>Owner</Th>
              {extraColumns.map((c) => (
                <Th key={c.name} width={c.width}>
                  {c.name}
                </Th>
              ))}
              <Th>Visible Columns</Th>
            </Tr>
          </Thead>
          <Tbody>
            {relationList?.map((r) => (
              <Tr key={r.id}>
                <Td>
                  <Button
                    size="sm"
                    aria-label="view catalog"
                    colorScheme="blue"
                    variant="link"
                    onClick={() => setModalId(r.id)}
                  >
                    {r.id}
                  </Button>
                </Td>
                <Td>{r.databaseName}</Td>
                <Td>{r.schemaName}</Td>
                <Td>{r.name}</Td>
                <Td>{r.ownerName}</Td>
                {extraColumns.map((c) => (
                  <Td key={c.name}>{c.content(r)}</Td>
                ))}
                {r.columns && r.columns.length > 0 && (
                  <Td overflowWrap="normal">
                    {r.columns
                      .filter((col) =>
                        "isHidden" in col ? !col.isHidden : true
                      )
                      .map((col) => extractColumnInfo(col))
                      .join(", ")}
                  </Td>
                )}
              </Tr>
            ))}
          </Tbody>
        </Table>
      </TableContainer>
    </Box>
  )

  return (
    <Fragment>
      <Head>
        <title>{title}</title>
      </Head>
      {modal}
      {table}
    </Fragment>
  )
}
