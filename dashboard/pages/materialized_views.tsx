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
  useToast,
} from "@chakra-ui/react"

import Link from "next/link"
import { useEffect, useState } from "react"
import Title from "../components/Title"
import { Table as RwTable } from "../proto/gen/catalog"
import { getMaterializedViews } from "./api/streaming"

export default function MaterializeViews() {
  const toast = useToast()
  const [mvList, setMvList] = useState<RwTable[]>([])

  useEffect(() => {
    async function doFetch() {
      try {
        setMvList(await getMaterializedViews())
      } catch (e: any) {
        toast({
          title: "Error Occurred",
          description: e.toString(),
          status: "error",
          duration: 5000,
          isClosable: true,
        })
        console.error(e)
      }
    }
    doFetch()
    return () => {}
  }, [])

  return (
    <Box p={3}>
      <Title>Materialized Views</Title>
      <TableContainer>
        <Table variant="simple" size="sm" maxWidth="full">
          <Thead>
            <Tr>
              <Th width={3}>Id</Th>
              <Th width={5}>Name</Th>
              <Th width={3}>Owner</Th>
              <Th width={3}>Metrics</Th>
              <Th width={3}>Depends</Th>
              <Th width={3}>Fragments</Th>
              <Th>Visible Columns</Th>
            </Tr>
          </Thead>
          <Tbody>
            {mvList
              .filter((mv) => !mv.name.startsWith("__"))
              .map((mv) => (
                <Tr key={mv.id}>
                  <Td>{mv.id}</Td>
                  <Td>{mv.name}</Td>
                  <Td>{mv.owner}</Td>
                  <Td>
                    <Button
                      size="sm"
                      aria-label="view metrics"
                      colorScheme="teal"
                      variant="link"
                    >
                      M
                    </Button>
                  </Td>
                  <Td>
                    <Button
                      size="sm"
                      aria-label="view metrics"
                      colorScheme="teal"
                      variant="link"
                    >
                      D
                    </Button>
                  </Td>
                  <Td>
                    <Link href={`/streaming_plan/?id=${mv.id}`}>
                      <Button
                        size="sm"
                        aria-label="view metrics"
                        colorScheme="teal"
                        variant="link"
                      >
                        F
                      </Button>
                    </Link>
                  </Td>
                  <Td overflowWrap="normal">
                    {mv.columns
                      .filter((col) => !col.isHidden)
                      .map(
                        (col) =>
                          `${col.columnDesc?.name} (${col.columnDesc?.columnType?.typeName})`
                      )
                      .join(", ")}
                  </Td>
                </Tr>
              ))}
          </Tbody>
        </Table>
      </TableContainer>
    </Box>
  )
}
