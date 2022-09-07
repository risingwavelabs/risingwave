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
import Head from "next/head"

import Link from "next/link"
import { Fragment, useEffect, useState } from "react"
import Title from "../components/Title"
import extractColumnInfo from "../lib/extractInfo"
import { Source } from "../proto/gen/catalog"
import { getDataSources } from "./api/streaming"

export default function DataSources() {
  const toast = useToast()
  const [sourceList, setSourceList] = useState<Source[]>([])

  useEffect(() => {
    async function doFetch() {
      try {
        setSourceList(await getDataSources())
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
  }, [toast])

  const retVal = (
    <Box p={3}>
      <Title>Data Sources</Title>
      <TableContainer>
        <Table variant="simple" size="sm" maxWidth="full">
          <Thead>
            <Tr>
              <Th width={3}>Id</Th>
              <Th width={5}>Name</Th>
              <Th width={3}>Owner</Th>
              <Th width={3}>Type</Th>
              <Th width={1}>Metrics</Th>
              <Th width={1}>Depends</Th>
              <Th width={1}>Fragments</Th>
              <Th>Visible Columns</Th>
            </Tr>
          </Thead>
          <Tbody>
            {sourceList
              .filter((source) => !source.name.startsWith("__"))
              .map((source) => (
                <Tr key={source.id}>
                  <Td>{source.id}</Td>
                  <Td>{source.name}</Td>
                  <Td>{source.owner}</Td>
                  <Td>{source.info?.$case}</Td>
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
                    <Link href={`/streaming_graph/?id=${source.id}`}>
                      <Button
                        size="sm"
                        aria-label="view metrics"
                        colorScheme="teal"
                        variant="link"
                      >
                        D
                      </Button>
                    </Link>
                  </Td>
                  <Td>
                    <Link href={`/streaming_plan/?id=${source.id}`}>
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
                    {(() => {
                      switch (source.info?.$case) {
                        case "streamSource":
                          return source.info?.streamSource?.columns
                            .map((col) => extractColumnInfo(col))
                            .join(", ")
                        case "tableSource":
                          return source.info?.tableSource?.columns
                            .map((col) => extractColumnInfo(col))
                            .join(", ")
                        default:
                          return <Fragment />
                      }
                    })()}
                  </Td>
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
        <title>Data Sources</title>
      </Head>
      {retVal}
    </Fragment>
  )
}
