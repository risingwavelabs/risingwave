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

import {
  Box,
  Button,
  Modal,
  ModalBody,
  ModalCloseButton,
  ModalContent,
  ModalFooter,
  ModalHeader,
  ModalOverlay,
  Table,
  TableContainer,
  Tbody,
  Td,
  Th,
  Thead,
  Tr,
  useDisclosure,
  useToast,
} from "@chakra-ui/react"
import loadable from "@loadable/component"
import Head from "next/head"

import Link from "next/link"
import { Fragment, useEffect, useState } from "react"
import Title from "../components/Title"
import extractColumnInfo from "../lib/extractInfo"
import { Relation } from "../pages/api/streaming"

const ReactJson = loadable(() => import("react-json-view"))

export type Column<R> = {
  name: string
  width: number
  content: (r: R) => React.ReactNode
}

export function Relations<R extends Relation>(
  title: string,
  getRelations: () => Promise<R[]>,
  extraColumns: Column<R>[] = [],
  isStreamingJob: boolean = true // only show metrics and graphs for streaming jobs
) {
  const toast = useToast()
  const [relationList, setRelationList] = useState<R[]>([])

  useEffect(() => {
    async function doFetch() {
      try {
        setRelationList(await getRelations())
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
  }, [toast, getRelations])

  const { isOpen, onOpen, onClose } = useDisclosure()
  const [currentRelation, setCurrentRelation] = useState<R>()
  const openRelationCatalog = (relation: R) => {
    if (relation) {
      setCurrentRelation(relation)
      onOpen()
    }
  }

  const catalogModal = (
    <Modal isOpen={isOpen} onClose={onClose} size="3xl">
      <ModalOverlay />
      <ModalContent>
        <ModalHeader>
          Catalog of {currentRelation?.id} - {currentRelation?.name}
        </ModalHeader>
        <ModalCloseButton />
        <ModalBody>
          {isOpen && currentRelation && (
            <ReactJson
              src={currentRelation}
              collapsed={1}
              name={null}
              displayDataTypes={false}
            />
          )}
        </ModalBody>

        <ModalFooter>
          <Button colorScheme="blue" mr={3} onClick={onClose}>
            Close
          </Button>
        </ModalFooter>
      </ModalContent>
    </Modal>
  )

  const table = (
    <Box p={3}>
      <Title>{title}</Title>
      <TableContainer>
        <Table variant="simple" size="sm" maxWidth="full">
          <Thead>
            <Tr>
              <Th width={3}>Id</Th>
              <Th width={5}>Name</Th>
              <Th width={3}>Owner</Th>
              {extraColumns.map((c) => (
                <Th key={c.name} width={c.width}>
                  {c.name}
                </Th>
              ))}
              {isStreamingJob && (
                <>
                  <Th width={1}>Metrics</Th>
                  <Th width={1}>Depends</Th>
                  <Th width={1}>Fragments</Th>
                </>
              )}
              <Th>Visible Columns</Th>
            </Tr>
          </Thead>
          <Tbody>
            {relationList.map((r) => (
              <Tr key={r.id}>
                <Td>
                  <Button
                    size="sm"
                    aria-label="view catalog"
                    colorScheme="teal"
                    variant="link"
                    onClick={() => openRelationCatalog(r)}
                  >
                    {r.id}
                  </Button>
                </Td>
                <Td>{r.name}</Td>
                <Td>{r.owner}</Td>
                {extraColumns.map((c) => (
                  <Td key={c.name}>{c.content(r)}</Td>
                ))}
                {isStreamingJob && (
                  <>
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
                      <Link href={`/streaming_graph/?id=${r.id}`}>
                        <Button
                          size="sm"
                          aria-label="view dependents"
                          colorScheme="teal"
                          variant="link"
                        >
                          D
                        </Button>
                      </Link>
                    </Td>
                    <Td>
                      <Link href={`/streaming_plan/?id=${r.id}`}>
                        <Button
                          size="sm"
                          aria-label="view fragments"
                          colorScheme="teal"
                          variant="link"
                        >
                          F
                        </Button>
                      </Link>
                    </Td>
                  </>
                )}
                <Td overflowWrap="normal">
                  {r.columns
                    .filter((col) => !col.isHidden)
                    .map((col) => extractColumnInfo(col))
                    .join(", ")}
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
        <title>{title}</title>
      </Head>
      {catalogModal}
      {table}
    </Fragment>
  )
}
