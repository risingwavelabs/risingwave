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

import { Box, Button, Flex, Text, VStack } from "@chakra-ui/react"
import { reverse, sortBy } from "lodash"
import Head from "next/head"
import { parseAsInteger, useQueryState } from "nuqs"
import { Fragment, useCallback } from "react"
import RelationDependencyGraph from "../components/RelationDependencyGraph"
import Title from "../components/Title"
import { FragmentPoint } from "../lib/layout"
import useFetch from "./api/fetch"
import { Relation, getRelations, relationIsStreamingJob } from "./api/streaming"

const SIDEBAR_WIDTH = "200px"

function buildDependencyAsEdges(list: Relation[]): FragmentPoint[] {
  const edges = []
  const relationSet = new Set(list.map((r) => r.id))
  for (const r of reverse(sortBy(list, "id"))) {
    edges.push({
      id: r.id.toString(),
      name: r.name,
      parentIds: relationIsStreamingJob(r)
        ? r.dependentRelations
            .filter((r) => relationSet.has(r))
            .map((r) => r.toString())
        : [],
      order: r.id,
    })
  }
  return edges
}

export default function StreamingGraph() {
  const { response: streamingJobList } = useFetch(getRelations)
  const [selectedId, setSelectedId] = useQueryState("id", parseAsInteger)

  const mvDependencyCallback = useCallback(() => {
    if (streamingJobList) {
      return buildDependencyAsEdges(streamingJobList)
    } else {
      return undefined
    }
  }, [streamingJobList])

  const mvDependency = mvDependencyCallback()

  const retVal = (
    <Flex p={3} height="calc(100vh - 20px)" flexDirection="column">
      <Title>Dependency Graph</Title>
      <Flex flexDirection="row" height="full">
        <Flex
          width={SIDEBAR_WIDTH}
          height="full"
          maxHeight="full"
          mr={3}
          alignItems="flex-start"
          flexDirection="column"
        >
          <Text fontWeight="semibold" mb={3}>
            Relations
          </Text>
          <Box flex={1} overflowY="scroll">
            <VStack width={SIDEBAR_WIDTH} align="start" spacing={1}>
              {streamingJobList?.map((r) => {
                const match = selectedId === r.id
                return (
                  <Button
                    colorScheme={match ? "blue" : "gray"}
                    color={match ? "blue.600" : "gray.500"}
                    variant={match ? "outline" : "ghost"}
                    width="full"
                    py={0}
                    height={8}
                    justifyContent="flex-start"
                    onClick={() => setSelectedId(r.id)}
                  >
                    {r.name}
                  </Button>
                )
              })}
            </VStack>
          </Box>
        </Flex>
        <Box
          flex={1}
          height="full"
          ml={3}
          overflowX="scroll"
          overflowY="scroll"
        >
          <Text fontWeight="semibold">Graph</Text>
          {mvDependency && (
            <RelationDependencyGraph
              nodes={mvDependency}
              selectedId={selectedId?.toString()}
            />
          )}
        </Box>
      </Flex>
    </Flex>
  )

  return (
    <Fragment>
      <Head>
        <title>Streaming Graph</title>
      </Head>
      {retVal}
    </Fragment>
  )
}
