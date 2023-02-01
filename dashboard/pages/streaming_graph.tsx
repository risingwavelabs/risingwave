/*
 * Copyright 2023 RisingWave Labs
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

import { Box, Button, Flex, Text, useToast, VStack } from "@chakra-ui/react"
import { reverse, sortBy } from "lodash"
import Head from "next/head"
import Link from "next/link"
import { useRouter } from "next/router"
import { Fragment, useCallback, useEffect, useState } from "react"
import { StreamGraph } from "../components/StreamGraph"
import Title from "../components/Title"
import { ActorPoint } from "../lib/layout"
import { getRelations, Relation, relationIsStreamingJob } from "./api/streaming"

const SIDEBAR_WIDTH = "200px"

function buildDependencyAsEdges(list: Relation[]): ActorPoint[] {
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
  const toast = useToast()
  const [streamingJobList, setStreamingJobList] = useState<Relation[]>()

  useEffect(() => {
    async function doFetch() {
      try {
        setStreamingJobList(await getRelations())
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

  const mvDependencyCallback = useCallback(() => {
    if (streamingJobList) {
      return buildDependencyAsEdges(streamingJobList)
    } else {
      return undefined
    }
  }, [streamingJobList])

  const mvDependency = mvDependencyCallback()

  const router = useRouter()

  const retVal = (
    <Flex p={3} height="calc(100vh - 20px)" flexDirection="column">
      <Title>Streaming Graph</Title>
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
            All Nodes
          </Text>
          <Box flex={1} overflowY="scroll">
            <VStack width="full" spacing={1}>
              {streamingJobList?.map((r) => {
                const match = router.query.id === r.id.toString()
                return (
                  <Link href={`?id=${r.id}`} key={r.id}>
                    <Button
                      colorScheme={match ? "teal" : "gray"}
                      color={match ? "teal.600" : "gray.500"}
                      variant={match ? "outline" : "ghost"}
                      width="full"
                      py={0}
                      height={8}
                      justifyContent="flex-start"
                    >
                      {r.name}
                    </Button>
                  </Link>
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
            <StreamGraph
              nodes={mvDependency}
              selectedId={router.query.id as string}
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
