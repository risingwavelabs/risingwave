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

import { Box, Button, Flex, Text, VStack } from "@chakra-ui/react"
import { reverse, sortBy } from "lodash"
import Head from "next/head"
import { parseAsInteger, useQueryState } from "nuqs"
import { Fragment, useCallback, useEffect, useMemo, useState } from "react"
import ReactFlowRelationGraph from "../components/ReactFlowRelationGraph"
import Title from "../components/Title"
import useErrorToast from "../hook/useErrorToast"
import api from "../lib/api/api"
import useFetch from "../lib/api/fetch"
import {
  Relation,
  getFragmentToRelationMap,
  getRelationDependencies,
  getRelations,
  relationIsStreamingJob,
} from "../lib/api/streaming"
import { RelationPoint } from "../lib/layout"
import {
  GetStreamingStatsResponse,
  RelationStats,
} from "../proto/gen/monitor_service"
import { ChannelStatsDerived, ChannelStatsSnapshot } from "./fragment_graph"

const SIDEBAR_WIDTH = "200px"
const INTERVAL_MS = 5000

function buildDependencyAsEdges(
  list: Relation[],
  relation_deps: Map<number, number[]>
): RelationPoint[] {
  const edges = []
  const relationSet = new Set(list.map((r) => r.id))
  for (const r of reverse(sortBy(list, "id"))) {
    edges.push({
      id: r.id.toString(),
      name: r.name,
      parentIds: relationIsStreamingJob(r)
        ? relation_deps.has(r.id)
          ? relation_deps
              .get(r.id)
              ?.filter((r) => relationSet.has(r))
              .map((r) => r.toString())
          : []
        : [],
      order: r.id,
      width: 150, // Previously boxWidth
      height: 45, // Previously boxHeight
      relation: r,
    })
  }
  return edges as RelationPoint[]
}

export default function StreamingGraph() {
  const { response: relationList } = useFetch(getRelations)
  // Since dependentRelations will be deprecated, we need to use getRelationDependencies here to separately obtain the dependency relationship.
  const { response: relationDeps } = useFetch(getRelationDependencies)
  const [selectedId, setSelectedId] = useQueryState("id", parseAsInteger)
  const { response: fragmentToRelationMap } = useFetch(getFragmentToRelationMap)
  const [resetEmbeddedBackPressures, setResetEmbeddedBackPressures] =
    useState<boolean>(false)

  const toggleResetEmbeddedBackPressures = () => {
    setResetEmbeddedBackPressures(
      (resetEmbeddedBackPressures) => !resetEmbeddedBackPressures
    )
  }

  const toast = useErrorToast()

  const relationDependencyCallback = useCallback(() => {
    if (relationList && relationDeps) {
      return buildDependencyAsEdges(relationList, relationDeps)
    } else {
      return undefined
    }
  }, [relationList, relationDeps])

  const relationDependency = relationDependencyCallback()

  // Periodically fetch fragment-level back-pressure from Meta node
  const [channelStats, setChannelStats] =
    useState<Map<string, ChannelStatsDerived>>()
  const [relationStats, setRelationStats] = useState<{
    [key: number]: RelationStats
  }>()

  useEffect(() => {
    // The initial snapshot is used to calculate the rate of back pressure
    // It's not used to render the page directly, so we don't need to set it in the state
    let initialSnapshot: ChannelStatsSnapshot | undefined

    if (resetEmbeddedBackPressures) {
      setChannelStats(undefined)
      toggleResetEmbeddedBackPressures()
    }

    function refresh() {
      api.get("/metrics/streaming_stats").then(
        (res) => {
          let response = GetStreamingStatsResponse.fromJSON(res)
          let snapshot = new ChannelStatsSnapshot(
            new Map(Object.entries(response.channelStats)),
            Date.now()
          )
          if (!initialSnapshot) {
            initialSnapshot = snapshot
          } else {
            setChannelStats(snapshot.getRate(initialSnapshot))
          }
          setRelationStats(response.relationStats)
        },
        (e) => {
          console.error(e)
          toast(e, "error")
        }
      )
    }
    refresh() // run once immediately
    const interval = setInterval(refresh, INTERVAL_MS) // and then run every interval
    return () => {
      clearInterval(interval)
    }
  }, [toast, resetEmbeddedBackPressures])

  // Convert fragment-level backpressure rate map to relation-level backpressure rate
  const relationChannelStats: Map<string, ChannelStatsDerived> | undefined =
    useMemo(() => {
      if (!fragmentToRelationMap) {
        return new Map<string, ChannelStatsDerived>()
      }
      let inMap = fragmentToRelationMap.inMap
      let outMap = fragmentToRelationMap.outMap
      if (channelStats) {
        let map = new Map<string, ChannelStatsDerived>()
        for (const [key, stats] of channelStats) {
          const [outputFragment, inputFragment] = key.split("_").map(Number)
          if (outMap[outputFragment] && inMap[inputFragment]) {
            const outputRelation = outMap[outputFragment]
            const inputRelation = inMap[inputFragment]
            let key = `${outputRelation}_${inputRelation}`
            map.set(key, stats)
          }
        }
        return map
      }
    }, [channelStats, fragmentToRelationMap])

  const retVal = (
    <Flex p={3} height="calc(100vh - 20px)" flexDirection="column">
      <Title>Relation Graph</Title>
      <Flex flexDirection="row" height="full">
        <Flex
          width={SIDEBAR_WIDTH}
          height="full"
          maxHeight="full"
          mr={3}
          alignItems="flex-start"
          flexDirection="column"
        >
          <Box flex={1} overflowY="scroll">
            <VStack width={SIDEBAR_WIDTH} align="start" spacing={1}>
              <Button onClick={(_) => toggleResetEmbeddedBackPressures()}>
                Reset Back Pressures
              </Button>

              <Text fontWeight="semibold" mb={3}>
                Relations
              </Text>
              {relationList?.map((r) => {
                const match = selectedId === r.id
                return (
                  <Button
                    key={r.id}
                    colorScheme={match ? "blue" : "gray"}
                    color={match ? "blue.600" : "gray.500"}
                    variant={match ? "outline" : "ghost"}
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
          <Text fontWeight="semibold">Relation Graph</Text>
          {relationDependency && (
            <ReactFlowRelationGraph
              nodes={relationDependency}
              selectedId={selectedId?.toString()}
              setSelectedId={(id: string) => setSelectedId(parseInt(id))}
              channelStats={relationChannelStats}
              relationStats={relationStats}
            />
          )}
        </Box>
      </Flex>
    </Flex>
  )

  return (
    <Fragment>
      <Head>
        <title>Relation Graph</title>
      </Head>
      {retVal}
    </Fragment>
  )
}
