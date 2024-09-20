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

import {
  Box,
  Button,
  Flex,
  FormControl,
  FormLabel,
  Select,
  Text,
  VStack,
} from "@chakra-ui/react"
import _, { reverse, sortBy } from "lodash"
import Head from "next/head"
import { parseAsInteger, useQueryState } from "nuqs"
import { Fragment, useCallback, useEffect, useMemo, useState } from "react"
import RelationGraph, {
  nodeRadius,
} from "../components/RelationGraph"
import Title from "../components/Title"
import useErrorToast from "../hook/useErrorToast"
import useFetch from "../lib/api/fetch"
import {
  calculateBPRate,
  calculateCumulativeBp,
  fetchEmbeddedBackPressure,
  fetchPrometheusBackPressure,
} from "../lib/api/metric"
import {
  Relation,
  getFragmentVertexToRelationMap,
  getRelationDependencies,
  getRelations,
  relationIsStreamingJob,
} from "../lib/api/streaming"
import { RelationPoint } from "../lib/layout"
import { BackPressureInfo } from "../proto/gen/monitor_service"

const SIDEBAR_WIDTH = "200px"
const INTERVAL_MS = 5000

type BackPressureDataSource = "Embedded" | "Prometheus"

// The state of the embedded back pressure metrics.
// The metrics from previous fetch are stored here to calculate the rate.
interface EmbeddedBackPressureInfo {
  previous: BackPressureInfo[]
  current: BackPressureInfo[]
  totalBackpressureNs: BackPressureInfo[]
  totalDurationNs: number
}

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
      width: nodeRadius * 2,
      height: nodeRadius * 2,
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
  const { response: fragmentVertexToRelationMap } = useFetch(
    getFragmentVertexToRelationMap
  )
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

  const [backPressureDataSource, setBackPressureDataSource] =
    useState<BackPressureDataSource>("Embedded")

  // Periodically fetch Prometheus back-pressure from Meta node
  const { response: prometheusMetrics } = useFetch(
    fetchPrometheusBackPressure,
    INTERVAL_MS,
    backPressureDataSource === "Prometheus"
  )

  // Periodically fetch embedded back-pressure from Meta node
  // Didn't call `useFetch()` because the `setState` way is special.
  const [embeddedBackPressureInfo, setEmbeddedBackPressureInfo] =
    useState<EmbeddedBackPressureInfo>()
  useEffect(() => {
    if (resetEmbeddedBackPressures) {
      setEmbeddedBackPressureInfo(undefined)
      toggleResetEmbeddedBackPressures()
    }
    if (backPressureDataSource === "Embedded") {
      const interval = setInterval(() => {
        fetchEmbeddedBackPressure().then(
          (newBP) => {
            setEmbeddedBackPressureInfo((prev) =>
              prev
                ? {
                    previous: prev.current,
                    current: newBP,
                    totalBackpressureNs: calculateCumulativeBp(
                      prev.totalBackpressureNs,
                      prev.current,
                      newBP
                    ),
                    totalDurationNs:
                      prev.totalDurationNs + INTERVAL_MS * 1000 * 1000,
                  }
                : {
                    previous: newBP, // Use current value to show zero rate, but it's fine
                    current: newBP,
                    totalBackpressureNs: [],
                    totalDurationNs: 0,
                  }
            )
          },
          (e) => {
            console.error(e)
            toast(e, "error")
          }
        )
      }, INTERVAL_MS)
      return () => {
        clearInterval(interval)
      }
    }
  }, [backPressureDataSource, toast, resetEmbeddedBackPressures])

  // Get relationId-relationId -> backpressure rate map
  const backPressures: Map<string, number> | undefined = useMemo(() => {
    if (!fragmentVertexToRelationMap) {
      return new Map<string, number>()
    }
    let inMap = fragmentVertexToRelationMap.inMap
    let outMap = fragmentVertexToRelationMap.outMap
    if (prometheusMetrics || embeddedBackPressureInfo) {
      let map = new Map<string, number>()

      if (backPressureDataSource === "Embedded" && embeddedBackPressureInfo) {
        const metrics = calculateBPRate(
          embeddedBackPressureInfo.totalBackpressureNs,
          embeddedBackPressureInfo.totalDurationNs
        )
        for (const m of metrics.outputBufferBlockingDuration) {
          let output = Number(m.metric.fragmentId)
          let input = Number(m.metric.downstreamFragmentId)
          if (outMap[output] && inMap[input]) {
            output = outMap[output]
            input = inMap[input]
            let key = `${output}_${input}`
            map.set(key, m.sample[0].value)
          }
        }
      } else if (backPressureDataSource === "Prometheus" && prometheusMetrics) {
        for (const m of prometheusMetrics.outputBufferBlockingDuration) {
          if (m.sample.length > 0) {
            // Note: We issue an instant query to Prometheus to get the most recent value.
            // So there should be only one sample here.
            //
            // Due to https://github.com/risingwavelabs/risingwave/issues/15280, it's still
            // possible that an old version of meta service returns a range-query result.
            // So we take the one with the latest timestamp here.
            const value = _(m.sample).maxBy((s) => s.timestamp)!.value * 100
            let output = Number(m.metric.fragment_id)
            let input = Number(m.metric.downstream_fragment_id)
            if (outMap[output] && inMap[input]) {
              output = outMap[output]
              input = inMap[input]
              let key = `${output}_${input}`
              map.set(key, value)
            }
          }
        }
      }
      return map
    }
  }, [
    backPressureDataSource,
    prometheusMetrics,
    embeddedBackPressureInfo,
    fragmentVertexToRelationMap,
  ])

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
              {/* NOTE(kwannoel): No need to reset prometheus bp, because it is stateless */}
              <Button onClick={(_) => toggleResetEmbeddedBackPressures()}>
                Reset Back Pressures
              </Button>

              <FormControl>
                <FormLabel fontWeight="semibold" mb={3}>
                  Back Pressure Data Source
                </FormLabel>
                <Select
                  value={backPressureDataSource}
                  onChange={(event) =>
                    setBackPressureDataSource(
                      event.target.value as BackPressureDataSource
                    )
                  }
                  defaultValue="Embedded"
                >
                  <option value="Embedded" key="Embedded">
                    Embedded (5 secs)
                  </option>
                  <option value="Prometheus" key="Prometheus">
                    Prometheus (1 min)
                  </option>
                </Select>
              </FormControl>

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
            <RelationGraph
              nodes={relationDependency}
              selectedId={selectedId?.toString()}
              setSelectedId={(id) => setSelectedId(parseInt(id))}
              backPressures={backPressures}
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
