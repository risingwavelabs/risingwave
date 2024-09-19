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
import _ from "lodash"
import Head from "next/head"
import { Fragment, useCallback, useEffect, useMemo, useState } from "react"
import DdlGraph from "../components/DdlGraph"
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
  StreamingJob,
  getFragmentVertexToRelationMap,
  getSchemas,
  getStreamingJobs, getRelationDependencies,
} from "../lib/api/streaming"
import { DdlBox } from "../lib/layout"
import { BackPressureInfo } from "../proto/gen/monitor_service"

// Refresh interval (ms) for back pressure stats
const INTERVAL_MS = 5000

function buildDdlDependencyAsEdges(relations: StreamingJob[], relationDeps: Map<number, number[]>): DdlBox[] {
  // Filter out non-streaming relations, e.g. source, views.
  let relationIds = new Set<number>()
  for (const relation of relations) {
    relationIds.add(relation.id)
  }
  const nodes: DdlBox[] = []
  for (const relation of relations) {
    let parentIds = relationDeps.get(relation.id)
    nodes.push({
      id: relation.id.toString(),
      order: relation.id,
      width: 0,
      height: 0,
      parentIds: parentIds
          ? parentIds
              .filter((x) => relationIds.has(x))
              .map((x) => x.toString())
          : [],
      ddlName: relation.name,
      schemaName: relation.schemaName ? relation.schemaName : "",
    })
  }
  return nodes
}

const SIDEBAR_WIDTH = 200

type BackPressureDataSource = "Embedded" | "Prometheus"

// The state of the embedded back pressure metrics.
// The metrics from previous fetch are stored here to calculate the rate.
interface EmbeddedBackPressureInfo {
  previous: BackPressureInfo[]
  current: BackPressureInfo[]
  totalBackpressureNs: BackPressureInfo[]
  totalDurationNs: number
}

export default function Streaming() {
  const { response: relationList } = useFetch(getStreamingJobs)
  const { response: relationDeps } = useFetch(getRelationDependencies)
  const { response: fragmentVertexToRelationMap } = useFetch(
    getFragmentVertexToRelationMap
  )
  const { response: schemas } = useFetch(getSchemas)
  const [resetEmbeddedBackPressures, setResetEmbeddedBackPressures] =
    useState<boolean>(false)

  const toast = useErrorToast()

  const toggleResetEmbeddedBackPressures = () => {
    setResetEmbeddedBackPressures(
      (resetEmbeddedBackPressures) => !resetEmbeddedBackPressures
    )
  }

  const ddlDependencyCallback = useCallback(() => {
    if (relationList) {
      if (relationDeps) {
        if (schemas) {
          let relationListWithSchemaName = relationList.map((relation) => {
            let schemaName = schemas.find(
                (schema) => schema.id === relation.schemaId
            )?.name
            return { ...relation, schemaName }
          })
          const ddlDep = buildDdlDependencyAsEdges(relationListWithSchemaName, relationDeps)
          return {
            ddlDep,
          }
        }
      }
    }
  }, [relationList, relationDeps, schemas])

  const ddlDependency = ddlDependencyCallback()?.ddlDep

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
  const backPressures = useMemo(() => {
    if (!fragmentVertexToRelationMap) {
      return
    }
    let inMap = fragmentVertexToRelationMap.inMap
    let outMap = fragmentVertexToRelationMap.outMap
    if (prometheusMetrics || embeddedBackPressureInfo) {
      let map = new Map()

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
      <Title>Ddl Graph</Title>
      <Flex flexDirection="row" height="full" width="full">
        <VStack
          mr={3}
          spacing={3}
          alignItems="flex-start"
          width={SIDEBAR_WIDTH}
          height="full"
        >
          <FormControl>
            <FormLabel>Back Pressure Data Source</FormLabel>
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
          {/* NOTE(kwannoel): No need to reset prometheus bp, because it is stateless */}
          <Button onClick={(_) => toggleResetEmbeddedBackPressures()}>
            Reset Back Pressures
          </Button>
        </VStack>
        <Box
          flex={1}
          height="full"
          ml={3}
          overflowX="scroll"
          overflowY="scroll"
        >
          <Text fontWeight="semibold">Ddl Graph</Text>
          {ddlDependency && (
            <DdlGraph
              ddlDependency={ddlDependency}
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
        <title>Ddl Graph</title>
      </Head>
      {retVal}
    </Fragment>
  )
}
