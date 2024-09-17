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
  HStack,
  Input,
  Select,
  Text,
  VStack,
} from "@chakra-ui/react"
import * as d3 from "d3"
import { dagStratify } from "d3-dag"
import _ from "lodash"
import Head from "next/head"
import { parseAsInteger, useQueryState } from "nuqs"
import { Fragment, useCallback, useEffect, useMemo, useState } from "react"
import FragmentDependencyGraph from "../components/FragmentDependencyGraph"
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
  getFragmentsByJobId, getFragmentVertexToRelationMap, getRelationDependencies,
  getRelationIdInfos,
  getStreamingJobs, Relation, StreamingJob,
} from "../lib/api/streaming"
import {DdlBox, FragmentBox} from "../lib/layout"
import { TableFragments, TableFragments_Fragment } from "../proto/gen/meta"
import { BackPressureInfo } from "../proto/gen/monitor_service"
import { Dispatcher, MergeNode, StreamNode } from "../proto/gen/stream_plan"

interface DispatcherNode {
  [actorId: number]: Dispatcher[]
}

// Refresh interval (ms) for back pressure stats
const INTERVAL_MS = 5000

/** Associated data of each plan node in the fragment graph, including the dispatchers. */
export interface PlanNodeDatum {
  name: string
  children?: PlanNodeDatum[]
  operatorId: string | number
  node: StreamNode | DispatcherNode
  actorIds?: string[]
}

const SampleDdlDependencyGraph: DdlBox[] = [
  {
    id: "1",
    order: 1, // preference order, item with larger order will be placed at right or down
    width: 100,
    height: 100,
    parentIds: [],
    ddl_name: "table",
    schema_name: "s1",
  },
  {
    id: "2",
    order: 2,
    width: 100,
    height: 100,
    parentIds: ["1"],
    ddl_name: "mv1",
    schema_name: "s1",
  },
  {
    id: "3",
    order: 3,
    width: 100,
    height: 100,
    parentIds: ["1"],
    ddl_name: "mv2",
    schema_name: "s1",
  },
  {
    id: "4",
    order: 4,
    width: 100,
    height: 100,
    parentIds: ["2", "3"],
    ddl_name: "mv3",
    schema_name: "s1",
  }
]

const SampleDdlBackpressures: Map<string, number> = new Map([
  ["1_2", 0.1],
  ["1_3", 0.2],
  ["2_4", 0.3],
  ["3_4", 0.4],
])

function findMergeNodes(root: StreamNode): MergeNode[] {
  let mergeNodes = new Set<MergeNode>()

  const findMergeNodesRecursive = (node: StreamNode) => {
    if (node.nodeBody?.$case === "merge") {
      mergeNodes.add(node.nodeBody.merge)
    }
    for (const child of node.input || []) {
      findMergeNodesRecursive(child)
    }
  }

  findMergeNodesRecursive(root)
  return Array.from(mergeNodes)
}

function buildFragmentDependencyAsEdges(
  fragments: TableFragments
): FragmentBox[] {
  const nodes: FragmentBox[] = []
  const actorToFragmentMapping = new Map<number, number>()
  for (const fragmentId in fragments.fragments) {
    const fragment = fragments.fragments[fragmentId]
    for (const actor of fragment.actors) {
      actorToFragmentMapping.set(actor.actorId, actor.fragmentId)
    }
  }
  for (const id in fragments.fragments) {
    const fragment = fragments.fragments[id]
    const parentIds = new Set<number>()
    const externalParentIds = new Set<number>()

    for (const actor of fragment.actors) {
      for (const upstreamActorId of actor.upstreamActorId) {
        const upstreamFragmentId = actorToFragmentMapping.get(upstreamActorId)
        if (upstreamFragmentId) {
          parentIds.add(upstreamFragmentId)
        } else {
          for (const m of findMergeNodes(actor.nodes!)) {
            externalParentIds.add(m.upstreamFragmentId)
          }
        }
      }
    }
    nodes.push({
      id: fragment.fragmentId.toString(),
      name: `Fragment ${fragment.fragmentId}`,
      parentIds: Array.from(parentIds).map((x) => x.toString()),
      externalParentIds: Array.from(externalParentIds).map((x) => x.toString()),
      width: 0,
      height: 0,
      order: fragment.fragmentId,
      fragment,
    })
  }
  return nodes
}

function buildDdlDependencyAsEdges(
    relations: StreamingJob[],
): DdlBox[] {
  // Filter out non-streaming relations, e.g. source, views.
  let relation_ids = new Set<number>()
  for (const relation of relations) {
      relation_ids.add(relation.id)
  }
  console.log("relation_ids: ", relation_ids)
  const nodes: DdlBox[] = []
  for (const relation of relations) {
    let parentIds = relation.dependentRelations
    console.log("parentIds: ", parentIds)
    nodes.push({
      id: relation.id.toString(),
      order: relation.id,
      width: 0,
      height: 0,
      parentIds: parentIds.filter((x) => relation_ids.has(x)).map((x) => x.toString()),
      ddl_name: relation.name,
      schema_name: relation.schemaName
    })
  }
  return nodes
}

const SIDEBAR_WIDTH = 200

type BackPressureDataSource = "Embedded" | "Prometheus"
const backPressureDataSources: BackPressureDataSource[] = [
  "Embedded",
  "Prometheus",
]

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
  const { response: fragmentVertexToRelationMap } = useFetch(getFragmentVertexToRelationMap)

  const [relationId, setRelationId] = useQueryState("id", parseAsInteger)

  const toast = useErrorToast()

  const ddlDependencyCallback = useCallback(() => {
    if (relationList) {
      if (relationDeps) {
        console.log(relationDeps)
        const ddlDep = buildDdlDependencyAsEdges(relationList)
        return {
          ddlDep,
        }
      }
    }
  }, [relationList, relationDeps])

  useEffect(() => {
    if (relationList) {
      if (!relationId) {
        if (relationList.length > 0) {
          setRelationId(relationList[0].id)
        }
      }
    }
  }, [relationId, relationList, setRelationId])

  const ddlDependency = ddlDependencyCallback()?.ddlDep
  console.log("ddlDependency: ", ddlDependency)

  const [backPressureDataSource, setBackPressureDataSource] =
    useState<BackPressureDataSource>("Embedded")

  // Periodically fetch Prometheus back-pressure from Meta node
  const { response: promethusMetrics } = useFetch(
    fetchPrometheusBackPressure,
    INTERVAL_MS,
    backPressureDataSource === "Prometheus"
  )

  // Periodically fetch embedded back-pressure from Meta node
  // Didn't call `useFetch()` because the `setState` way is special.
  const [embeddedBackPressureInfo, setEmbeddedBackPressureInfo] =
    useState<EmbeddedBackPressureInfo>()
  useEffect(() => {
    if (backPressureDataSource === "Embedded") {
      const interval = setInterval(() => {
        fetchEmbeddedBackPressure().then(
          (newBP) => {
            console.log(newBP)
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
  }, [backPressureDataSource, toast])

  // Map from (fragment_id, downstream_fragment_id) -> back pressure rate
  const backPressures = useMemo(() => {
    if (!fragmentVertexToRelationMap) {
      return
    }
    let inMap = fragmentVertexToRelationMap.inMap
    let outMap = fragmentVertexToRelationMap.outMap
    console.log("fragmentVertexToRelationMap", fragmentVertexToRelationMap);
    if (promethusMetrics || embeddedBackPressureInfo) {
      let map = new Map()

      if (backPressureDataSource === "Embedded" && embeddedBackPressureInfo) {
        const metrics = calculateBPRate(
          embeddedBackPressureInfo.totalBackpressureNs,
          embeddedBackPressureInfo.totalDurationNs
        )
        for (const m of metrics.outputBufferBlockingDuration) {
          let output = m.metric.fragmentId
          let input = m.metric.downstreamFragmentId
          if (outMap[output] && inMap[input]) {
            output = outMap[output]
            input = inMap[input]
            let key = `${output}_${input}`
            map.set(
                key,
                m.sample[0].value
            )
          }
        }
      } else if (backPressureDataSource === "Prometheus" && promethusMetrics) {
        for (const m of promethusMetrics.outputBufferBlockingDuration) {
          if (m.sample.length > 0) {
            // Note: We issue an instant query to Prometheus to get the most recent value.
            // So there should be only one sample here.
            //
            // Due to https://github.com/risingwavelabs/risingwave/issues/15280, it's still
            // possible that an old version of meta service returns a range-query result.
            // So we take the one with the latest timestamp here.
            const value = _(m.sample).maxBy((s) => s.timestamp)!.value * 100
            map.set(
              `${m.metric.fragment_id}_${m.metric.downstream_fragment_id}`,
              value
            )
          }
        }
      }
      console.log("backpressure map: ", map)
      return map
    }
  }, [backPressureDataSource, promethusMetrics, embeddedBackPressureInfo, fragmentVertexToRelationMap])

  const retVal = (
    <Flex p={3} height="calc(100vh - 20px)" flexDirection="column">
      <Title>Fragment Graph</Title>
      <Flex flexDirection="row" height="full" width="full">
        <VStack
          mr={3}
          spacing={3}
          alignItems="flex-start"
          width={SIDEBAR_WIDTH}
          height="full"
        >
          <FormControl>
            <FormLabel>Relations</FormLabel>
            <Input
              list="relationList"
              spellCheck={false}
              onChange={(event) => {
                const id = relationList?.find(
                  (x) => x.name == event.target.value
                )?.id
                if (id) {
                  setRelationId(id)
                }
              }}
              placeholder="Search..."
              mb={2}
            ></Input>
            <datalist id="relationList">
              {relationList &&
                relationList.map((r) => (
                  <option value={r.name} key={r.id}>
                    ({r.id}) {r.name}
                  </option>
                ))}
            </datalist>
            <Select
              value={relationId ?? undefined}
              onChange={(event) => setRelationId(parseInt(event.target.value))}
            >
              {relationList &&
                relationList.map((r) => (
                  <option value={r.id} key={r.name}>
                    ({r.id}) {r.name}
                  </option>
                ))}
            </Select>
          </FormControl>
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
        </VStack>
        <Box
          flex={1}
          height="full"
          ml={3}
          overflowX="scroll"
          overflowY="scroll"
        >
          <Text fontWeight="semibold">Fragment Graph</Text>
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
        <title>Streaming Fragments</title>
      </Head>
      {retVal}
    </Fragment>
  )
}
