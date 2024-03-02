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
import _, { sortBy } from "lodash"
import Head from "next/head"
import { parseAsInteger, useQueryState } from "nuqs"
import { Fragment, useCallback, useEffect, useMemo, useState } from "react"
import FragmentDependencyGraph from "../components/FragmentDependencyGraph"
import FragmentGraph from "../components/FragmentGraph"
import Title from "../components/Title"
import useErrorToast from "../hook/useErrorToast"
import useFetch from "../lib/api/fetch"
import {
  BackPressureInfo,
  calculateBPRate,
  fetchEmbeddedBackPressure,
  fetchPrometheusBackPressure,
} from "../lib/api/metric"
import { getFragments, getStreamingJobs } from "../lib/api/streaming"
import { FragmentBox } from "../lib/layout"
import { TableFragments, TableFragments_Fragment } from "../proto/gen/meta"
import { Dispatcher, MergeNode, StreamNode } from "../proto/gen/stream_plan"

interface DispatcherNode {
  [actorId: number]: Dispatcher[]
}

// Refresh interval (ms) for back pressure stats
const INTERVAL = 5000

/** Associated data of each plan node in the fragment graph, including the dispatchers. */
export interface PlanNodeDatum {
  name: string
  children?: PlanNodeDatum[]
  operatorId: string | number
  node: StreamNode | DispatcherNode
  actorIds?: string[]
}

function buildPlanNodeDependency(
  fragment: TableFragments_Fragment
): d3.HierarchyNode<PlanNodeDatum> {
  const firstActor = fragment.actors[0]

  const hierarchyActorNode = (node: StreamNode): PlanNodeDatum => {
    return {
      name: node.nodeBody?.$case?.toString() || "unknown",
      children: (node.input || []).map(hierarchyActorNode),
      operatorId: node.operatorId,
      node,
    }
  }

  let dispatcherName: string

  if (firstActor.dispatcher.length > 0) {
    const firstDispatcherName = _.camelCase(
      firstActor.dispatcher[0].type.replace(/^DISPATCHER_TYPE_/, "")
    )
    if (firstActor.dispatcher.length > 1) {
      if (
        firstActor.dispatcher.every(
          (d) => d.type === firstActor.dispatcher[0].type
        )
      ) {
        dispatcherName = `${firstDispatcherName}Dispatchers`
      } else {
        dispatcherName = "multipleDispatchers"
      }
    } else {
      dispatcherName = `${firstDispatcherName}Dispatcher`
    }
  } else {
    dispatcherName = "noDispatcher"
  }

  const dispatcherNode = fragment.actors.reduce((obj, actor) => {
    obj[actor.actorId] = actor.dispatcher
    return obj
  }, {} as DispatcherNode)

  return d3.hierarchy({
    name: dispatcherName,
    actorIds: fragment.actors.map((a) => a.actorId.toString()),
    children: firstActor.nodes ? [hierarchyActorNode(firstActor.nodes)] : [],
    operatorId: "dispatcher",
    node: dispatcherNode,
  })
}

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
}

export default function Streaming() {
  const { response: relationList } = useFetch(getStreamingJobs)
  const { response: fragmentList } = useFetch(getFragments)

  const [relationId, setRelationId] = useQueryState("id", parseAsInteger)
  const [selectedFragmentId, setSelectedFragmentId] = useState<number>()

  const toast = useErrorToast()

  const fragmentDependencyCallback = useCallback(() => {
    if (fragmentList) {
      if (relationId) {
        const fragments = fragmentList.find((x) => x.tableId === relationId)
        if (fragments) {
          const fragmentDep = buildFragmentDependencyAsEdges(fragments)
          return {
            fragments,
            fragmentDep,
            fragmentDepDag: dagStratify()(fragmentDep),
          }
        }
      }
    }
  }, [fragmentList, relationId])

  useEffect(() => {
    if (relationList) {
      if (!relationId) {
        if (relationList.length > 0) {
          setRelationId(relationList[0].id)
        }
      }
    }
  }, [relationId, relationList, setRelationId])

  const fragmentDependency = fragmentDependencyCallback()?.fragmentDep
  const fragmentDependencyDag = fragmentDependencyCallback()?.fragmentDepDag
  const fragments = fragmentDependencyCallback()?.fragments

  const planNodeDependenciesCallback = useCallback(() => {
    const fragments_ = fragments?.fragments
    if (fragments_) {
      const planNodeDependencies = new Map<
        string,
        d3.HierarchyNode<PlanNodeDatum>
      >()
      for (const fragmentId in fragments_) {
        const fragment = fragments_[fragmentId]
        const dep = buildPlanNodeDependency(fragment)
        planNodeDependencies.set(fragmentId, dep)
      }
      return planNodeDependencies
    }
    return undefined
  }, [fragments?.fragments])

  const planNodeDependencies = planNodeDependenciesCallback()

  const [searchActorId, setSearchActorId] = useState<string>("")
  const [searchFragId, setSearchFragId] = useState<string>("")

  const handleSearchFragment = () => {
    const searchFragIdInt = parseInt(searchFragId)
    if (fragmentList) {
      for (const tf of fragmentList) {
        for (const fragmentId in tf.fragments) {
          if (tf.fragments[fragmentId].fragmentId == searchFragIdInt) {
            setRelationId(tf.tableId)
            setSelectedFragmentId(searchFragIdInt)
            return
          }
        }
      }
    }

    toast(new Error(`Fragment ${searchFragIdInt} not found`))
  }

  const handleSearchActor = () => {
    const searchActorIdInt = parseInt(searchActorId)
    if (fragmentList) {
      for (const tf of fragmentList) {
        for (const fragmentId in tf.fragments) {
          const fragment = tf.fragments[fragmentId]
          for (const actor of fragment.actors) {
            if (actor.actorId == searchActorIdInt) {
              setRelationId(tf.tableId)
              setSelectedFragmentId(fragment.fragmentId)
              return
            }
          }
        }
      }
    }

    toast(new Error(`Actor ${searchActorIdInt} not found`))
  }

  const [backPressureDataSource, setBackPressureDataSource] =
    useState("Embedded")

  // Periodically fetch Prometheus back-pressure from Meta node
  const { response: promethusMetrics } = useFetch(
    fetchPrometheusBackPressure,
    INTERVAL,
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
                  }
                : {
                    previous: newBP, // Use current value to show zero rate, but it's fine
                    current: newBP,
                  }
            )
          },
          (e) => {
            console.error(e)
            toast(e, "error")
          }
        )
      }, INTERVAL)
      return () => {
        clearInterval(interval)
      }
    }
  }, [backPressureDataSource])

  const backPressures = useMemo(() => {
    if (promethusMetrics || embeddedBackPressureInfo) {
      let map = new Map()

      if (backPressureDataSource === "Embedded" && embeddedBackPressureInfo) {
        const metrics = calculateBPRate(
          embeddedBackPressureInfo.current,
          embeddedBackPressureInfo.previous,
          INTERVAL
        )
        // why sorting?
        metrics.outputBufferBlockingDuration = sortBy(
          metrics.outputBufferBlockingDuration,
          (m) => (m.metric.fragmentId, m.metric.downstreamFragmentId)
        )
        for (const m of metrics.outputBufferBlockingDuration) {
          map.set(
            `${m.metric.fragmentId}_${m.metric.downstreamFragmentId}`,
            m.sample[0].value
          )
        }
      } else if (backPressureDataSource === "Prometheus" && promethusMetrics) {
        for (const m of promethusMetrics.outputBufferBlockingDuration) {
          map.set(
            `${m.metric.fragment_id}_${m.metric.downstream_fragment_id}`,
            m.sample[0].value * 100
          )
        }
      }
      return map
    }
  }, [backPressureDataSource, promethusMetrics, embeddedBackPressureInfo])

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
            <FormLabel>Goto</FormLabel>
            <VStack spacing={2}>
              <HStack>
                <Input
                  placeholder="Fragment Id"
                  value={searchFragId}
                  onChange={(event) => setSearchFragId(event.target.value)}
                ></Input>
                <Button onClick={(_) => handleSearchFragment()}>Go</Button>
              </HStack>
              <HStack>
                <Input
                  placeholder="Actor Id"
                  value={searchActorId}
                  onChange={(event) => setSearchActorId(event.target.value)}
                ></Input>
                <Button onClick={(_) => handleSearchActor()}>Go</Button>
              </HStack>
            </VStack>
          </FormControl>
          <FormControl>
            <FormLabel>Back Pressure Data Source</FormLabel>
            <Select
              value={backPressureDataSource}
              onChange={(event) =>
                setBackPressureDataSource(event.target.value)
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
          <Flex height="full" width="full" flexDirection="column">
            <Text fontWeight="semibold">Fragments</Text>
            {fragmentDependencyDag && (
              <Box flex="1" overflowY="scroll">
                <FragmentDependencyGraph
                  svgWidth={SIDEBAR_WIDTH}
                  fragmentDependency={fragmentDependencyDag}
                  onSelectedIdChange={(id) =>
                    setSelectedFragmentId(parseInt(id))
                  }
                  selectedId={selectedFragmentId?.toString()}
                />
              </Box>
            )}
          </Flex>
        </VStack>
        <Box
          flex={1}
          height="full"
          ml={3}
          overflowX="scroll"
          overflowY="scroll"
        >
          <Text fontWeight="semibold">Fragment Graph</Text>
          {planNodeDependencies && fragmentDependency && (
            <FragmentGraph
              selectedFragmentId={selectedFragmentId?.toString()}
              fragmentDependency={fragmentDependency}
              planNodeDependencies={planNodeDependencies}
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
