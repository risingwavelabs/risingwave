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
  Table,
  TableContainer,
  Tbody,
  Td,
  Text,
  Tr,
  VStack,
} from "@chakra-ui/react"
import * as d3 from "d3"
import { dagStratify } from "d3-dag"
import _ from "lodash"
import Head from "next/head"
import { parseAsInteger, useQueryState } from "nuqs"
import { Fragment, useCallback, useEffect, useMemo, useState } from "react"
import FragmentDependencyGraph from "../components/FragmentDependencyGraph"
import FragmentGraph from "../components/FragmentGraph"
import Title from "../components/Title"
import useErrorToast from "../hook/useErrorToast"
import api from "../lib/api/api"
import useFetch from "../lib/api/fetch"
import {
  getFragmentsByJobId,
  getRelationIdInfos,
  getStreamingJobs,
} from "../lib/api/streaming"
import { FragmentBox } from "../lib/layout"
import { TableFragments, TableFragments_Fragment } from "../proto/gen/meta"
import { BackPressureInfo, FragmentStats } from "../proto/gen/monitor_service"
import { Dispatcher, MergeNode, StreamNode } from "../proto/gen/stream_plan"

interface DispatcherNode {
  [actorId: number]: Dispatcher[]
  fragment: TableFragments_Fragment
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

  let dispatcherNode = fragment.actors.reduce((obj, actor) => {
    obj[actor.actorId] = actor.dispatcher
    return obj
  }, {} as DispatcherNode)
  dispatcherNode.fragment = {
    ...fragment,
    actors: [],
  }

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

const SIDEBAR_WIDTH = 225

export class BackPressureSnapshot {
  // The first fetch result.
  // key: `<fragmentId>_<downstreamFragmentId>`
  // value: output blocking duration in nanoseconds.
  result: Map<string, number>

  // The time of the current fetch in milliseconds. (`Date.now()`)
  time: number

  constructor(result: Map<string, number>, time: number) {
    this.result = result
    this.time = time
  }

  static fromResponse(channelStats: {
    [key: string]: BackPressureInfo
  }): BackPressureSnapshot {
    const result = new Map<string, number>()
    for (const [key, info] of Object.entries(channelStats)) {
      result.set(key, info.value / info.actorCount)
    }
    return new BackPressureSnapshot(result, Date.now())
  }

  getRate(initial: BackPressureSnapshot): Map<string, number> {
    const result = new Map<string, number>()
    for (const [key, value] of this.result) {
      const initialValue = initial.result.get(key)
      if (initialValue) {
        result.set(
          key,
          (value - initialValue) / (this.time - initial.time) / 1000000
        )
      }
    }
    return result
  }
}

export default function Streaming() {
  const { response: streamingJobList } = useFetch(getStreamingJobs)
  const { response: relationIdInfos } = useFetch(getRelationIdInfos)

  const [jobId, setJobId] = useQueryState("id", parseAsInteger)
  const [selectedFragmentId, setSelectedFragmentId] = useState<number>()
  const [tableFragments, setTableFragments] = useState<TableFragments>()

  const job = useMemo(
    () => streamingJobList?.find((j) => j.id === jobId),
    [streamingJobList, jobId]
  )

  const toast = useErrorToast()

  useEffect(() => {
    if (jobId) {
      setTableFragments(undefined)
      getFragmentsByJobId(jobId).then((tf) => {
        setTableFragments(tf)
      })
    }
  }, [jobId])

  const fragmentDependencyCallback = useCallback(() => {
    if (tableFragments) {
      const fragmentDep = buildFragmentDependencyAsEdges(tableFragments)
      return {
        fragments: tableFragments,
        fragmentDep,
        fragmentDepDag: dagStratify()(fragmentDep),
      }
    }
  }, [tableFragments])

  useEffect(() => {
    if (streamingJobList) {
      if (!jobId) {
        if (streamingJobList.length > 0) {
          setJobId(streamingJobList[0].id)
        }
      }
    }
  }, [jobId, streamingJobList, setJobId])

  // The table fragments of the selected fragment id
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
    if (relationIdInfos) {
      let map = relationIdInfos.map
      for (const relationId in map) {
        const fragmentIdToRelationId = map[relationId].map
        for (const fragmentId in fragmentIdToRelationId) {
          if (parseInt(fragmentId) == searchFragIdInt) {
            setJobId(parseInt(relationId))
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
    if (relationIdInfos) {
      let map = relationIdInfos.map
      for (const relationId in map) {
        const fragmentIdToRelationId = map[relationId].map
        for (const fragmentId in fragmentIdToRelationId) {
          let actorIds = fragmentIdToRelationId[fragmentId].ids
          if (actorIds.includes(searchActorIdInt)) {
            setJobId(parseInt(relationId))
            setSelectedFragmentId(parseInt(fragmentId))
            return
          }
        }
      }
    }
    toast(new Error(`Actor ${searchActorIdInt} not found`))
  }

  // Keep the initial snapshot to calculate the rate of back pressure
  const [backPressureRate, setBackPressureRate] =
    useState<Map<string, number>>()

  const [fragmentStats, setFragmentStats] = useState<{
    [key: number]: FragmentStats
  }>()

  useEffect(() => {
    // The initial snapshot is used to calculate the rate of back pressure
    // It's not used to render the page directly, so we don't need to set it in the state
    let initialSnapshot: BackPressureSnapshot | undefined

    function refresh() {
      api.get("/metrics/fragment/embedded_back_pressures").then(
        (response) => {
          let snapshot = BackPressureSnapshot.fromResponse(
            response.channelStats
          )
          if (!initialSnapshot) {
            initialSnapshot = snapshot
          } else {
            setBackPressureRate(snapshot.getRate(initialSnapshot!))
          }
          setFragmentStats(response.fragmentStats)
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
  }, [toast])

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
            <FormLabel>Streaming Jobs</FormLabel>
            <Input
              list="relationList"
              spellCheck={false}
              onChange={(event) => {
                const id = streamingJobList?.find(
                  (x) => x.name == event.target.value
                )?.id
                if (id) {
                  setJobId(id)
                }
              }}
              placeholder="Search..."
              mb={2}
            ></Input>
            <datalist id="relationList">
              {streamingJobList &&
                streamingJobList.map((r) => (
                  <option value={r.name} key={r.id}>
                    ({r.id}) {r.name}
                  </option>
                ))}
            </datalist>
            <Select
              value={jobId ?? undefined}
              onChange={(event) => setJobId(parseInt(event.target.value))}
            >
              {streamingJobList &&
                streamingJobList.map((r) => (
                  <option value={r.id} key={r.name}>
                    ({r.id}) {r.name}
                  </option>
                ))}
            </Select>
          </FormControl>
          {job && (
            <FormControl>
              <FormLabel>Information</FormLabel>
              <TableContainer>
                <Table size="sm">
                  <Tbody>
                    <Tr>
                      <Td fontWeight="medium">Type</Td>
                      <Td isNumeric>{job.type}</Td>
                    </Tr>
                    <Tr>
                      <Td fontWeight="medium">Status</Td>
                      <Td isNumeric>{job.jobStatus}</Td>
                    </Tr>
                    <Tr>
                      <Td fontWeight="medium">Parallelism</Td>
                      <Td isNumeric>{job.parallelism}</Td>
                    </Tr>
                    <Tr>
                      <Td fontWeight="medium" paddingEnd={0}>
                        Max Parallelism
                      </Td>
                      <Td isNumeric>{job.maxParallelism}</Td>
                    </Tr>
                  </Tbody>
                </Table>
              </TableContainer>
            </FormControl>
          )}
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
              backPressures={backPressureRate}
              fragmentStats={fragmentStats}
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
