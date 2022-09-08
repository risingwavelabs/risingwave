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
  Flex,
  FormControl,
  FormLabel,
  Select,
  Text,
  useToast,
  VStack,
} from "@chakra-ui/react"
import * as d3 from "d3"
import { Dag, dagStratify } from "d3-dag"
import { toLower } from "lodash"
import Head from "next/head"
import { useRouter } from "next/router"
import { Fragment, useCallback, useEffect, useState } from "react"
import DependencyGraph from "../components/DependencyGraph"
import FragmentGraph, { ActorBox } from "../components/FragmentGraph"
import Title from "../components/Title"
import { TableFragments, TableFragments_Fragment } from "../proto/gen/meta"
import { StreamNode } from "../proto/gen/stream_plan"
import { getFragments, getMaterializedViews } from "./api/streaming"

function buildPlanNodeDependency(
  fragment: TableFragments_Fragment
): d3.HierarchyNode<any> {
  const actor = fragment.actors[0]

  const hierarchyActorNode = (node: StreamNode): any => {
    return {
      name: node.nodeBody?.$case.toString() || "unknown",
      children: (node.input || []).map(hierarchyActorNode),
      operatorId: node.operatorId,
    }
  }

  return d3.hierarchy({
    name:
      actor.dispatcher.map((d) => `${toLower(d.type)}Dispatcher`).join(",") ||
      "noDispatcher",
    children: actor.nodes ? [hierarchyActorNode(actor.nodes)] : [],
    operatorId: "dispatcher",
  })
}

function buildFragmentDependencyAsEdges(
  fragments: TableFragments
): Dag<ActorBox, any> {
  const nodes: ActorBox[] = []
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
    for (const actor of fragment.actors) {
      for (const upstreamActorId of actor.upstreamActorId) {
        const upstreamFragmentId = actorToFragmentMapping.get(upstreamActorId)
        if (upstreamFragmentId) {
          parentIds.add(upstreamFragmentId)
        }
      }
    }
    nodes.push({
      id: fragment.fragmentId.toString(),
      name: `Fragment ${fragment.fragmentId}`,
      parentIds: Array.from(parentIds).map((x) => x.toString()),
      width: 200,
      height: 100,
      order: fragment.fragmentId,
    } as ActorBox)
  }
  return dagStratify()(nodes)
}

const SIDEBAR_WIDTH = 200

function useFetch<T>(fetchFn: () => Promise<T>) {
  const [response, setResponse] = useState<T>()
  const toast = useToast()

  useEffect(() => {
    const fetchData = async () => {
      try {
        const abortController = new AbortController()
        const res = await fetchFn()
        setResponse(res)
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
    fetchData()
  }, [toast, fetchFn])

  return { response }
}

export default function Streaming() {
  const { response: mvList } = useFetch(getMaterializedViews)
  const { response: fragmentList } = useFetch(getFragments)

  const [selectedFragmentId, setSelectedFragmentId] = useState<number>()
  const router = useRouter()

  const fragmentDependencyCallback = useCallback(() => {
    if (fragmentList) {
      if (router.query.id) {
        const mvId = parseInt(router.query.id as string)
        const fragments = fragmentList.find((x) => x.tableId === mvId)
        if (fragments) {
          return {
            fragments,
            fragmentDep: buildFragmentDependencyAsEdges(fragments),
          }
        }
      }
    }
    return undefined
  }, [fragmentList, router.query.id])

  useEffect(() => {
    if (mvList) {
      if (!router.query.id) {
        if (mvList.length > 0) {
          router.replace(`?id=${mvList[0].id}`)
        }
      }
    }
    return () => {}
  }, [router, router.query.id, mvList])

  const fragmentDependency = fragmentDependencyCallback()?.fragmentDep
  const fragments = fragmentDependencyCallback()?.fragments

  const planNodeDependencyCallback = useCallback(() => {
    if (selectedFragmentId) {
      const fragment = fragments?.fragments[selectedFragmentId]
      if (fragment) {
        const dep = buildPlanNodeDependency(fragment)
        console.log(dep)
        return dep
      }
    }
    return undefined
  }, [selectedFragmentId, fragments?.fragments])

  const planNodeDependency = planNodeDependencyCallback()

  const retVal = (
    <Flex p={3} height="100vh" flexDirection="column">
      <Title>Streaming Plan</Title>
      <Flex flexDirection="row" height="full" width="full">
        <VStack
          mr={3}
          spacing={3}
          alignItems="flex-start"
          width={SIDEBAR_WIDTH}
          height="full"
        >
          <FormControl>
            <FormLabel>Materialized View</FormLabel>
            <Select
              value={router.query.id}
              onChange={(event) => router.replace(`?id=${event.target.value}`)}
            >
              {mvList &&
                mvList
                  .filter((mv) => !mv.name.startsWith("__"))
                  .map((mv) => (
                    <option value={mv.id} key={mv.name}>
                      ({mv.id}) {mv.name}
                    </option>
                  ))}
            </Select>
          </FormControl>
          <Flex height="full" width="full" flexDirection="column">
            <Text fontWeight="semibold">Plan</Text>
            {fragmentDependency && (
              <Box flex="1" overflowY="scroll">
                <DependencyGraph
                  svgWidth={SIDEBAR_WIDTH}
                  mvDependency={fragmentDependency}
                  onSelectedIdChange={(id) =>
                    setSelectedFragmentId(parseInt(id))
                  }
                  selectedId={selectedFragmentId?.toString()}
                />
              </Box>
            )}
          </Flex>
        </VStack>
        <Flex flex={1} height="full" ml={3} flexDirection="column">
          <Text fontWeight="semibold">Fragment Graph</Text>
          {planNodeDependency && (
            <Box flex="1" overflowX="scroll" overflowY="scroll">
              <FragmentGraph planNodeDependency={planNodeDependency} />
            </Box>
          )}
        </Flex>
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
