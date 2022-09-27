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
  Button,
  Flex,
  FormControl,
  FormLabel,
  HStack,
  Input,
  Select,
  Text,
  useToast,
  VStack,
} from "@chakra-ui/react"
import * as d3 from "d3"
import { dagStratify } from "d3-dag"
import { toLower } from "lodash"
import Head from "next/head"
import { useRouter } from "next/router"
import { Fragment, useCallback, useEffect, useState } from "react"
import DependencyGraph from "../components/DependencyGraph"
import FragmentGraph from "../components/FragmentGraph"
import Title from "../components/Title"
import { ActorBox } from "../lib/layout"
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
      node,
    }
  }

  return d3.hierarchy({
    name:
      actor.dispatcher.map((d) => `${toLower(d.type)}Dispatcher`).join(",") ||
      "noDispatcher",
    extraInfo: `Actor ${fragment.actors.map((a) => a.actorId).join(", ")}`,
    children: actor.nodes ? [hierarchyActorNode(actor.nodes)] : [],
    operatorId: "dispatcher",
  })
}

function buildFragmentDependencyAsEdges(fragments: TableFragments): ActorBox[] {
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
      width: 0,
      height: 0,
      order: fragment.fragmentId,
      fragment: fragment,
    } as ActorBox)
  }
  return nodes
}

const SIDEBAR_WIDTH = 200

function useFetch<T>(fetchFn: () => Promise<T>) {
  const [response, setResponse] = useState<T>()
  const toast = useToast()

  useEffect(() => {
    const fetchData = async () => {
      try {
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
          const fragmentDep = buildFragmentDependencyAsEdges(fragments)
          return {
            fragments,
            fragmentDep,
            fragmentDepDag: dagStratify()(fragmentDep),
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
  const fragmentDependencyDag = fragmentDependencyCallback()?.fragmentDepDag
  const fragments = fragmentDependencyCallback()?.fragments

  const planNodeDependenciesCallback = useCallback(() => {
    const fragments_ = fragments?.fragments
    if (fragments_) {
      const planNodeDependencies = new Map<string, d3.HierarchyNode<any>>()
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

  const mvInfoCallback = useCallback(() => {
    const id = router.query.id
    if (id) {
      if (mvList) {
        return mvList.find((x) => x.id == parseInt(id as string))
      }
    }
    return undefined
  }, [mvList, router.query.id])

  const mvInfo = mvInfoCallback()

  const [searchActorId, setSearchActorId] = useState<string>("")
  const [searchFragId, setSearchFragId] = useState<string>("")

  const setMvId = (id: number) => router.replace(`?id=${id}`)

  const toast = useToast()

  const handleSearchFragment = () => {
    const searchFragIdInt = parseInt(searchFragId)
    if (fragmentList) {
      for (const mv of fragmentList) {
        for (const fragmentId in mv.fragments) {
          if (mv.fragments[fragmentId].fragmentId == searchFragIdInt) {
            setMvId(mv.tableId)
            setSelectedFragmentId(searchFragIdInt)
            return
          }
        }
      }
    }

    toast({
      title: "Fragment not found",
      description: "",
      status: "error",
      duration: 5000,
      isClosable: true,
    })
  }

  const handleSearchActor = () => {
    const searchActorIdInt = parseInt(searchActorId)
    if (fragmentList) {
      for (const mv of fragmentList) {
        for (const fragmentId in mv.fragments) {
          const fragment = mv.fragments[fragmentId]
          for (const actor of fragment.actors) {
            if (actor.actorId == searchActorIdInt) {
              setMvId(mv.tableId)
              setSelectedFragmentId(fragment.fragmentId)
              return
            }
          }
        }
      }
    }

    toast({
      title: "Actor not found",
      description: "",
      status: "error",
      duration: 5000,
      isClosable: true,
    })
  }

  const retVal = (
    <Flex p={3} height="calc(100vh - 20px)" flexDirection="column">
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
            <Input
              list="mvList"
              spellCheck={false}
              onChange={(event) => {
                const id = mvList?.find((x) => x.name == event.target.value)?.id
                if (id) {
                  setMvId(id)
                }
              }}
              placeholder="Search..."
              mb={2}
            ></Input>
            <datalist id="mvList">
              {mvList &&
                mvList
                  .filter((mv) => !mv.name.startsWith("__"))
                  .map((mv) => (
                    <option value={mv.name} key={mv.id}>
                      ({mv.id}) {mv.name}
                    </option>
                  ))}
            </datalist>
            <Select
              value={router.query.id}
              onChange={(event) => setMvId(parseInt(event.target.value))}
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
            <Text fontWeight="semibold">Plan</Text>
            {mvInfo && (
              <Text>
                {mvInfo.id} - {mvInfo.name}
              </Text>
            )}
            {fragmentDependencyDag && (
              <Box flex="1" overflowY="scroll">
                <DependencyGraph
                  svgWidth={SIDEBAR_WIDTH}
                  mvDependency={fragmentDependencyDag}
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
