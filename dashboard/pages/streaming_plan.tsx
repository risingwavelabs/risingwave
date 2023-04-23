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
import { Dispatcher, StreamNode } from "../proto/gen/stream_plan"
import useFetch from "./api/fetch"
import { getFragments, getStreamingJobs } from "./api/streaming"
import _ from "lodash"

interface DispatcherNode {
  [actorId: number]: Dispatcher[]
}

/** Associated data of each plan node in the fragment graph, including the dispatchers. */
export interface PlanNodeDatum {
  name: string
  children?: PlanNodeDatum[]
  operatorId: string | number
  node: StreamNode | DispatcherNode
  extraInfo?: string
}

function buildPlanNodeDependency(
  fragment: TableFragments_Fragment
): d3.HierarchyNode<PlanNodeDatum> {
  const firstActor = fragment.actors[0]

  const hierarchyActorNode = (node: StreamNode): PlanNodeDatum => {
    return {
      name: node.nodeBody?.$case.toString() || "unknown",
      children: (node.input || []).map(hierarchyActorNode),
      operatorId: node.operatorId,
      node,
    }
  }

  let dispatcherName = "noDispatcher"
  if (firstActor.dispatcher.length > 1) {
    if (
      firstActor.dispatcher.every(
        (d) => d.type === firstActor.dispatcher[0].type
      )
    ) {
      dispatcherName = `${_.camelCase(firstActor.dispatcher[0].type)}Dispatchers`
    } else {
      dispatcherName = "multipleDispatchers"
    }
  } else if (firstActor.dispatcher.length === 1) {
    dispatcherName = `${_.camelCase(firstActor.dispatcher[0].type)}Dispatcher`
  }

  const dispatcherNode = fragment.actors.reduce((obj, actor) => {
    obj[actor.actorId] = actor.dispatcher
    return obj
  }, {} as DispatcherNode)

  return d3.hierarchy({
    name: dispatcherName,
    extraInfo: `Actor ${fragment.actors.map((a) => a.actorId).join(", ")}`,
    children: firstActor.nodes ? [hierarchyActorNode(firstActor.nodes)] : [],
    operatorId: "dispatcher",
    node: dispatcherNode,
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

export default function Streaming() {
  const { response: relationList } = useFetch(getStreamingJobs)
  const { response: fragmentList } = useFetch(getFragments)

  const [selectedFragmentId, setSelectedFragmentId] = useState<number>()
  const router = useRouter()

  const fragmentDependencyCallback = useCallback(() => {
    if (fragmentList) {
      if (router.query.id) {
        const id = parseInt(router.query.id as string)
        const fragments = fragmentList.find((x) => x.tableId === id)
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
    if (relationList) {
      if (!router.query.id) {
        if (relationList.length > 0) {
          router.replace(`?id=${relationList[0].id}`)
        }
      }
    }
    return () => {}
  }, [router, router.query.id, relationList])

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

  const relationInfoCallback = useCallback(() => {
    const id = router.query.id
    if (id) {
      if (relationList) {
        return relationList.find((x) => x.id == parseInt(id as string))
      }
    }
    return undefined
  }, [relationList, router.query.id])

  const relationInfo = relationInfoCallback()

  const [searchActorId, setSearchActorId] = useState<string>("")
  const [searchFragId, setSearchFragId] = useState<string>("")

  const setRelationId = (id: number) => router.replace(`?id=${id}`)

  const toast = useToast()

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
              value={router.query.id}
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
          <Flex height="full" width="full" flexDirection="column">
            <Text fontWeight="semibold">Plan</Text>
            {relationInfo && (
              <Text>
                {relationInfo.id} - {relationInfo.name}
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
