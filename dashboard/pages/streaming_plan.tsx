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
import { Dag, dagStratify } from "d3-dag"
import Head from "next/head"
import { useRouter } from "next/router"
import { Fragment, useCallback, useEffect, useState } from "react"
import DependencyGraph from "../components/DependencyGraph"
import Title from "../components/Title"
import { TableFragments } from "../proto/gen/meta"
import { getFragments, getMaterializedViews } from "./api/streaming"

function buildFragmentDependencyAsEdges(fragments: TableFragments): Dag {
  const edges = []
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
    edges.push({
      id: fragment.fragmentId.toString(),
      name: `Fragment ${fragment.fragmentId}`,
      parentIds: Array.from(parentIds).map((x) => x.toString()),
    })
  }
  return dagStratify()(edges)
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
        const fragment = fragmentList.find((x) => x.tableId === mvId)
        if (fragment) {
          return buildFragmentDependencyAsEdges(fragment)
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

  const fragmentDependency = fragmentDependencyCallback()

  const retVal = (
    <Flex p={3} height="100vh" flexDirection="column">
      <Title>Streaming Plan</Title>
      <Flex flexDirection="row" height="100%" width="full">
        <VStack
          mr={3}
          spacing={3}
          alignItems="flex-start"
          width={SIDEBAR_WIDTH}
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
          <Box>
            <Text fontWeight="semibold">Plan</Text>
            {fragmentDependency && (
              <DependencyGraph
                svgWidth={SIDEBAR_WIDTH}
                mvDependency={fragmentDependency}
                onSelectedIdChange={(id) => setSelectedFragmentId(parseInt(id))}
                selectedId={selectedFragmentId?.toString()}
              />
            )}
          </Box>
        </VStack>
        <Box flex={1} height="full" ml={3}>
          <Text fontWeight="semibold">Fragment Graph</Text>
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
