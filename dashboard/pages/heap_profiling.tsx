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
  Input,
  Select,
  VStack,
} from "@chakra-ui/react"
import Editor from "@monaco-editor/react"
import _ from "lodash"
import Head from "next/head"
import { Fragment, useEffect, useState } from "react"
import SpinnerOverlay from "../components/SpinnerOverlay"
import Title from "../components/Title"
import { ListHeapProfilingResponse } from "../proto/gen/monitor_service"
import api from "./api/api"
import { getClusterInfoComputeNode } from "./api/cluster"
import useFetch from "./api/fetch"
import base64url from "base64url"
import path from "path"

const SIDEBAR_WIDTH = 200

export default function HeapProfiling() {
  const { response: computeNodes } = useFetch(getClusterInfoComputeNode)

  const [computeNodeId, setComputeNodeId] = useState<number>()
  const [dump, setDump] = useState<string | undefined>("")
  const [profileCollapsed, setProfileCollapsed] = useState<string | undefined>("")
  const [profileList, setProfileList] = useState<ListHeapProfilingResponse | undefined>()
  const [analyzeTargetFileName, setAnalyzeTargetFileName] = useState<string | undefined>()

  useEffect(() => {
    if (computeNodes && !computeNodeId && computeNodes.length > 0) {
      setComputeNodeId(computeNodes[0].id)

    }
  }, [computeNodes, computeNodeId])

  useEffect(() => {
    if (computeNodes && computeNodeId && computeNodes.length > 0) {
      const getProfileList = async () => {
        try {
          let list: ListHeapProfilingResponse = ListHeapProfilingResponse.fromJSON(await api.get(`/api/monitor/list_heap_profile/${computeNodeId}`))
          setProfileList(list)
        } catch (e: any) {
          console.error(e)
          let result = `Getting Profiling File List\n$Error: ${e.message}]`
          setProfileCollapsed(result)
        }
      }
      getProfileList()
    }
  }, [computeNodes, computeNodeId])

  async function dumpProfile() {
    const title = `Dumping Profiling at Compute Node ${computeNodeId}`

    let result
    try {
      api.get(`/api/monitor/dump_heap_profile/${computeNodeId}`)
      result = `${title}\n\nSucceed!`
    } catch (e: any) {
      console.error(e)
      result = `${title}\n\nError: ${e.message}`
    }
    setProfileCollapsed(result)
  }

  async function analyzeHeapFile() {
    if (profileList === undefined || analyzeTargetFileName === undefined) {
      return
    }

    let analyzeFilePath = path.join(profileList.dir, analyzeTargetFileName)

    const title = `Collapsed Profiling of Compute Node ${computeNodeId} for ${analyzeTargetFileName}`

    let result
    try {
      let analyzeFilePathBase64 = base64url(analyzeFilePath)
      const response: string = await api.get(`/api/monitor/analyze/${computeNodeId}/${analyzeFilePathBase64}`)
      result = `${title}\n\n${response}`
    } catch (e: any) {
      result = `${title}\n\nError: ${e.message}`
    }

    setProfileCollapsed(result)
  }

  const retVal = (
    <Flex p={3} height="calc(50vh - 20px)" flexDirection="column">
      <Title>Heap Profiling</Title>
      <Flex flexDirection="row" height="full" width="full">
        <VStack
          mr={3}
          spacing={3}
          alignItems="flex-start"
          width={SIDEBAR_WIDTH}
          height="full"
          >
          <FormControl>
            <FormLabel textColor="teal.500">Dump Heap Profile</FormLabel>
            <VStack>
              <FormLabel>Compute Nodes</FormLabel>
              <Select
                onChange={(event) =>
                  setComputeNodeId(parseInt(event.target.value))
                }
              >
                {computeNodes &&
                  computeNodes.map((n) => (
                    <option value={n.id} key={n.id}>
                      ({n.id}) {n.host?.host}:{n.host?.port}
                    </option>
                  ))}
              </Select>
              <Button onClick={(_) => dumpProfile()} width="full">
                Dump
              </Button>
            </VStack>
          </FormControl>
          <FormControl>
            <FormLabel textColor="teal.500">Analyze Heap Profile</FormLabel>
            <VStack>
              <FormLabel>Dumped Files</FormLabel>
              <Select
                onChange={(event) =>
                  setAnalyzeTargetFileName(event.target.value)
                }
              >
                {profileList &&
                  profileList.name.map((n) => (
                    <option value={n} key={n}>
                      {n}
                    </option>
                  ))}
              </Select>
              <Button onClick={(_) => analyzeHeapFile()} width="full">
                Analyze
              </Button>
            </VStack>
          </FormControl>
        </VStack>
        <Box
          flex={1}
          height="full"
          ml={3}
          overflowX="scroll"
          overflowY="scroll"
        >
          {dump === undefined ? (
            <SpinnerOverlay></SpinnerOverlay>
          ) : (
            <Editor
              language="sql"
              options={{
                fontSize: 13,
                readOnly: true,
                renderWhitespace: "boundary",
                wordWrap: "on",
              }}
              defaultValue='Select a compute node and target profiling result file and click "Analyze"...'
              value={profileCollapsed}
            ></Editor>
          )}
        </Box>
      </Flex>

    </Flex>
  )

  return (
    <Fragment>
      <Head>
        <title>Await Tree Dump</title>
      </Head>
      {retVal}
    </Fragment>
  )
}
