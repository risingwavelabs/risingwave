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
  Select,
  VStack,
} from "@chakra-ui/react"
import Editor from "@monaco-editor/react"
import base64url from "base64url"
import { randomUUID } from "crypto"
import Head from "next/head"
import path from "path"
import { Fragment, useEffect, useState } from "react"
import SpinnerOverlay from "../components/SpinnerOverlay"
import Title from "../components/Title"
import { WorkerNode } from "../proto/gen/common"
import { ListHeapProfilingResponse } from "../proto/gen/monitor_service"
import api from "./api/api"
import { getClusterInfoComputeNode } from "./api/cluster"
import useFetch from "./api/fetch"

const SIDEBAR_WIDTH = 200

interface FileList {
  dir: string
  name: string[]
}

export default function HeapProfiling() {
  const { response: computeNodes } = useFetch(getClusterInfoComputeNode)

  const [computeNodeId, setComputeNodeId] = useState<number>()
  const [displayInfo, setDisplayInfo] = useState<string | undefined>("")
  const [profileList, setProfileList] = useState<
    ListHeapProfilingResponse | undefined
  >()
  const [selectedProfileList, setSelectedProfileList] = useState<
    FileList | undefined
  >()
  const [profileType, setProfileType] = useState<string | undefined>("Auto")
  const [analyzeTargetFileName, setAnalyzeTargetFileName] = useState<
    string | undefined
  >()

  useEffect(() => {
    if (computeNodes && !computeNodeId && computeNodes.length > 0) {
      setComputeNodeId(computeNodes[0].id)
    }
  }, [computeNodes, computeNodeId])

  async function getProfileList(
    computeNodes: WorkerNode[] | undefined,
    computeNodeId: number | undefined
  ) {
    if (computeNodes && computeNodeId && computeNodes.length > 0) {
      try {
        let list: ListHeapProfilingResponse =
          ListHeapProfilingResponse.fromJSON(
            await api.get(`/api/monitor/list_heap_profile/${computeNodeId}`)
          )
        setProfileList(list)
      } catch (e: any) {
        console.error(e)
        let result = `Getting Profiling File List\n$Error: ${e.message}]`
        setDisplayInfo(result)
      }
    }
  }

  useEffect(() => {
    getProfileList(computeNodes, computeNodeId)
  }, [computeNodes, computeNodeId])

  useEffect(() => {
    if (!profileList) {
      return
    }
    if (profileType === "Auto") {
      setSelectedProfileList({
        dir: profileList.dir,
        name: profileList.nameAuto,
      })
    } else if (profileType === "Manually") {
      setSelectedProfileList({
        dir: profileList.dir,
        name: profileList.nameManually,
      })
    } else {
      console.error(`Bad profileType ${profileType}`)
    }
  }, [profileType, profileList])

  useEffect(() => {
    if (!selectedProfileList) {
      return
    }
    if (selectedProfileList.name.length > 0) {
      setAnalyzeTargetFileName(selectedProfileList.name[0])
    }
  }, [selectedProfileList])

  async function dumpProfile() {
    api.get(`/api/monitor/dump_heap_profile/${computeNodeId}`)
    getProfileList(computeNodes, computeNodeId)
  }

  async function analyzeHeapFile() {
    if (
      selectedProfileList === undefined ||
      analyzeTargetFileName === undefined
    ) {
      console.log(
        `selectedProfileList: ${selectedProfileList}, analyzeTargetFileName: ${analyzeTargetFileName}`
      )
      return
    }

    let analyzeFilePath = path.join(
      selectedProfileList.dir,
      analyzeTargetFileName
    )

    setDisplayInfo(
      `Analyzing ${analyzeTargetFileName} from Compute Node ${computeNodeId}`
    )

    const title = `Collapsed Profiling of Compute Node ${computeNodeId} for ${analyzeTargetFileName}`

    let result
    try {
      let analyzeFilePathBase64 = base64url(analyzeFilePath)
      let resObj = await fetch(
        `/api/monitor/analyze/${computeNodeId}/${analyzeFilePathBase64}`
      ).then(async (res) => ({
        filename: res.headers.get("content-disposition"),
        blob: await res.blob(),
      }))
      let objUrl = window.URL.createObjectURL(resObj.blob)
      let link = document.createElement("a")
      link.href = objUrl
      link.download = resObj.filename || randomUUID()
      link.click()
      result = `${title}\n\nDownloaded!`
    } catch (e: any) {
      result = `${title}\n\nError: ${e.message}`
    }

    setDisplayInfo(result)
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
              <FormLabel>Dumped By</FormLabel>
              <Select onChange={(event) => setProfileType(event.target.value)}>
                {["Auto", "Manually"].map((n) => (
                  <option value={n} key={n}>
                    {n}
                  </option>
                ))}
              </Select>
              <FormLabel>Dumped Files</FormLabel>
              <Select
                onChange={(event) =>
                  setAnalyzeTargetFileName(event.target.value)
                }
              >
                {selectedProfileList &&
                  selectedProfileList.name.map((n) => (
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
          {displayInfo === undefined ? (
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
              value={displayInfo}
            ></Editor>
          )}
        </Box>
      </Flex>
    </Flex>
  )

  return (
    <Fragment>
      <Head>
        <title>Heap Profiling</title>
      </Head>
      {retVal}
    </Fragment>
  )
}
