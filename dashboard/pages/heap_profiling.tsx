/*
 * Copyright 2025 RisingWave Labs
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
import { saveAs } from "file-saver"
import Head from "next/head"
import path from "path"
import { Fragment, useCallback, useEffect, useState } from "react"
import SpinnerOverlay from "../components/SpinnerOverlay"
import Title from "../components/Title"
import api from "../lib/api/api"
import { getClusterInfoProfileWorkers } from "../lib/api/cluster"
import useFetch from "../lib/api/fetch"
import { WorkerNode, WorkerType } from "../proto/gen/common"
import { ListHeapProfilingResponse } from "../proto/gen/monitor_service"

const SIDEBAR_WIDTH = 200

interface FileList {
  dir: string
  name: string[]
}

const workerTypeLabel = (workerType: WorkerType) => {
  switch (workerType) {
    case WorkerType.WORKER_TYPE_FRONTEND:
      return "Frontend"
    case WorkerType.WORKER_TYPE_COMPUTE_NODE:
      return "Compute"
    case WorkerType.WORKER_TYPE_COMPACTOR:
      return "Compactor"
    default:
      return "Other"
  }
}

const getWorkerLabel = (
  nodeId: number | undefined,
  workerNodes: WorkerNode[] | undefined
) => {
  if (nodeId === undefined) {
    return "Worker Node"
  }
  const node = (workerNodes ?? []).find((n) => n.id === nodeId)
  if (!node) {
    return `Worker Node ${nodeId}`
  }
  return `Worker Node ${nodeId} (${workerTypeLabel(node.type)})`
}

export default function HeapProfiling() {
  const { response: workerNodes } = useFetch(getClusterInfoProfileWorkers)

  const [workerNodeId, setWorkerNodeId] = useState<number>()
  const [displayInfo, setDisplayInfo] = useState<string | undefined>("")
  const [profileList, setProfileList] = useState<
    ListHeapProfilingResponse | undefined
  >()
  const [analyzeTargetFileName, setAnalyzeTargetFileName] = useState<
    string | undefined
  >()

  useEffect(() => {
    if (workerNodes && !workerNodeId && workerNodes.length > 0) {
      setWorkerNodeId(workerNodes[0].id)
    }
  }, [workerNodes, workerNodeId])

  const getProfileList = useCallback(async function getProfileList(
    workerNodes: WorkerNode[] | undefined,
    workerNodeId: number | undefined
  ) {
    if (workerNodes && workerNodeId && workerNodes.length > 0) {
      try {
        let list: ListHeapProfilingResponse =
          ListHeapProfilingResponse.fromJSON(
            await api.get(`/monitor/list_heap_profile/${workerNodeId}`)
          )
        list.nameAuto.sort().reverse()
        list.nameManually.sort().reverse()
        setProfileList(list)
        setDisplayInfo(
          `Successfully loaded profiling file list from ${getWorkerLabel(
            workerNodeId,
            workerNodes
          )}\n\nFound ${list.nameAuto.length} auto and ${
            list.nameManually.length
          } manually dumped files.`
        )
      } catch (e: any) {
        console.error(e)
        let result = `Failed to load profiling file list from ${getWorkerLabel(
          workerNodeId,
          workerNodes
        )}\n\nError: ${e.message}\nCause: ${e.cause}`
        setDisplayInfo(result)
      }
    }
  },
  [])

  useEffect(() => {
    getProfileList(workerNodes, workerNodeId)
  }, [getProfileList, workerNodes, workerNodeId])

  useEffect(() => {
    if (!profileList) {
      return
    }
    if (profileList.nameAuto.length > 0) {
      setAnalyzeTargetFileName(profileList.nameAuto[0])
    } else if (profileList.nameManually.length > 0) {
      setAnalyzeTargetFileName(profileList.nameManually[0])
    } else {
      setAnalyzeTargetFileName(undefined)
    }
  }, [profileList])

  async function dumpProfile() {
    try {
      await api.get(`/monitor/dump_heap_profile/${workerNodeId}`)
      getProfileList(workerNodes, workerNodeId)
    } catch (e: any) {
      setDisplayInfo(
        `Dumping heap profile on ${getWorkerLabel(
          workerNodeId,
          workerNodes
        )}.\n\nError: ${e.message}\n${e.cause}`
      )
    }
  }

  async function analyzeHeapFile() {
    if (profileList === undefined || analyzeTargetFileName === undefined) {
      console.log(
        `profileList: ${profileList}, analyzeTargetFileName: ${analyzeTargetFileName}`
      )
      return
    }

    let analyzeFilePath = path.join(profileList.dir, analyzeTargetFileName)

    const workerLabel = getWorkerLabel(workerNodeId, workerNodes)
    setDisplayInfo(`Analyzing ${analyzeTargetFileName} from ${workerLabel}`)

    const title = `Collapsed Profiling of ${workerLabel} for ${analyzeTargetFileName}`

    let result
    try {
      let analyzeFilePathBase64 = base64url(analyzeFilePath)
      const url = api.urlFor(
        `/monitor/analyze/${workerNodeId}/${analyzeFilePathBase64}`
      )
      const res = await fetch(url)
      if (!res.ok) {
        throw Error(`${res.status} ${res.statusText}`)
      }
      const blob = await res.blob()
      saveAs(blob, `${analyzeTargetFileName}.collapsed`)
      result = `${title}\n\nDownloaded!`
    } catch (e: any) {
      result = `${title}\n\nError: ${e.message}`
    }

    setDisplayInfo(result)
  }

  const workerTypeOrder = [
    WorkerType.WORKER_TYPE_COMPUTE_NODE,
    WorkerType.WORKER_TYPE_FRONTEND,
    WorkerType.WORKER_TYPE_COMPACTOR,
  ]
  const groupedWorkerNodes = (workerNodes ?? []).reduce((groups, node) => {
    const list = groups.get(node.type) ?? []
    list.push(node)
    groups.set(node.type, list)
    return groups
  }, new Map<WorkerType, WorkerNode[]>())
  for (const nodes of groupedWorkerNodes.values()) {
    nodes.sort((a, b) => a.id - b.id)
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
            <FormLabel textColor="blue.500">Dump Heap Profile</FormLabel>
            <VStack>
              <FormLabel>Worker Nodes</FormLabel>
              <Select
                onChange={(event) =>
                  setWorkerNodeId(parseInt(event.target.value))
                }
              >
                {workerTypeOrder.flatMap((workerType) => {
                  const nodes = groupedWorkerNodes.get(workerType)
                  if (!nodes || nodes.length === 0) {
                    return []
                  }
                  return (
                    <optgroup
                      key={workerType}
                      label={workerTypeLabel(workerType)}
                    >
                      {nodes.map((n) => (
                        <option value={n.id} key={n.id}>
                          ({n.id}) {n.host?.host}:{n.host?.port}
                        </option>
                      ))}
                    </optgroup>
                  )
                })}
              </Select>
              <Button onClick={(_) => dumpProfile()} width="full">
                Dump
              </Button>
            </VStack>
          </FormControl>
          <FormControl>
            <FormLabel textColor="blue.500">Analyze Heap Profile</FormLabel>
            <VStack>
              <FormLabel>Dumped Files</FormLabel>
              <Select
                onChange={(event) =>
                  setAnalyzeTargetFileName(event.target.value)
                }
              >
                {profileList && (
                  <optgroup label="Auto">
                    {profileList.nameAuto.map((n) => (
                      <option value={n} key={`auto-${n}`}>
                        {n}
                      </option>
                    ))}
                  </optgroup>
                )}
                {profileList && (
                  <optgroup label="Manually">
                    {profileList.nameManually.map((n) => (
                      <option value={n} key={`manually-${n}`}>
                        {n}
                      </option>
                    ))}
                  </optgroup>
                )}
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
              defaultValue='Select a worker node and target profiling result file and click "Analyze"...'
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
