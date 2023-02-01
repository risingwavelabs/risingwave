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
  Grid,
  GridItem,
  HStack,
  SimpleGrid,
  Text,
  theme,
  useToast,
  VStack,
} from "@chakra-ui/react"
import { clone, reverse, sortBy } from "lodash"
import Head from "next/head"
import { Fragment, useCallback, useEffect, useState } from "react"
import { Area, AreaChart, ResponsiveContainer, XAxis, YAxis } from "recharts"
import Title from "../components/Title"
import { WorkerNode } from "../proto/gen/common"
import {
  getClusterInfoComputeNode,
  getClusterInfoFrontend,
  getClusterMetrics,
} from "./api/cluster"

function WorkerNodeComponent({
  workerNodeType,
  workerNode,
}: {
  workerNodeType: string
  workerNode: WorkerNode
}) {
  return (
    <Fragment>
      <VStack alignItems="start" spacing={1}>
        <HStack>
          <Box w={3} h={3} flex="none" bgColor="green.600" rounded="full"></Box>
          <Text fontWeight="medium" fontSize="xl" textColor="black">
            {workerNodeType} #{workerNode.id}
          </Text>
        </HStack>
        <Text textColor="gray.500" m={0}>
          Running
        </Text>
        <Text textColor="gray.500" m={0}>
          {workerNode.host?.host}:{workerNode.host?.port}
        </Text>
      </VStack>
    </Fragment>
  )
}

function WorkerNodeMetricsComponent({
  job,
  instance,
  metrics,
  isCpuMetrics,
}: {
  job: string
  instance: string
  metrics: MetricsSample[]
  isCpuMetrics: boolean
}) {
  const metricsCallback = useCallback(() => {
    const filledMetrics: MetricsSample[] = []
    if (metrics.length === 0) {
      return []
    }
    let lastTs: number = metrics.at(-1)!.timestamp
    for (let pt of reverse(clone(metrics))) {
      while (lastTs - pt.timestamp > 0) {
        lastTs -= 60
        filledMetrics.push({
          timestamp: lastTs,
          value: 0,
        })
      }
      filledMetrics.push(pt)
      lastTs -= 60
    }
    while (filledMetrics.length < 60) {
      filledMetrics.push({ timestamp: lastTs, value: 0 })
      lastTs -= 60
    }
    return reverse(filledMetrics)
  }, [metrics])
  return (
    <Fragment>
      <VStack alignItems="start" spacing={1}>
        <Text textColor="gray.500" mx={3}>
          <b>{job}</b> {instance}
        </Text>

        <ResponsiveContainer width="100%" height={100}>
          <AreaChart data={metricsCallback()}>
            <XAxis
              dataKey="timestamp"
              type="number"
              domain={["dataMin", "dataMax"]}
              hide={true}
            />
            {isCpuMetrics && (
              <YAxis type="number" domain={[0, 1]} hide={true} />
            )}
            <Area
              isAnimationActive={false}
              type="linear"
              dataKey="value"
              strokeWidth={1}
              stroke={theme.colors.teal["500"]}
              fill={theme.colors.teal["100"]}
            />
          </AreaChart>
        </ResponsiveContainer>
      </VStack>
    </Fragment>
  )
}

interface MetricsSample {
  timestamp: number
  value: number
}

interface NodeMetrics {
  metric: { [key: string]: string }
  sample: MetricsSample[]
}

interface ClusterNodeMetrics {
  cpuData: NodeMetrics[]
  memoryData: NodeMetrics[]
}

export default function Cluster() {
  const [frontendList, setFrontendList] = useState<WorkerNode[]>([])
  const [computeNodeList, setComputeNodeList] = useState<WorkerNode[]>([])
  const [metrics, setMetrics] = useState<ClusterNodeMetrics>()
  const toast = useToast()

  useEffect(() => {
    async function doFetch() {
      try {
        setFrontendList(await getClusterInfoFrontend())
        setComputeNodeList(await getClusterInfoComputeNode())
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
    doFetch()
    return () => {}
  }, [toast])

  useEffect(() => {
    async function doFetch() {
      while (true) {
        try {
          let metrics: ClusterNodeMetrics = await getClusterMetrics()
          metrics.cpuData = sortBy(metrics.cpuData, (m) => m.metric.instance)
          metrics.memoryData = sortBy(
            metrics.memoryData,
            (m) => m.metric.instance
          )
          setMetrics(metrics)
          await new Promise((resolve) => setTimeout(resolve, 5000)) // refresh every 5 secs
        } catch (e: any) {
          toast({
            title: "Error Occurred",
            description: e.toString(),
            status: "error",
            duration: 5000,
            isClosable: true,
          })
          console.error(e)
          break
        }
      }
    }
    doFetch()
    return () => {}
  }, [toast])

  const retVal = (
    <Box p={3}>
      <Title>Cluster Overview</Title>
      <Grid my={3} templateColumns="repeat(3, 1fr)" gap={6} width="full">
        {frontendList.map((frontend) => (
          <GridItem
            w="full"
            rounded="xl"
            bg="white"
            shadow="md"
            borderWidth={1}
            p={6}
            key={frontend.id}
          >
            <WorkerNodeComponent
              workerNodeType="Frontend"
              workerNode={frontend}
            />
          </GridItem>
        ))}
        {computeNodeList.map((computeNode) => (
          <GridItem
            w="full"
            rounded="xl"
            bg="white"
            shadow="md"
            borderWidth={1}
            p={6}
            key={computeNode.id}
          >
            <WorkerNodeComponent
              workerNodeType="Compute"
              workerNode={computeNode}
            />
          </GridItem>
        ))}
      </Grid>
      <Title>CPU Usage</Title>
      <SimpleGrid my={3} columns={3} spacing={6} width="full">
        {metrics &&
          metrics.cpuData.map((data) => (
            <GridItem
              w="full"
              rounded="xl"
              bg="white"
              shadow="md"
              borderWidth={1}
              key={data.metric.instance}
            >
              <WorkerNodeMetricsComponent
                job={data.metric.job}
                instance={data.metric.instance}
                metrics={data.sample}
                isCpuMetrics={true}
              />
            </GridItem>
          ))}
      </SimpleGrid>
      <Title>Memory Usage</Title>
      <SimpleGrid my={3} columns={3} spacing={6} width="full">
        {metrics &&
          metrics.memoryData.map((data) => (
            <GridItem
              w="full"
              rounded="xl"
              bg="white"
              shadow="md"
              borderWidth={1}
              key={data.metric.instance}
            >
              <WorkerNodeMetricsComponent
                job={data.metric.job}
                instance={data.metric.instance}
                metrics={data.sample}
                isCpuMetrics={false}
              />
            </GridItem>
          ))}
      </SimpleGrid>
    </Box>
  )
  return (
    <Fragment>
      <Head>
        <title>Cluster Overview</title>
      </Head>
      {retVal}
    </Fragment>
  )
}
