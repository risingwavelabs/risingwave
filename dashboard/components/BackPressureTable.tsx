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

import {Metrics} from "./metrics";
import {Fragment, useEffect, useState} from "react";
import {Table, TableCaption, TableContainer, Thead, Th, useToast, Tbody, Tr, Td} from "@chakra-ui/react";
import {getActorBackPressures, sampleAverage, sampleMax, sampleMin} from "../pages/api/metric";
import {sortBy} from "lodash";
import Head from "next/head";

interface BackPressuresMetrics {
    outputBufferBlockingDuration: Metrics[]
}

export default function BackPressureTable({
    selectedActorIds,
}: {
    selectedActorIds: Set<string>
}) {
    const [backPressuresMetrics, setBackPressuresMetrics] = useState<BackPressuresMetrics>()
    const toast = useToast()

    useEffect(() => {
        async function doFetch() {
            while (true) {
                try {
                    let metrics: BackPressuresMetrics = await getActorBackPressures()
                    metrics.outputBufferBlockingDuration = sortBy(
                        metrics.outputBufferBlockingDuration,
                        (m) => m.metric.actor_id
                    )
                    setBackPressuresMetrics(metrics)
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

    const isSelected = (actorId: string) => selectedActorIds.has(actorId)

    const retVal = (
        <TableContainer>
            <Table variant='simple'>
                <TableCaption>Back Pressures (Last 1 hour)</TableCaption>
                <Thead>
                    <Th>Actor ID</Th>
                    <Th>Instance</Th>
                    <Th>Block Rate(Min~Max)</Th>
                    <Th>Block Rate(Last)</Th>
                    <Th>Block Rate(Avg)</Th>
                </Thead>
                <Tbody>
                    {backPressuresMetrics &&
                        backPressuresMetrics.outputBufferBlockingDuration.filter(
                            (m) => isSelected(m.metric.actor_id)).map((m) => (
                        <Tr key={m.metric.actor_id}>
                            <Td>{m.metric.actor_id}</Td>
                            <Td>{m.metric.instance}</Td>
                            <Td>{(sampleMin(m.sample) * 100).toFixed(6)}%~{(sampleMax(m.sample) * 100).toFixed(6)}%</Td>
                            <Td>{(m.sample[m.sample.length - 1].value * 100).toFixed(6)}%</Td>
                            <Td>{(sampleAverage(m.sample) * 100).toFixed(6)}%</Td>
                        </Tr>
                    ))}
                </Tbody>
            </Table>
        </TableContainer>
    )
    return (
        <Fragment>
            <Head>
                <title>Streaming Back Pressure</title>
            </Head>
            {retVal}
        </Fragment>
    )
}