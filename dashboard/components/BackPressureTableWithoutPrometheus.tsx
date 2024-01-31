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
    Table,
    TableCaption,
    TableContainer,
    Tbody,
    Td,
    Th,
    Thead,
    Tr,
} from "@chakra-ui/react"
import { sortBy } from "lodash"
import Head from "next/head"
import { Fragment, useEffect, useState } from "react"
import useErrorToast from "../hook/useErrorToast"
import RateBar from "./RateBar"
import { BackPressureInfo, getBackPressureWithoutPrometheus, BackPressuresMetrics, calculateBPRate, INTERVAL } from "../pages/api/metric"

export default function BackPressureTableWithoutPrometheus({
    selectedFragmentIds,
}: {
    selectedFragmentIds: Set<string>
}) {
    const [backPressuresMetrics, setBackPressuresMetrics] =
        useState<BackPressuresMetrics>()
    const [previousBP, setPreviousBP] = useState<BackPressureInfo[]>([])
    const toast = useErrorToast()

    useEffect(() => {
        let localPreviousBP = previousBP

        async function doFetch() {
            while (true) {
                try {
                    const currentBP = await getBackPressureWithoutPrometheus()
                    const metrics = calculateBPRate(currentBP, localPreviousBP)
                    setBackPressuresMetrics(metrics)
                    localPreviousBP = currentBP
                    setPreviousBP(currentBP)

                    metrics.outputBufferBlockingDuration = sortBy(
                        metrics.outputBufferBlockingDuration,
                        (m) => (m.metric.fragment_id, m.metric.downstream_fragment_id)
                    )
                    setBackPressuresMetrics(metrics)
                    await new Promise((resolve) => setTimeout(resolve, INTERVAL)) // refresh every 5 secs
                } catch (e: any) {
                    toast(e, "warning")
                    break
                }
            }
        }
        doFetch()
        return () => { }
    }, [toast])

    const isSelected = (fragmentId: string) => selectedFragmentIds.has(fragmentId)

    const retVal = (
        <TableContainer>
            <Table variant="simple">
                <TableCaption>Back Pressures (Last 30 minutes)</TableCaption>
                <Thead>
                    <Tr>
                        <Th>Fragment IDs &rarr; Downstream</Th>
                        <Th>Block Rate</Th>
                    </Tr>
                </Thead>
                <Tbody>
                    {backPressuresMetrics &&
                        backPressuresMetrics.outputBufferBlockingDuration
                            .filter((m) => isSelected(m.metric.fragment_id))
                            .map((m) => (
                                <Tr
                                    key={`${m.metric.fragment_id}_${m.metric.downstream_fragment_id}`}
                                >
                                    <Td>{`Fragment ${m.metric.fragment_id} -> ${m.metric.downstream_fragment_id}`}</Td>
                                    <Td>
                                        <RateBar samples={m.sample} />
                                    </Td>
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
