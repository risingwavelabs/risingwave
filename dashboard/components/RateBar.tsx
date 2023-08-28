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

import { Box, Text, Tooltip } from "@chakra-ui/react"
import { tinycolor } from "@ctrl/tinycolor"
import { p50, p90, p95, p99 } from "../pages/api/metric"
import { MetricsSample } from "./metrics"

export default function RateBar({ samples }: { samples: MetricsSample[] }) {
  const p_50 = (p50(samples) * 100).toFixed(6)
  const p_95 = (p95(samples) * 100).toFixed(6)
  const p_99 = (p99(samples) * 100).toFixed(6)
  const p_90 = p90(samples) * 100

  const bgWidth = Math.ceil(p_90).toFixed(6) + "%"
  const detailRate = `p50: ${p_50}% p95: ${p_95}% p99: ${p_99}%`

  // calculate gradient color
  const colorRange = ["#C6F6D5", "#C53030"]
  const endColor = tinycolor(colorRange[0])
    .mix(tinycolor(colorRange[1]), Math.ceil(p_90))
    .toHexString()
  const bgGradient = `linear(to-r, ${colorRange[0]}, ${endColor})`

  return (
    <Tooltip bgColor="gray.100" label={detailRate}>
      <Box width={bgWidth} bgGradient={bgGradient}>
        <Text>p90: {p_90.toFixed(6)}%</Text>
      </Box>
    </Tooltip>
  )
}
