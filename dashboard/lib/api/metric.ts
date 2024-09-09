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
import { Metrics, MetricsSample } from "../../components/metrics"
import {
  BackPressureInfo,
  GetBackPressureResponse,
} from "../../proto/gen/monitor_service"
import api from "./api"

export interface BackPressuresMetrics {
  outputBufferBlockingDuration: Metrics[]
}

// Get back pressure from Prometheus
export async function fetchPrometheusBackPressure() {
  const res: BackPressuresMetrics = await api.get(
    "/metrics/fragment/prometheus_back_pressures"
  )
  return res
}

export interface BackPressureRateInfo {
  actorId: number
  fragmentId: number
  downstreamFragmentId: number
  backPressureRate: number
}

function convertToMapAndAgg(
  backPressures: BackPressureInfo[]
): Map<string, number> {
  // FragmentId-downstreamFragmentId, total value
  const mapValue = new Map<string, number>()
  // FragmentId-downstreamFragmentId, total count
  const mapNumber = new Map<string, number>()
  // FragmentId-downstreamFragmentId, average value
  const map = new Map<string, number>()
  for (const item of backPressures) {
    const key = `${item.fragmentId}-${item.downstreamFragmentId}`
    mapValue.set(key, (mapValue.get(key) || 0) + item.value)
    mapNumber.set(key, (mapNumber.get(key) || 0) + item.actorCount)
  }

  for (const [key, value] of mapValue) {
    map.set(key, value / mapNumber.get(key)!)
  }
  return map
}

function convertFromMapAndAgg(
  map: Map<string, number>
): BackPressureRateInfo[] {
  const result: BackPressureRateInfo[] = []
  map.forEach((value, key) => {
    const [fragmentId, downstreamFragmentId] = key.split("-").map(Number)
    const backPressureRateInfo: BackPressureRateInfo = {
      actorId: 0,
      fragmentId,
      downstreamFragmentId,
      backPressureRate: value,
    }
    result.push(backPressureRateInfo)
  })
  return result
}

function convertToBackPressureMetrics(
  bpRates: BackPressureRateInfo[]
): BackPressuresMetrics {
  const bpMetrics: BackPressuresMetrics = {
    outputBufferBlockingDuration: [],
  }
  for (const item of bpRates) {
    bpMetrics.outputBufferBlockingDuration.push({
      metric: {
        actorId: item.actorId.toString(),
        fragmentId: item.fragmentId.toString(),
        downstreamFragmentId: item.downstreamFragmentId.toString(),
      },
      sample: [
        {
          timestamp: Date.now(),
          value: item.backPressureRate,
        },
      ],
    })
  }
  return bpMetrics
}

export function calculateCumulativeBp(
  backPressureCumulative: BackPressureInfo[],
  backPressureCurrent: BackPressureInfo[],
  backPressureNew: BackPressureInfo[]
): BackPressureInfo[] {
  let mapCumulative = convertToMapAndAgg(backPressureCumulative)
  let mapCurrent = convertToMapAndAgg(backPressureCurrent)
  let mapNew = convertToMapAndAgg(backPressureNew)
  let mapResult = new Map<string, number>()
  let keys = new Set([
    ...mapCumulative.keys(),
    ...mapCurrent.keys(),
    ...mapNew.keys(),
  ])
  keys.forEach((key) => {
    let backpressureCumulativeValue = mapCumulative.get(key) || 0
    let backpressureCurrentValue = mapCurrent.get(key) || 0
    let backpressureNewValue = mapNew.get(key) || 0
    let increment = backpressureNewValue - backpressureCurrentValue
    mapResult.set(key, backpressureCumulativeValue + increment)
  })
  const result: BackPressureInfo[] = []
  mapResult.forEach((value, key) => {
    const [fragmentId, downstreamFragmentId] = key.split("-").map(Number)
    const backPressureInfo: BackPressureInfo = {
      actorCount: 1, // the value here has already been averaged by real actor count
      fragmentId,
      downstreamFragmentId,
      value,
    }
    result.push(backPressureInfo)
  })
  return result
}

export function calculateBPRate(
  backPressureCumulative: BackPressureInfo[],
  totalDurationNs: number
): BackPressuresMetrics {
  let map = convertToMapAndAgg(backPressureCumulative)
  let result = new Map<string, number>()
  map.forEach((backpressureNs, key) => {
    let backpressureRateRatio = backpressureNs / totalDurationNs
    let backpressureRatePercent = backpressureRateRatio * 100
    result.set(key, backpressureRatePercent)
  })
  return convertToBackPressureMetrics(convertFromMapAndAgg(result))
}

// Get back pressure from meta node -> compute node
export async function fetchEmbeddedBackPressure() {
  const response: GetBackPressureResponse = await api.get(
    "/metrics/fragment/embedded_back_pressures"
  )
  let backPressureInfos: BackPressureInfo[] =
    response.backPressureInfos?.map(BackPressureInfo.fromJSON) ?? []
  return backPressureInfos
}

function calculatePercentile(samples: MetricsSample[], percentile: number) {
  const sorted = samples.sort((a, b) => a.value - b.value)
  const index = Math.floor(sorted.length * percentile)
  return sorted[index].value
}

export function p50(samples: MetricsSample[]) {
  return calculatePercentile(samples, 0.5)
}

export function p90(samples: MetricsSample[]) {
  return calculatePercentile(samples, 0.9)
}

export function p95(samples: MetricsSample[]) {
  return calculatePercentile(samples, 0.95)
}

export function p99(samples: MetricsSample[]) {
  return calculatePercentile(samples, 0.99)
}

function isSet(value: any): boolean {
  return value !== null && value !== undefined
}
