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
import api from "./api"

export const INTERVAL = 5000
export interface BackPressuresMetrics {
  outputBufferBlockingDuration: Metrics[]
}

// Get back pressure from meta node -> prometheus
export async function getActorBackPressures() {
  const res: BackPressuresMetrics = await api.get(
    "/metrics/fragment/prometheus_back_pressures"
  )
  return res
}

export interface BackPressureInfo {
  actorId: number
  fragmentId: number
  downstreamFragmentId: number
  value: number
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
    if (mapValue.has(key) && mapNumber.has(key)) {
      // add || tp avoid NaN and pass check
      mapValue.set(key, (mapValue.get(key) || 0) + item.value)
      mapNumber.set(key, (mapNumber.get(key) || 0) + 1)
    } else {
      mapValue.set(key, item.value)
      mapNumber.set(key, 1)
    }
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

export function calculateBPRate(
  backPressureNew: BackPressureInfo[],
  backPressureOld: BackPressureInfo[]
): BackPressuresMetrics {
  let mapNew = convertToMapAndAgg(backPressureNew)
  let mapOld = convertToMapAndAgg(backPressureOld)
  let result = new Map<string, number>()
  mapNew.forEach((value, key) => {
    if (mapOld.has(key)) {
      result.set(
        key,
        // The *100 in end of the formular is to convert the BP rate to the value used in web UI drawing
        ((value - (mapOld.get(key) || 0)) / ((INTERVAL / 1000) * 1000000000)) *
          100
      )
    } else {
      result.set(key, 0)
    }
  })

  return convertToBackPressureMetrics(convertFromMapAndAgg(result))
}

export const BackPressureInfo = {
  fromJSON: (object: any) => {
    return {
      actorId: isSet(object.actorId) ? Number(object.actorId) : 0,
      fragmentId: isSet(object.fragmentId) ? Number(object.fragmentId) : 0,
      downstreamFragmentId: isSet(object.downstreamFragmentId)
        ? Number(object.downstreamFragmentId)
        : 0,
      value: isSet(object.value) ? Number(object.value) : 0,
    }
  },
}

// Get back pressure from meta node -> compute node
export async function getBackPressureWithoutPrometheus() {
  const response = await api.get("/metrics/fragment/embedded_back_pressures")
  let backPressureInfos: BackPressureInfo[] = response.backPressureInfos.map(
    BackPressureInfo.fromJSON
  )
  backPressureInfos = backPressureInfos.sort((a, b) => a.actorId - b.actorId)
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
