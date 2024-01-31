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

export const INTERVAL = 20000
export interface BackPressuresMetrics {
  outputBufferBlockingDuration: Metrics[]
}

// Get back pressure from meta node -> prometheus
export async function getActorBackPressures() {
  const res: BackPressuresMetrics = await api.get(
    "/metrics/actor/back_pressures",
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

function convertToMapAndAgg(back_pressures: BackPressureInfo[]): Map<string, number> {
  const map = new Map<string, number>()
  for (const item of back_pressures) {
    const key = `${item.fragmentId}-${item.downstreamFragmentId}`
    if (map.has(key)) {
      map.set(key, map.get(key) + item.value)
    } else {
      map.set(key, item.value)
    }
  }
  return map
}

function convertFromMapAndAgg(map: Map<string, number>): BackPressureRateInfo[] {
  const result: BackPressureRateInfo[] = []
  map.forEach((value, key) => {
    const [fragmentId, downstreamFragmentId] = key
      .split("-")
      .map(Number)
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

function convertToBackPressureMetrics(bp_rates: BackPressureRateInfo[]): BackPressuresMetrics {
  const bp_metrics: BackPressuresMetrics = {
    outputBufferBlockingDuration: [],
  }
  for (const item of bp_rates) {
    bp_metrics.outputBufferBlockingDuration.push({
      metric: {
        actor_id: item.actorId.toString(),
        fragment_id: item.fragmentId.toString(),
        downstream_fragment_id: item.downstreamFragmentId.toString(),
      },
      sample: [{
        timestamp: Date.now()
        , value: item.backPressureRate
      }],
    })
  }
  return bp_metrics
}

export function calculateBPRate(
  back_pressure_new: BackPressureInfo[],
  back_pressure_old: BackPressureInfo[],
): BackPressuresMetrics {
  let map_new = convertToMapAndAgg(back_pressure_new)
  let map_old = convertToMapAndAgg(back_pressure_old)
  let result = new Map<string, number>()
  map_new.forEach((value, key) => {
    if (map_old.has(key)) {
      result.set(
        key,
        (value - map_old.get(key)) / (INTERVAL * 1000000000),
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
  const response = await api.get("/metrics/back_pressures")
  let back_pressure_infos: BackPressureInfo[] = response.backPressureInfos.map(
    BackPressureInfo.fromJSON,
  )
  back_pressure_infos = back_pressure_infos.sort(
    (a, b) => a.actorId - b.actorId,
  )
  return back_pressure_infos
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
