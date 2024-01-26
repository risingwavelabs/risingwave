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
import { Field } from "../../proto/gen/plan_common"
import api from "./api"

export interface BackPressuresMetrics {
  outputBufferBlockingDuration: Metrics[]
}

// Get back pressure from meta node -> prometheus
export async function getActorBackPressures() {
  const res: BackPressuresMetrics = await api.get(
    "/metrics/actor/back_pressures"
  )
  return res
}

export interface BackPressureInfo {
  id: number;
  name: string;
  owner: number;
  columns: Field[];
  actorId: number,
  fragementId: number,
  donwStreamFragmentId: number,
  value: number,
}

export const BackPressureInfo = {
  fromJSON: (object: any) => {
    return {
      id: 0,
      name: "",
      owner: 0,
      columns: [],
      actorId: isSet(object.actorId) ? Number(object.actorId) : 0,
      fragementId: isSet(object.fragementId) ? Number(object.fragementId) : 0,
      donwStreamFragmentId: isSet(object.donwStreamFragmentId) ? Number(object.donwStreamFragmentId) : 0,
      value: isSet(object.value) ? Number(object.value) : 0,
    }
  },
}

// Get back pressure from meta node -> compute node
export async function getComputeBackPressures() {
  const response = await api.get("/metrics/back_pressures");

  let back_pressure_infos: BackPressureInfo[] = response.backPressureInfos.map(BackPressureInfo.fromJSON)

  back_pressure_infos = back_pressure_infos.sort((a, b) => a.actorId - b.actorId)
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
  return value !== null && value !== undefined;
}