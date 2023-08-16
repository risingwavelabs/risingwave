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
import { MetricsSample } from "../../components/metrics"
import api from "./api"

export async function getActorBackPressures() {
  const res = await api.get("/api/metrics/actor/back_pressures")
  return res
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
