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
import { WorkerNode } from "../../proto/gen/common"
import api from "./api"

export async function getClusterMetrics() {
  const res = await api.get("/metrics/cluster")
  return res
}

export async function getClusterInfoFrontend() {
  const res: WorkerNode[] = (await api.get("/clusters/1")).map(
    WorkerNode.fromJSON
  )
  return res
}

export async function getClusterInfoComputeNode() {
  const res: WorkerNode[] = (await api.get("/clusters/2")).map(
    WorkerNode.fromJSON
  )
  return res
}

export async function getClusterVersion() {
  const res = await api.get("/version")
  return res
}
