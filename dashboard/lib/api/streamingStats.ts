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

import { ChannelStatsSnapshot } from "../../pages/fragment_graph"
import {
  ChannelDeltaStats,
  FragmentStats,
  GetStreamingPrometheusStatsResponse,
  GetStreamingStatsResponse,
  RelationStats,
} from "../../proto/gen/monitor_service"
import api from "./api"

export type StatsType = "relation" | "fragment"

export interface StreamingStatsCallbacks {
  setChannelStats: (stats: Map<string, ChannelDeltaStats> | undefined) => void
  setRelationStats?: (
    stats: { [key: number]: RelationStats } | undefined
  ) => void
  setFragmentStats?: (
    stats: { [key: number]: FragmentStats } | undefined
  ) => void
  toast: (
    error: any,
    status?: "info" | "warning" | "success" | "error" | "loading"
  ) => void
}

export function createStreamingStatsRefresh(
  callbacks: StreamingStatsCallbacks,
  statsType: StatsType,
  useInitialSnapshot: boolean = false
) {
  let initialSnapshot: ChannelStatsSnapshot | undefined

  return function refresh() {
    let embedded = false
    if (embedded) {
      api.get("/metrics/streaming_stats").then(
        (res) => {
          let response = GetStreamingStatsResponse.fromJSON(res)
          let snapshot = new ChannelStatsSnapshot(
            new Map(Object.entries(response.channelStats)),
            Date.now()
          )

          if (useInitialSnapshot) {
            if (!initialSnapshot) {
              initialSnapshot = snapshot
            } else {
              callbacks.setChannelStats(snapshot.getRate(initialSnapshot))
            }
          } else {
            callbacks.setChannelStats(snapshot.getRate(snapshot))
          }

          // Dispatch to the appropriate stats setter based on statsType
          if (
            statsType === "relation" &&
            callbacks.setRelationStats &&
            response.relationStats
          ) {
            callbacks.setRelationStats(response.relationStats)
          } else if (
            statsType === "fragment" &&
            callbacks.setFragmentStats &&
            response.fragmentStats
          ) {
            callbacks.setFragmentStats(response.fragmentStats)
          }
        },
        (e) => {
          console.error(e)
          callbacks.toast(e, "error")
        }
      )
    } else {
      api.get("/metrics/streaming_stats_prometheus").then(
        (res) => {
          let response = GetStreamingPrometheusStatsResponse.fromJSON(res)
          const result = new Map<string, ChannelDeltaStats>()
          for (const [key, value] of Object.entries(response.channelStats)) {
            result.set(key, {
              actorCount: value.actorCount,
              backpressureRate: value.backpressureRate,
              recvThroughput: value.recvThroughput,
              sendThroughput: value.sendThroughput,
            })
          }
          callbacks.setChannelStats(result)

          // Dispatch to the appropriate stats setter based on statsType
          if (
            statsType === "relation" &&
            callbacks.setRelationStats &&
            response.relationStats
          ) {
            callbacks.setRelationStats(response.relationStats)
          } else if (
            statsType === "fragment" &&
            callbacks.setFragmentStats &&
            response.fragmentStats
          ) {
            callbacks.setFragmentStats(response.fragmentStats)
          }
        },
        (e) => {
          console.error(e)
          callbacks.toast(e, "error")
        }
      )
    }
  }
}
