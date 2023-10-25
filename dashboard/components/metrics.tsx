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

export interface MetricsSample {
  timestamp: number
  value: number
}

export interface Metrics {
  // Tags of this timeseries. Example: {"downstream_fragment_id":"15001","fragment_id":"15002"}
  metric: { [key: string]: string }

  // Example: [{"timestamp":1695041872.0,"value":0.3797035002929275},
  //  {"timestamp":1695041887.0,"value":0.5914327683152408},
  //  {"timestamp":1695041902.0,"value":0.8272212493499999}, ... ]
  sample: MetricsSample[]
}
