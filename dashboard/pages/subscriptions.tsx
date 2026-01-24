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

import { Column, Relations } from "../components/Relations"
import { getSubscriptions } from "../lib/api/streaming"
import { Subscription as RwSubscription } from "../proto/gen/catalog"

export default function Subscriptions() {
  const subscriptionRetentionSeconds: Column<RwSubscription> = {
    name: "Retention Seconds",
    width: 3,
    content: (r) => r.retentionSeconds ?? "unknown",
  }

  const subscriptionDependentTableId: Column<RwSubscription> = {
    name: "Dependent Table Id",
    width: 3,
    content: (r) => r.dependentTableId ?? "unknown",
  }
  return Relations("Subscriptions", getSubscriptions, [
    subscriptionRetentionSeconds,
    subscriptionDependentTableId,
  ])
}
