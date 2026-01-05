/*
 * Copyright 2022 RisingWave Labs
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
 */

import { TableFragments_Fragment } from "../proto/gen/meta"
import { Relation } from "./api/streaming"

export type Enter<Type> = Type extends d3.Selection<
  any,
  infer B,
  infer C,
  infer D
>
  ? d3.Selection<d3.EnterElement, B, C, D>
  : never

export interface LayoutItemBase {
  id: string
  order: number // preference order, item with larger order will be placed at right or down
  width: number
  height: number
  parentIds: string[]
}

export type FragmentBox = LayoutItemBase & {
  name: string
  // Upstream Fragment Ids.
  externalParentIds: string[]
  fragment: TableFragments_Fragment
}

export type RelationBox = LayoutItemBase & {
  relationName: string
  schemaName: string
}

export type RelationPoint = LayoutItemBase & {
  name: string
  relation: Relation
}

export interface Position {
  x: number
  y: number
}

export type FragmentBoxPosition = FragmentBox & Position
export type RelationPointPosition = RelationPoint & Position
export type RelationBoxPosition = RelationBox & Position

export interface Edge {
  points: Array<Position>
  source: string
  target: string
}
