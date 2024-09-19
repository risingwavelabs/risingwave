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

import { max } from "lodash"
import { TableFragments_Fragment } from "../proto/gen/meta"
import { GraphNode } from "./algo"
import { Relation } from "./api/streaming"

export type Enter<Type> = Type extends d3.Selection<
  any,
  infer B,
  infer C,
  infer D
>
  ? d3.Selection<d3.EnterElement, B, C, D>
  : never

interface DagNode {
  node: GraphNode
  temp: boolean
  perm: boolean
  g: number // generation
  isInput: boolean
  isOutput: boolean
}

interface DagLayer {
  nodes: Array<GraphNode>
  occupyRow: Set<any>
}

function getDagNode(node: GraphNode): DagNode {
  return {
    node: node,
    temp: false,
    perm: false,
    isInput: true,
    isOutput: true,
    g: 0,
  }
}

function dagLayout(nodes: GraphNode[]) {
  let sorted = []
  let _nodes = []
  let node2dagNode = new Map()

  // calculate the generations of nodes
  const visit = (n: DagNode) => {
    if (n.temp) {
      throw Error("This is not a DAG")
    }
    if (!n.perm) {
      n.temp = true
      let maxG = -1
      for (let nextNode of n.node.nextNodes) {
        node2dagNode.get(nextNode).isInput = false
        n.isOutput = false
        let g = visit(node2dagNode.get(nextNode))
        if (g > maxG) {
          maxG = g
        }
      }
      n.temp = false
      n.perm = true
      n.g = maxG + 1
      sorted.unshift(n.node)
    }
    return n.g
  }

  for (let node of nodes) {
    let dagNode = getDagNode(node)
    node2dagNode.set(node, dagNode)
    _nodes.push(dagNode)
  }

  let maxLayer = 0
  for (let node of _nodes) {
    let g = visit(node)
    if (g > maxLayer) {
      maxLayer = g
    }
  }

  // use the bottom up strategy to construct generation number
  // makes the generation number of root node the samllest
  // to make the computation easier, need to flip it back.
  for (let node of _nodes) {
    // node.g = node.isInput ? 0 : (maxLayer - node.g); // TODO: determine which is more suitable
    node.g = maxLayer - node.g
  }

  let layers = new Array<DagLayer>()
  for (let i = 0; i < maxLayer + 1; ++i) {
    layers.push({
      nodes: [],
      occupyRow: new Set(),
    })
  }
  let node2Layer = new Map()
  let node2Row = new Map()
  for (let node of _nodes) {
    layers[node.g].nodes.push(node.node)
    node2Layer.set(node.node, node.g)
  }

  const putNodeInPosition = (node: GraphNode, row: number) => {
    node2Row.set(node, row)
    layers[node2Layer.get(node)].occupyRow.add(row)
  }

  const occupyLine = (ls: number, le: number, r: number) => {
    // layer start, layer end, row
    for (let i = ls; i <= le; ++i) {
      layers[i].occupyRow.add(r)
    }
  }

  const hasOccupied = (layer: number, row: number) =>
    layers[layer].occupyRow.has(row)

  const isStraightLineOccupied = (ls: number, le: number, r: number) => {
    // layer start, layer end, row
    if (r < 0) {
      return false
    }
    for (let i = ls; i <= le; ++i) {
      if (hasOccupied(i, r)) {
        return true
      }
    }
    return false
  }

  for (let node of nodes) {
    node.nextNodes.sort((a, b) => node2Layer.get(b) - node2Layer.get(a))
  }

  for (let layer of layers) {
    for (let node of layer.nodes) {
      if (!node2Row.has(node)) {
        // checking node is not placed.
        for (let nextNode of node.nextNodes) {
          if (node2Row.has(nextNode)) {
            continue
          }
          let r = -1
          while (
            isStraightLineOccupied(
              node2Layer.get(node),
              node2Layer.get(nextNode),
              ++r
            )
          ) {}
          putNodeInPosition(node, r)
          putNodeInPosition(nextNode, r)
          occupyLine(node2Layer.get(node) + 1, node2Layer.get(nextNode) - 1, r)
          break
        }
        if (!node2Row.has(node)) {
          let r = -1
          while (hasOccupied(node2Layer.get(node), ++r)) {}
          putNodeInPosition(node, r)
        }
      }
      // checking node is placed in some position
      for (let nextNode of node.nextNodes) {
        if (node2Row.has(nextNode)) {
          continue
        }
        // check straight line position first
        let r = node2Row.get(node)
        if (
          !isStraightLineOccupied(
            node2Layer.get(node) + 1,
            node2Layer.get(nextNode),
            r
          )
        ) {
          putNodeInPosition(nextNode, r)
          occupyLine(node2Layer.get(node) + 1, node2Layer.get(nextNode) - 1, r)
          continue
        }
        // check lowest available position
        r = -1
        while (
          isStraightLineOccupied(
            node2Layer.get(node) + 1,
            node2Layer.get(nextNode),
            ++r
          )
        ) {}
        putNodeInPosition(nextNode, r)
        occupyLine(node2Layer.get(node) + 1, node2Layer.get(nextNode) - 1, r)
      }
    }
  }

  // layers to rtn
  let rtn = new Map<GraphNode, [number, number]>()
  for (let node of nodes) {
    rtn.set(node, [node2Layer.get(node), node2Row.get(node)])
  }
  return rtn
}

/**
 * @param items
 * @returns Layer and row of the item
 */
function gridLayout<I extends LayoutItemBase>(
  items: Array<I>
): Map<I, [number, number]> {
  // turn item to GraphNode
  let idToItem = new Map<String, I>()
  for (let item of items) {
    idToItem.set(item.id, item)
  }

  let nodeToId = new Map<GraphNode, String>()
  let idToNode = new Map<String, GraphNode>()
  const getNode = (id: String): GraphNode => {
    let rtn = idToNode.get(id)
    if (rtn !== undefined) {
      return rtn
    }
    let newNode = {
      nextNodes: new Array<GraphNode>(),
    }
    let item = idToItem.get(id)
    if (item === undefined) {
      throw Error(`no such id ${id}`)
    }
    for (let id of item.parentIds) {
      getNode(id).nextNodes.push(newNode)
    }
    idToNode.set(id, newNode)
    nodeToId.set(newNode, id)
    return newNode
  }
  for (let item of items) {
    getNode(item.id)
  }

  // run daglayout on GraphNode
  let rtn = new Map<I, [number, number]>()
  let allNodes = new Array<GraphNode>()
  for (let _n of nodeToId.keys()) {
    allNodes.push(_n)
  }
  let resultMap = dagLayout(allNodes)
  for (let item of resultMap) {
    let id = nodeToId.get(item[0])
    if (!id) {
      throw Error(`no corresponding item of node ${item[0]}`)
    }
    let fb = idToItem.get(id)
    if (!fb) {
      throw Error(`item id ${id} is not present in idToBox`)
    }
    rtn.set(fb, item[1])
  }
  return rtn
}

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

/**
 * @param items
 * @returns the coordination of the top-left corner of the fragment box
 */
export function layoutItem<I extends LayoutItemBase>(
  items: Array<I>,
  layerMargin: number,
  rowMargin: number
): (I & Position)[] {
  let layoutMap = gridLayout(items)
  let layerRequiredWidth = new Map<number, number>()
  let rowRequiredHeight = new Map<number, number>()
  let maxLayer = 0,
    maxRow = 0

  for (let item of layoutMap) {
    let fb = item[0],
      layer = item[1][0],
      row = item[1][1]
    let currentWidth = layerRequiredWidth.get(layer) || 0
    if (fb.width > currentWidth) {
      layerRequiredWidth.set(layer, fb.width)
    }
    let currentHeight = rowRequiredHeight.get(row) || 0
    if (fb.height > currentHeight) {
      rowRequiredHeight.set(row, fb.height)
    }

    maxLayer = max([layer, maxLayer]) || 0
    maxRow = max([row, maxRow]) || 0
  }

  let layerCumulativeWidth = new Map<number, number>()
  let rowCumulativeHeight = new Map<number, number>()

  const getCumulativeMargin = (
    index: number,
    margin: number,
    resultMap: Map<number, number>,
    marginMap: Map<number, number>
  ): number => {
    let rtn = resultMap.get(index)
    if (rtn) {
      return rtn
    }
    if (index === 0) {
      rtn = 0
    } else {
      let delta = marginMap.get(index - 1)
      if (!delta) {
        throw Error(`${index - 1} has no result`)
      }
      rtn =
        getCumulativeMargin(index - 1, margin, resultMap, marginMap) +
        delta +
        margin
    }
    resultMap.set(index, rtn)
    return rtn
  }

  for (let i = 0; i <= maxLayer; ++i) {
    getCumulativeMargin(
      i,
      layerMargin,
      layerCumulativeWidth,
      layerRequiredWidth
    )
  }
  for (let i = 0; i <= maxRow; ++i) {
    getCumulativeMargin(i, rowMargin, rowCumulativeHeight, rowRequiredHeight)
  }

  let rtn: Array<I & Position> = []

  for (let [data, [layer, row]] of layoutMap) {
    let x = layerCumulativeWidth.get(layer)
    let y = rowCumulativeHeight.get(row)
    if (x !== undefined && y !== undefined) {
      rtn.push({
        x,
        y,
        ...data,
      })
    } else {
      throw Error(`x of layer ${layer}: ${x}, y of row ${row}: ${y} `)
    }
  }
  return rtn
}

function layoutRelation(
  relations: Array<RelationPoint>,
  layerMargin: number,
  rowMargin: number,
  nodeRadius: number
): RelationPointPosition[] {
  const result = layoutItem(relations, layerMargin, rowMargin)
  return result.map(({ x, y, ...data }) => ({
    x: x + nodeRadius,
    y: y + nodeRadius,
    ...data,
  }))
}

export function flipLayoutRelation(
  relations: Array<RelationPoint>,
  layerMargin: number,
  rowMargin: number,
  nodeRadius: number
): RelationPointPosition[] {
  const fragmentPosition = layoutRelation(
    relations,
    rowMargin,
    layerMargin,
    nodeRadius
  )
  return fragmentPosition.map(({ x, y, ...data }) => ({
    x: y,
    y: x,
    ...data,
  }))
}

export function generateRelationEdges(
  layoutMap: RelationPointPosition[]
): Edge[] {
  const links = []
  const relationMap = new Map<string, RelationPointPosition>()
  for (const x of layoutMap) {
    relationMap.set(x.id, x)
  }
  for (const relation of layoutMap) {
    for (const parentId of relation.parentIds) {
      const parentRelation = relationMap.get(parentId)!
      links.push({
        points: [
          { x: relation.x, y: relation.y },
          { x: parentRelation.x, y: parentRelation.y },
        ],
        source: relation.id,
        target: parentId,
      })
    }
  }
  return links
}

export function generateFragmentEdges(
  layoutMap: FragmentBoxPosition[]
): Edge[] {
  const links = []
  const fragmentMap = new Map<string, FragmentBoxPosition>()
  for (const x of layoutMap) {
    fragmentMap.set(x.id, x)
  }
  for (const fragment of layoutMap) {
    for (const parentId of fragment.parentIds) {
      const parentFragment = fragmentMap.get(parentId)!
      links.push({
        points: [
          {
            x: fragment.x + fragment.width / 2,
            y: fragment.y + fragment.height / 2,
          },
          {
            x: parentFragment.x + parentFragment.width / 2,
            y: parentFragment.y + parentFragment.height / 2,
          },
        ],
        source: fragment.id,
        target: parentId,
      })
    }

    // Simply draw a horizontal line here.
    // Typically, external parent is only applicable to `StreamScan` fragment,
    // and there'll be only one external parent due to `UpstreamShard` distribution
    // and plan node sharing. So we won't see multiple horizontal lines overlap each other.
    for (const externalParentId of fragment.externalParentIds) {
      links.push({
        points: [
          {
            x: fragment.x,
            y: fragment.y + fragment.height / 2,
          },
          {
            x: fragment.x + 100,
            y: fragment.y + fragment.height / 2,
          },
        ],
        source: fragment.id,
        target: externalParentId,
      })
    }
  }
  return links
}

export function generateRelationBackPressureEdges(layoutMap: RelationBoxPosition[]): Edge[] {
  const links = []
  const relationMap = new Map<string, RelationBoxPosition>()
  for (const x of layoutMap) {
    relationMap.set(x.id, x)
  }
  for (const relation of layoutMap) {
    for (const parentId of relation.parentIds) {
      const parentRelation = relationMap.get(parentId)!
      links.push({
        points: [
          {
            x: relation.x + relation.width / 2,
            y: relation.y + relation.height / 2,
          },
          {
            x: parentRelation.x + parentRelation.width / 2,
            y: parentRelation.y + parentRelation.height / 2,
          },
        ],
        source: relation.id,
        target: parentId,
      })
    }
  }
  return links
}
