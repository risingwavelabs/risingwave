/*
 * Copyright 2022 Singularity Data
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

import { cloneDeep, max } from "lodash"
import { GraphNode } from "./algo"

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
 * @param fragments
 * @returns Layer and row of the actor
 */
function gridLayout(
  fragments: Array<ActorBox>
): Map<ActorBox, [number, number]> {
  // turn ActorBox to GraphNode
  let actorBoxIdToActorBox = new Map<String, ActorBox>()
  for (let fragment of fragments) {
    actorBoxIdToActorBox.set(fragment.id, fragment)
  }

  let nodeToActorBoxId = new Map<GraphNode, String>()
  let actorBoxIdToNode = new Map<String, GraphNode>()
  const getActorBoxNode = (actorboxId: String): GraphNode => {
    let rtn = actorBoxIdToNode.get(actorboxId)
    if (rtn !== undefined) {
      return rtn
    }
    let newNode = {
      nextNodes: new Array<GraphNode>(),
    }
    let ab = actorBoxIdToActorBox.get(actorboxId)
    if (ab === undefined) {
      throw Error(`no such id ${actorboxId}`)
    }
    for (let id of ab.parentIds) {
      // newNode.nextNodes.push(getActorBoxNode(id))
      getActorBoxNode(id).nextNodes.push(newNode)
    }
    actorBoxIdToNode.set(actorboxId, newNode)
    nodeToActorBoxId.set(newNode, actorboxId)
    return newNode
  }
  for (let fragment of fragments) {
    getActorBoxNode(fragment.id)
  }

  // run daglayout on GraphNode
  let rtn = new Map<ActorBox, [number, number]>()
  let allNodes = new Array<GraphNode>()
  for (let _n of nodeToActorBoxId.keys()) {
    allNodes.push(_n)
  }
  let resultMap = dagLayout(allNodes)
  for (let item of resultMap) {
    let abId = nodeToActorBoxId.get(item[0])
    if (!abId) {
      throw Error(`no corresponding actorboxid of node ${item[0]}`)
    }
    let ab = actorBoxIdToActorBox.get(abId)
    if (!ab) {
      throw Error(`actorbox id ${abId} is not present in actorBoxIdToActorBox`)
    }
    rtn.set(ab, item[1])
  }
  return rtn
}

export interface ActorBox {
  id: string
  name: string
  order: number // preference order, actor box with larger order will be placed at right
  width: number
  height: number
  parentIds: string[]
}

export interface ActorPoint {
  id: string
  name: string
  order: number // preference order, actor box with larger order will be placed at right
  parentIds: string[]
}

export interface ActorBoxPosition {
  id: string
  x: number
  y: number
  data: ActorBox
}

export interface ActorPointPosition {
  id: string
  x: number
  y: number
  data: ActorPoint
}

/**
 * @param fragments
 * @returns the coordination of the top-left corner of the actor box
 */
export function layout(
  fragments: Array<ActorBox>,
  layerMargin: number,
  rowMargin: number
): ActorBoxPosition[] {
  let layoutMap = gridLayout(fragments)
  let layerRequiredWidth = new Map<number, number>()
  let rowRequiredHeight = new Map<number, number>()
  let maxLayer = 0,
    maxRow = 0

  for (let item of layoutMap) {
    let ab = item[0],
      layer = item[1][0],
      row = item[1][1]
    let currentWidth = layerRequiredWidth.get(layer) || 0
    if (ab.width > currentWidth) {
      layerRequiredWidth.set(layer, ab.width)
    }
    let currentHeight = rowRequiredHeight.get(row) || 0
    if (ab.height > currentHeight) {
      rowRequiredHeight.set(row, ab.height)
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

  let rtn: Array<ActorBoxPosition> = []

  for (let [data, [layer, row]] of layoutMap) {
    let x = layerCumulativeWidth.get(layer)
    let y = rowCumulativeHeight.get(row)
    if (x !== undefined && y !== undefined) {
      rtn.push({
        id: data.id,
        x,
        y,
        data,
      })
    } else {
      throw Error(`x of layer ${layer}: ${x}, y of row ${row}: ${y} `)
    }
  }
  return rtn
}

export function flipLayout(
  fragments: Array<ActorBox>,
  layerMargin: number,
  rowMargin: number
): ActorBoxPosition[] {
  const fragments_ = cloneDeep(fragments)
  for (let fragment of fragments_) {
    ;[fragment.width, fragment.height] = [fragment.height, fragment.width]
  }
  const actorPosition = layout(fragments_, rowMargin, layerMargin)
  return actorPosition.map(({ id, x, y, data }) => ({
    id,
    data,
    x: y,
    y: x,
  }))
}

export function layoutPoint(
  fragments: Array<ActorPoint>,
  layerMargin: number,
  rowMargin: number,
  nodeRadius: number
): ActorPointPosition[] {
  const fragmentBoxes: Array<ActorBox> = []
  for (let { id, name, order, parentIds, ...others } of fragments) {
    fragmentBoxes.push({
      id,
      name,
      parentIds,
      width: nodeRadius * 2,
      height: nodeRadius * 2,
      order,
      ...others,
    })
  }
  const result = layout(fragmentBoxes, layerMargin, rowMargin)
  return result.map(({ id, x, y, data }) => ({
    id,
    data,
    x: x + nodeRadius,
    y: y + nodeRadius,
  }))
}

export function flipLayoutPoint(
  fragments: Array<ActorPoint>,
  layerMargin: number,
  rowMargin: number,
  nodeRadius: number
): ActorPointPosition[] {
  const actorPosition = layoutPoint(
    fragments,
    rowMargin,
    layerMargin,
    nodeRadius
  )
  return actorPosition.map(({ id, x, y, data }) => ({
    id,
    data,
    x: y,
    y: x,
  }))
}

export function generatePointLinks(layoutMap: ActorPointPosition[]) {
  const links = []
  const fragmentMap = new Map<string, ActorPointPosition>()
  for (const x of layoutMap) {
    fragmentMap.set(x.id, x)
  }
  for (const fragment of layoutMap) {
    for (const parentId of fragment.data.parentIds) {
      const parentFragment = fragmentMap.get(parentId)!
      links.push({
        points: [
          { x: fragment.x, y: fragment.y },
          { x: parentFragment.x, y: parentFragment.y },
        ],
        source: fragment.id,
        target: parentId,
      })
    }
  }
  return links
}

export function generateBoxLinks(layoutMap: ActorBoxPosition[]) {
  const links = []
  const fragmentMap = new Map<string, ActorBoxPosition>()
  for (const x of layoutMap) {
    fragmentMap.set(x.id, x)
  }
  for (const fragment of layoutMap) {
    for (const parentId of fragment.data.parentIds) {
      const parentFragment = fragmentMap.get(parentId)!
      links.push({
        points: [
          {
            x: fragment.x + fragment.data.width / 2,
            y: fragment.y + fragment.data.height / 2,
          },
          {
            x: parentFragment.x + parentFragment.data.width / 2,
            y: parentFragment.y + parentFragment.data.height / 2,
          },
        ],
        source: fragment.id,
        target: parentId,
      })
    }
  }
  return links
}
