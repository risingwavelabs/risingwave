import { ActorBox } from "../components/FragmentGraph";
import { GraphNode } from "./algo";


interface DagNode {
  node: GraphNode
  temp: boolean
  perm: boolean
  g: number // generation
  isInput: boolean
  isOutput: boolean
}

interface DagLayer{
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

function dagLayout(nodes: IterableIterator<GraphNode>) {
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

  const hasOccupied = (layer: number, row: number) => layers[layer].occupyRow.has(row)

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
          ) { }
          putNodeInPosition(node, r)
          putNodeInPosition(nextNode, r)
          occupyLine(
            node2Layer.get(node) + 1,
            node2Layer.get(nextNode) - 1,
            r
          )
          break
        }
        if (!node2Row.has(node)) {
          let r = -1
          while (hasOccupied(node2Layer.get(node), ++r)) { }
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
          occupyLine(
            node2Layer.get(node) + 1,
            node2Layer.get(nextNode) - 1,
            r
          )
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
        ) { }
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

export function layout(fragments: Array<ActorBox>):Map<ActorBox, [number, number]>  {
  // turn ActorBox to GraphNode
  let actorBoxIdToActorBox = new Map<String, ActorBox>();
  for (let fragment of fragments){
    actorBoxIdToActorBox.set(fragment.id, fragment)
  }

  let nodeToActorBoxId = new Map<GraphNode, String>();
  let actorBoxIdToNode =  new Map<String, GraphNode>();
  const getActorBoxNode = (actorboxId: String): GraphNode => {
    let rtn = actorBoxIdToNode.get(actorboxId)
    if (rtn !== undefined){
      return rtn
    }
    let newNode = {
      nextNodes: new Array<GraphNode>(),
    }
    let ab = actorBoxIdToActorBox.get(actorboxId)
    if (ab === undefined){
      throw Error(`no such id ${actorboxId}`)
    }
    for(let id of ab.parentIds){
      newNode.nextNodes.push(getActorBoxNode(id))
    }
    actorBoxIdToNode.set(actorboxId, newNode)
    nodeToActorBoxId.set(newNode, actorboxId)
    return newNode
  }
  for(let fragment of fragments){
    getActorBoxNode(fragment.id)
  }

  // run daglayout on GraphNode
  let rtn = new Map<ActorBox, [number, number]>
  let resultMap = dagLayout(nodeToActorBoxId.keys())
  for(let item of resultMap){
    let abId = nodeToActorBoxId.get(item[0])
    if (!abId){
      throw Error(`no corresponding actorboxid of node ${item[0]}`)
    }
    let ab = actorBoxIdToActorBox.get(abId)
    if (!ab){
      throw Error(`actorbox id ${abId} is not present in actorBoxIdToActorBox`)
    }
    rtn.set(ab, item[1])
  }
  return rtn
}