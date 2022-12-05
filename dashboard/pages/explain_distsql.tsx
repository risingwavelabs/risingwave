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

import { Box, Button, Stack, Textarea } from "@chakra-ui/react"
import { Fragment } from "react"
import Title from "../components/Title"

import React, { useState } from "react"

import ReactFlow, {
  Background,
  Controls,
  Edge,
  MiniMap,
  Node,
} from "react-flow-renderer"
import styled from "styled-components"
import NodeType from "./node"

import * as d3 from "d3"

const ContainerDiv = styled(Box)`
  font-family: sans-serif;
  text-align: left;
`

const DemoArea = styled(Box)`
  width: 100%;
  height: 80vh;
`

const position = {
  x: 200,
  y: 100,
}

const nodeTypes = { node: NodeType }

function getColor() {
  return (
    "hsl(" +
    360 * Math.random() +
    "," +
    (25 + 70 * Math.random()) +
    "%," +
    (85 + 10 * Math.random()) +
    "%)"
  )
}

function getStyle() {
  return {
    background: `linear-gradient(${getColor()}, white, white)`,
    height: 50,
    width: 150,
    border: "0.5px solid black",
    padding: "5px",
    "border-radius": "5px",
  }
}

function layoutElements(
  nodeList: any,
  edgeList: any,
  stageToNode: { [key: string]: number },
  rootStageId: string
) {
  const idToNode = new Map()
  nodeList.forEach((node: { id: any }) => {
    idToNode.set(node.id, [{ id: node.id, children: [] }, node])
  })

  edgeList.forEach((edge: any) => {
    const sourceNode = idToNode.get(edge.source)[0]
    const targetNode = idToNode.get(edge.target)[0]
    sourceNode.children.push(targetNode)
  })

  var rootNode = idToNode.get(stageToNode[rootStageId].toString())[0]
  var root = d3.hierarchy(rootNode)
  var tree = d3.tree().nodeSize([60, 180])
  const treeRoot = tree(root)

  treeRoot.each((treeNode: { x: number; y: number; data: any }) => {
    const node = idToNode.get(treeNode.data.id)[1]
    if (node == undefined) return
    node.position = {
      x: treeNode.y,
      y: treeNode.x,
    }
  })
}

function parseSubElements(
  root: any,
  stage: string,
  style: any,
  nodeList: any,
  edgeList: any,
  visited: Set<string>,
  nodeStagePairs: number[][]
) {
  if (root.children.length == 0) return
  for (var i = 0; i < root.children.length; i++) {
    const child = root.children[i]
    var edge = {
      id: `e${root.plan_node_id}-${child.plan_node_id}`,
      source: root.plan_node_id.toString(),
      target: child.plan_node_id.toString(),
      type: "smoothstep",
    }
    edgeList.push(edge)
    if (visited.has(child.plan_node_id)) continue
    var node = {
      id: child.plan_node_id.toString(),
      data: {
        label: `#${child.plan_node_id} ${child.plan_node_type}`,
        stage: stage,
        content: Object.values(child.schema),
      },
      position: position,
      type: "node",
      style: style,
    }
    if (child.source_stage_id != null) {
      nodeStagePairs.push([child.plan_node_id, child.source_stage_id])
    }
    parseSubElements(
      child,
      stage,
      style,
      nodeList,
      edgeList,
      visited,
      nodeStagePairs
    )
    nodeList.push(node)
  }
}

type PlanNode = {
  plan_node_id: number
  plan_node_type: string
  schema: [any]
  children: [PlanNode]
  source_stage_id: number
}

type Stage = {
  root: PlanNode
  children: [number]
  source_stage_id: number
}

function parseElements(input: any) {
  var nodeList: Node[] = []
  var edgeList: Edge[] = []
  var stages: { [key: number]: Stage } = input.stages
  var visited: Set<string> = new Set()
  var stageToNode: { [key: string]: number } = {}
  var nodeStagePairs: number[][] = []

  const rootStageId = input.root_stage_id.toString()
  for (const [key, value] of Object.entries(stages)) {
    const root: PlanNode = value.root
    stageToNode[key] = root.plan_node_id
    var style = getStyle()
    var node = {
      id: root.plan_node_id.toString(),
      data: {
        label: `#${root.plan_node_id} ${root.plan_node_type}`,
        stage: key,
        content: Object.values(root.schema),
      },
      position: position,
      type: "node",
      style: style,
    }
    if (root.source_stage_id != null) {
      nodeStagePairs.push([root.plan_node_id, root.source_stage_id])
    }
    visited.add(node.id)
    parseSubElements(
      root,
      key,
      style,
      nodeList,
      edgeList,
      visited,
      nodeStagePairs
    )
    nodeList.push(node)
  }
  for (var i = 0; i < nodeStagePairs.length; i++) {
    var source = nodeStagePairs[i][0]
    var target = stageToNode[nodeStagePairs[i][1].toString()]
    var edge = {
      id: `e${target}-${source}`,
      source: source.toString(),
      target: target.toString(),
      type: "smoothstep",
    }
    edgeList.push(edge)
  }

  layoutElements(nodeList, edgeList, stageToNode, rootStageId)
  return { node: nodeList, edge: edgeList }
}

export default function Explain() {
  const [input, setInput] = useState("")
  const [isUpdate, setIsUpdate] = useState(false)
  const [nodes, setNodes] = useState<Node[]>([])
  const [edges, setEdges] = useState<Edge[]>([])

  const handleChange = (event: {
    target: { value: React.SetStateAction<string> }
  }) => {
    setInput(event.target.value)
    setIsUpdate(true)
  }
  const handleClick = () => {
    if (!isUpdate) return
    const jsonInput = JSON.parse(input)
    var elements = parseElements(jsonInput)
    setEdges(elements.edge)
    setNodes(elements.node)
    setIsUpdate(false)
  }

  return (
    <Fragment>
      <Box p={3}>
        <Title>Distributed Plan Explain</Title>
        <Stack direction="row" spacing={4} align="center">
          <Textarea
            name="input json"
            placeholder="Explain"
            value={input}
            onChange={handleChange}
            style={{ width: "1000px", height: "100px" }}
          />
          <Button
            colorScheme="blue"
            onClick={handleClick}
            style={{ width: "80px", height: "100px" }}
          >
            Click
          </Button>
        </Stack>

        <ContainerDiv fluid>
          <DemoArea>
            <ReactFlow nodes={nodes} edges={edges} nodeTypes={nodeTypes}>
              <MiniMap />
              <Controls />
              <Background color="#aaa" gap={16} />
            </ReactFlow>
          </DemoArea>
        </ContainerDiv>
      </Box>
    </Fragment>
  )
}
