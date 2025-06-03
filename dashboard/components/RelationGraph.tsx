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

import {
  Box,
  Button,
  Flex,
  HStack,
  Text,
  Tooltip,
  VStack,
} from "@chakra-ui/react"
import {
  Background,
  Controls,
  Edge,
  Node,
  ReactFlow,
  ReactFlowProvider,
  addEdge,
  getConnectedEdges,
  getIncomers,
  getOutgoers,
  useEdgesState,
  useNodesState,
  useReactFlow,
} from "@xyflow/react"
import "@xyflow/react/dist/style.css"
import * as dagre from "dagre"
import React, { memo, useCallback, useEffect, useMemo, useState } from "react"
import {
  Relation,
  relationIsStreamingJob,
  relationType,
  relationTypeTitleCase,
} from "../lib/api/streaming"
import { RelationPoint } from "../lib/layout"
import { ChannelStatsDerived } from "../pages/fragment_graph"
import { RelationStats } from "../proto/gen/monitor_service"
import { CatalogModal, useCatalogModal } from "./CatalogModal"
import {
  backPressureColor,
  backPressureWidth,
  epochToUnixMillis,
  latencyToColor,
} from "./utils/backPressure"

// Size of each relation node in pixels
const NODE_WIDTH = 180
const NODE_HEIGHT = 50

// Layout configuration
const LAYOUT_CONFIG = {
  rankdir: "LR",
  nodesep: 50,
  ranksep: 100,
  marginx: 20,
  marginy: 20,
}

type ViewMode = "all" | "focus"

interface RelationNode extends Node {
  data: {
    relation: Relation
    relationStats?: RelationStats
    channelStats?: Map<string, ChannelStatsDerived>
  }
}

interface RelationEdge extends Edge {
  data?: {
    channelStats?: ChannelStatsDerived
  }
}

// Custom node component for relations
const RelationNodeComponent = memo(({ data, selected }: { data: any; selected: boolean }) => {
  const { relation, relationStats } = data
  const now_ms = Date.now()

  const relationTypeAbbr = (relation: Relation) => {
    const type = relationType(relation)
    if (type === "SINK") {
      return "K"
    } else {
      return type.charAt(0)
    }
  }

  const getNodeColor = () => {
    const weight = relationIsStreamingJob(relation) ? "500" : "400"
    const baseColor = selected ? "#3182ce" : "#718096"
    
    if (relationStats) {
      const currentMs = epochToUnixMillis(relationStats.currentEpoch)
      return latencyToColor(now_ms - currentMs, baseColor)
    }
    return baseColor
  }

  const getTooltipContent = () => {
    const latencySeconds = relationStats
      ? ((Date.now() - epochToUnixMillis(relationStats.currentEpoch)) / 1000).toFixed(2)
      : "N/A"
    const epoch = relationStats?.currentEpoch ?? "N/A"

    return `${relationTypeTitleCase(relation)} ${relation.id}: ${relation.name}\nEpoch: ${epoch}\nLatency: ${latencySeconds} seconds`
  }

  return (
    <Tooltip label={getTooltipContent()} hasArrow placement="top">
      <Box
        bg="white"
        border="2px solid"
        borderColor={selected ? "blue.500" : "gray.200"}
        borderRadius="md"
        p={2}
        minW={NODE_WIDTH}
        h={NODE_HEIGHT}
        display="flex"
        alignItems="center"
        cursor="pointer"
        _hover={{ borderColor: "blue.300" }}
        boxShadow={selected ? "lg" : "sm"}
      >
        <Flex
          w={8}
          h={8}
          borderRadius="full"
          bg={getNodeColor()}
          color="white"
          alignItems="center"
          justifyContent="center"
          fontSize="sm"
          fontWeight="bold"
          mr={3}
        >
          {relationTypeAbbr(relation)}
        </Flex>
        <Text
          fontSize="sm"
          fontWeight="medium"
          noOfLines={2}
          flex={1}
          title={relation.name}
        >
          {relation.name}
        </Text>
      </Box>
    </Tooltip>
  )
})

RelationNodeComponent.displayName = "RelationNodeComponent"

// Node types for ReactFlow
const nodeTypes = {
  relationNode: RelationNodeComponent,
}

// Layout function using Dagre
const getLayoutedElements = (nodes: Node[], edges: Edge[]) => {
  const g = new dagre.graphlib.Graph()
  g.setGraph(LAYOUT_CONFIG)
  g.setDefaultEdgeLabel(() => ({}))

  nodes.forEach((node) => {
    g.setNode(node.id, { width: NODE_WIDTH, height: NODE_HEIGHT })
  })

  edges.forEach((edge) => {
    g.setEdge(edge.source, edge.target)
  })

  dagre.layout(g)

  const layoutedNodes = nodes.map((node) => {
    const nodeWithPosition = g.node(node.id)
    return {
      ...node,
      position: {
        x: nodeWithPosition.x - NODE_WIDTH / 2,
        y: nodeWithPosition.y - NODE_HEIGHT / 2,
      },
    }
  })

  return { nodes: layoutedNodes, edges }
}

interface RelationGraphProps {
  nodes: RelationPoint[]
  selectedId: string | undefined
  setSelectedId: (id: string) => void
  channelStats?: Map<string, ChannelStatsDerived>
  relationStats: { [relationId: number]: RelationStats } | undefined
}

const RelationGraphInner = ({
  nodes: relationPoints,
  selectedId,
  setSelectedId,
  channelStats,
  relationStats,
}: RelationGraphProps) => {
  const [modalData, setModalId] = useCatalogModal(
    relationPoints.map((n: RelationPoint) => n.relation)
  )
  const [viewMode, setViewMode] = useState<ViewMode>("all")
  const [focusNodeId, setFocusNodeId] = useState<string | undefined>()
  const { fitView } = useReactFlow()

  // Convert RelationPoint[] to ReactFlow nodes and edges
  const { initialNodes, initialEdges } = useMemo(() => {
    const nodes: RelationNode[] = relationPoints.map((point) => ({
      id: point.id,
      type: "relationNode",
      position: { x: 0, y: 0 }, // Will be set by layout
      data: {
        relation: point.relation,
        relationStats: relationStats?.[parseInt(point.id)],
        channelStats,
      },
    }))

    const edges: Edge[] = []
    relationPoints.forEach((point) => {
      point.parentIds?.forEach((parentId) => {
        const channelStat = channelStats?.get(`${parentId}_${point.id}`)
        edges.push({
          id: `${parentId}-${point.id}`,
          source: parentId,
          target: point.id,
          style: {
            stroke: channelStat
              ? backPressureColor(channelStat.backPressure)
              : "#94a3b8",
            strokeWidth: channelStat
              ? backPressureWidth(channelStat.backPressure, 15)
              : 2,
          },
        })
      })
    })

    return { initialNodes: nodes, initialEdges: edges }
  }, [relationPoints, relationStats, channelStats])

  const [reactFlowNodes, setNodes, onNodesChange] = useNodesState(initialNodes)
  const [reactFlowEdges, setEdges, onEdgesChange] = useEdgesState(initialEdges)

  // Filter nodes and edges based on view mode
  const filteredData = useMemo(() => {
    if (viewMode === "all" || !focusNodeId) {
      return { nodes: initialNodes, edges: initialEdges }
    }

    // Sub-DAG mode: show only upstream/downstream of focused node
    const allNodes = initialNodes
    const allEdges = initialEdges
    
    const focusNode = allNodes.find((node: Node) => node.id === focusNodeId)
    if (!focusNode) return { nodes: allNodes, edges: allEdges }

    // Get all connected nodes (upstream and downstream)
    const incomers = getIncomers(focusNode, allNodes, allEdges)
    const outgoers = getOutgoers(focusNode, allNodes, allEdges)
    const connectedEdges = getConnectedEdges([focusNode], allEdges)

    // Include the focus node plus all its connections
    const relevantNodeIds = new Set([
      focusNode.id,
      ...incomers.map((n: Node) => n.id),
      ...outgoers.map((n: Node) => n.id),
    ])

    const filteredNodes = allNodes.filter((node: Node) =>
      relevantNodeIds.has(node.id)
    )
    
    // Get edges that connect the relevant nodes
    const filteredEdges = allEdges.filter(
      (edge) =>
        relevantNodeIds.has(edge.source) && relevantNodeIds.has(edge.target)
    )

    return { nodes: filteredNodes, edges: filteredEdges }
  }, [viewMode, focusNodeId, initialNodes, initialEdges])

  // Apply layout when data changes
  useEffect(() => {
    const layouted = getLayoutedElements(filteredData.nodes, filteredData.edges)
    setNodes(layouted.nodes)
    setEdges(layouted.edges)
    
    // Fit view after layout
    setTimeout(() => fitView({ duration: 300 }), 10)
  }, [filteredData, setNodes, setEdges, fitView])

  const onNodeClick = useCallback(
    (event: React.MouseEvent, node: Node) => {
      setSelectedId(node.id)
      setModalId(parseInt(node.id))
    },
    [setSelectedId, setModalId]
  )

  const onNodeDoubleClick = useCallback(
    (event: React.MouseEvent, node: Node) => {
      // Double-click to focus on a node (show sub-DAG)
      if (viewMode === "all") {
        setViewMode("focus")
        setFocusNodeId(node.id)
      } else if (focusNodeId === node.id) {
        // Double-click on focused node to go back to all view
        setViewMode("all")
        setFocusNodeId(undefined)
      } else {
        // Focus on different node
        setFocusNodeId(node.id)
      }
    },
    [viewMode, focusNodeId]
  )

  const handleShowAll = useCallback(() => {
    setViewMode("all")
    setFocusNodeId(undefined)
  }, [])

  const handleFocusMode = useCallback(() => {
    if (selectedId) {
      setViewMode("focus")
      setFocusNodeId(selectedId)
    }
  }, [selectedId])

  // Update node selection visual
  useEffect(() => {
    setNodes((nodes: Node[]) =>
      nodes.map((node: Node) => ({
        ...node,
        selected: node.id === selectedId,
      }))
    )
  }, [selectedId, setNodes])

  const edgeStyle = useCallback((edge: Edge) => {
    const isConnectedToSelected =
      edge.source === selectedId || edge.target === selectedId
    return {
      ...edge.style,
      opacity: selectedId && !isConnectedToSelected ? 0.3 : 1,
    }
  }, [selectedId])

  // Apply edge styling based on selection
  useEffect(() => {
    setEdges((edges: Edge[]) =>
      edges.map((edge: Edge) => ({
        ...edge,
        style: edgeStyle(edge),
      }))
    )
  }, [selectedId, setEdges, edgeStyle])

  const focusedNodeName = useMemo(() => {
    if (!focusNodeId) return ""
    const node = relationPoints.find((n: RelationPoint) => n.id === focusNodeId)
    return node?.relation.name || ""
  }, [focusNodeId, relationPoints])

  return (
    <Box h="full" position="relative">
      {/* Controls */}
      <Box position="absolute" top={4} left={4} zIndex={10}>
        <VStack align="start" spacing={2}>
          <HStack spacing={2}>
            <Button
              size="sm"
              colorScheme={viewMode === "all" ? "blue" : "gray"}
              onClick={handleShowAll}
            >
              Show All ({relationPoints.length})
            </Button>
            <Button
              size="sm"
              colorScheme={viewMode === "focus" ? "blue" : "gray"}
              onClick={handleFocusMode}
              isDisabled={!selectedId}
            >
              Focus Mode
            </Button>
          </HStack>
          
          {viewMode === "focus" && focusedNodeName && (
            <Text fontSize="sm" color="gray.600" bg="white" px={2} py={1} borderRadius="md">
              Focusing on: <Text as="span" fontWeight="semibold">{focusedNodeName}</Text>
            </Text>
          )}
          
          <Text fontSize="xs" color="gray.500" bg="white" px={2} py={1} borderRadius="md">
            Double-click node to focus • Drag nodes to rearrange
          </Text>
        </VStack>
      </Box>

      <ReactFlow
        nodes={reactFlowNodes}
        edges={reactFlowEdges}
        onNodesChange={onNodesChange}
        onEdgesChange={onEdgesChange}
        onNodeClick={onNodeClick}
        onNodeDoubleClick={onNodeDoubleClick}
        nodeTypes={nodeTypes}
        fitView
        attributionPosition="bottom-left"
        proOptions={{ hideAttribution: true }}
      >
        <Background />
        <Controls />
      </ReactFlow>

      <CatalogModal modalData={modalData} onClose={() => setModalId(null)} />
    </Box>
  )
}

export default function RelationGraph(props: RelationGraphProps) {
  return (
    <ReactFlowProvider>
      <RelationGraphInner {...props} />
    </ReactFlowProvider>
  )
}

// Keep the same exports for backward compatibility
export const boxWidth = NODE_WIDTH
export const boxHeight = NODE_HEIGHT
