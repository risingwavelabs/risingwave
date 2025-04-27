import { theme } from "@chakra-ui/react"
import dagre from "dagre"
import { useCallback, useEffect, useMemo, useState } from "react"
import ReactFlow, {
  Background,
  Controls,
  Edge,
  MiniMap,
  Node,
  Panel,
  ReactFlowProvider,
  useEdgesState,
  useNodesState,
} from "reactflow"
import { RelationPoint } from "../lib/layout"
import { ChannelStatsDerived } from "../pages/fragment_graph"
import { RelationStats } from "../proto/gen/monitor_service"
import { backPressureColor, backPressureWidth } from "./utils/backPressure"

import "reactflow/dist/style.css"

const nodeWidth = 150
const nodeHeight = 45

const getNodeStyle = (selected: boolean, relation: any) => {
  return {
    padding: 10,
    borderRadius: 5,
    borderWidth: selected ? 3 : 1,
    borderColor: selected ? theme.colors.blue[500] : theme.colors.gray[500],
    backgroundColor: "white",
    width: nodeWidth,
    height: nodeHeight,
  }
}

const getEdgeStyle = (
  source: string,
  target: string,
  channelStats?: Map<string, ChannelStatsDerived>,
  selected?: boolean
) => {
  let style = {
    stroke: selected ? theme.colors.blue[500] : theme.colors.gray[300],
    strokeWidth: selected ? 4 : 2,
  }

  if (channelStats) {
    const key = `${source}_${target}`
    const stats = channelStats.get(key)
    if (stats?.backPressure) {
      style.stroke = backPressureColor(stats.backPressure)
      style.strokeWidth = backPressureWidth(stats.backPressure, 30)
    }
  }

  return style
}

const getEdgeTooltipContent = (
  source: string,
  target: string,
  channelStats?: Map<string, ChannelStatsDerived>
) => {
  if (!channelStats) return ""

  const stats = channelStats.get(`${source}_${target}`)
  if (!stats) return ""

  return `
    <div>
      <div><b>Back Pressure:</b> ${
        stats.backPressure != null
          ? `${(stats.backPressure * 100).toFixed(2)}%`
          : "N/A"
      }</div>
      <div><b>Recv Throughput:</b> ${
        stats.recvThroughput != null
          ? `${stats.recvThroughput.toFixed(2)} rows/s`
          : "N/A"
      }</div>
      <div><b>Send Throughput:</b> ${
        stats.sendThroughput != null
          ? `${stats.sendThroughput.toFixed(2)} rows/s`
          : "N/A"
      }</div>
    </div>
  `
}

const RelationNode = ({ data, selected }: { data: any; selected: boolean }) => {
  return (
    <div
      style={getNodeStyle(selected, data.relation)}
      className="relation-node"
    >
      <div
        style={{ fontWeight: "bold", textAlign: "center", fontSize: "14px" }}
      >
        {data.name}
      </div>
      <div style={{ fontSize: "12px", textAlign: "center", marginTop: "4px" }}>
        ID: {data.id}
      </div>
    </div>
  )
}

const getLayoutedElements = (
  nodes: Node[],
  edges: Edge[],
  direction = "LR"
) => {
  const dagreGraph = new dagre.graphlib.Graph()
  dagreGraph.setDefaultEdgeLabel(() => ({}))
  dagreGraph.setGraph({ rankdir: direction })

  nodes.forEach((node: Node) => {
    dagreGraph.setNode(node.id, { width: nodeWidth, height: nodeHeight })
  })

  edges.forEach((edge: Edge) => {
    dagreGraph.setEdge(edge.source, edge.target)
  })

  dagre.layout(dagreGraph)

  return {
    nodes: nodes.map((node: Node) => {
      const nodeWithPosition = dagreGraph.node(node.id)
      return {
        ...node,
        position: {
          x: nodeWithPosition.x - nodeWidth / 2,
          y: nodeWithPosition.y - nodeHeight / 2,
        },
      }
    }),
    edges,
  }
}

const ReactFlowRelationGraph = ({
  nodes: initialNodes,
  selectedId,
  setSelectedId,
  channelStats,
  relationStats,
}: {
  nodes: RelationPoint[]
  selectedId?: string
  setSelectedId: (id: string) => void
  channelStats?: Map<string, ChannelStatsDerived>
  relationStats?: { [key: number]: RelationStats }
}) => {
  const rfNodes = useMemo(
    () =>
      initialNodes.map((node: RelationPoint) => ({
        id: node.id,
        data: {
          name: node.name,
          relation: node.relation,
          id: node.id,
        },
        position: { x: 0, y: 0 }, // Will be calculated by dagre
        type: "relation",
      })),
    [initialNodes]
  )

  const rfEdges = useMemo(() => {
    const edges: Edge[] = []
    initialNodes.forEach((node: RelationPoint) => {
      if (node.parentIds && node.parentIds.length > 0) {
        node.parentIds.forEach((parentId: string) => {
          edges.push({
            id: `${parentId}-${node.id}`,
            source: parentId,
            target: node.id,
            style: getEdgeStyle(
              parentId,
              node.id,
              channelStats,
              selectedId === parentId || selectedId === node.id
            ),
            data: {
              tooltipContent: getEdgeTooltipContent(
                parentId,
                node.id,
                channelStats
              ),
            },
          })
        })
      }
    })
    return edges
  }, [initialNodes, channelStats, selectedId])

  const { nodes: layoutedNodes, edges: layoutedEdges } = useMemo(
    () => getLayoutedElements(rfNodes, rfEdges),
    [rfNodes, rfEdges]
  )

  const [nodes, setNodes, onNodesChange] = useNodesState(layoutedNodes)
  const [edges, setEdges, onEdgesChange] = useEdgesState(layoutedEdges)
  const [filteredNodes, setFilteredNodes] = useState<string[]>([])

  useEffect(() => {
    setEdges(layoutedEdges)
  }, [channelStats, layoutedEdges, setEdges])

  const onNodeClick = useCallback(
    (_, node: Node) => {
      setSelectedId(node.id)
    },
    [setSelectedId]
  )

  const showSubDag = useCallback(
    (nodeId: string) => {
      if (!nodeId || nodeId === "") {
        setFilteredNodes([])
        return
      }

      const getRelatedNodes = (
        nodes: Node[],
        edges: Edge[],
        nodeId: string
      ) => {
        const relatedNodes = new Set<string>([nodeId])

        const getDownstreamNodes = (nodeId: string) => {
          edges.forEach((edge: Edge) => {
            if (edge.source === nodeId && !relatedNodes.has(edge.target)) {
              relatedNodes.add(edge.target)
              getDownstreamNodes(edge.target)
            }
          })
        }

        const getUpstreamNodes = (nodeId: string) => {
          edges.forEach((edge: Edge) => {
            if (edge.target === nodeId && !relatedNodes.has(edge.source)) {
              relatedNodes.add(edge.source)
              getUpstreamNodes(edge.source)
            }
          })
        }

        getDownstreamNodes(nodeId)
        getUpstreamNodes(nodeId)

        return Array.from(relatedNodes)
      }

      const relatedNodes = getRelatedNodes(nodes, edges, nodeId)
      setFilteredNodes(relatedNodes)
    },
    [nodes, edges]
  )

  useEffect(() => {
    if (selectedId) {
      showSubDag(selectedId)
    } else {
      setFilteredNodes([])
    }
  }, [selectedId, showSubDag])

  const nodeTypes = {
    relation: RelationNode,
  }

  const displayedNodes = useMemo(() => {
    if (filteredNodes.length === 0) return nodes
    return nodes.filter((node: Node) => filteredNodes.includes(node.id))
  }, [nodes, filteredNodes])

  const displayedEdges = useMemo(() => {
    if (filteredNodes.length === 0) return edges
    return edges.filter(
      (edge: Edge) =>
        filteredNodes.includes(edge.source) &&
        filteredNodes.includes(edge.target)
    )
  }, [edges, filteredNodes])

  const resetFilter = () => {
    setFilteredNodes([])
    setSelectedId("")
  }

  return (
    <div style={{ width: "100%", height: "600px" }}>
      <ReactFlowProvider>
        <ReactFlow
          nodes={displayedNodes}
          edges={displayedEdges}
          onNodesChange={onNodesChange}
          onEdgesChange={onEdgesChange}
          onNodeClick={onNodeClick}
          nodeTypes={nodeTypes}
          fitView
          fitViewOptions={{ padding: 0.2 }}
        >
          <Controls />
          <MiniMap />
          <Background gap={16} color="#f8f8f8" />
          {filteredNodes.length > 0 && (
            <Panel position="top-left">
              <button
                onClick={resetFilter}
                style={{
                  padding: "6px 12px",
                  background: theme.colors.blue[500],
                  color: "white",
                  border: "none",
                  borderRadius: "4px",
                  cursor: "pointer",
                }}
              >
                Show All Nodes
              </button>
            </Panel>
          )}
        </ReactFlow>
      </ReactFlowProvider>
    </div>
  )
}

export default ReactFlowRelationGraph
