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

import { theme } from "@chakra-ui/react"
import * as d3 from "d3"
import * as dagre from "dagre"
import { useCallback, useEffect, useRef } from "react"
import {
  Relation,
  relationIsStreamingJob,
  relationType,
  relationTypeTitleCase,
} from "../lib/api/streaming"
import {
  Edge,
  Enter,
  Position,
  RelationPoint,
  RelationPointPosition,
} from "../lib/layout"
import { ChannelDeltaStats } from "../proto/gen/monitor_service"
import { RelationStats } from "../proto/gen/monitor_service"
import { CatalogModal, useCatalogModal } from "./CatalogModal"
import {
  backPressureColor,
  backPressureWidth,
  epochToUnixMillis,
  latencyToColor,
} from "./utils/backPressure"

// Size of each relation box in pixels
export const boxWidth = 150
export const boxHeight = 45

// Radius of the icon circle in pixels
const iconRadius = 12

// Horizontal spacing between layers in the graph in pixels
const layerMargin = 80

// Vertical spacing between rows in the graph in pixels
const rowMargin = 30

// Margin around the entire graph layout in pixels
const layoutMargin = 30

function boundBox(relationPosition: RelationPointPosition[]): {
  width: number
  height: number
} {
  let width = 0
  let height = 0
  for (const { x, y } of relationPosition) {
    width = Math.max(width, x + boxWidth)
    height = Math.max(height, y + boxHeight)
  }
  return { width, height }
}

export default function RelationGraph({
  nodes,
  selectedId,
  setSelectedId,
  channelStats,
  relationStats,
}: {
  nodes: RelationPoint[] // rename to RelationNode
  selectedId: string | undefined
  setSelectedId: (id: string) => void
  channelStats?: Map<string, ChannelDeltaStats>
  relationStats: { [relationId: number]: RelationStats } | undefined
}) {
  const [modalData, setModalId] = useCatalogModal(nodes.map((n) => n.relation))

  const svgRef = useRef<SVGSVGElement>(null)

  const layoutMapCallback = useCallback(() => {
    // Create a new directed graph
    const g = new dagre.graphlib.Graph()

    // Set graph direction and spacing
    g.setGraph({
      rankdir: "LR",
      nodesep: rowMargin,
      ranksep: layerMargin,
      marginx: layoutMargin,
      marginy: layoutMargin,
    })

    // Default to assigning empty object as edge label
    g.setDefaultEdgeLabel(() => ({}))

    // Add nodes
    nodes.forEach((node) => {
      g.setNode(node.id, node)
    })

    // Add edges
    nodes.forEach((node) => {
      node.parentIds?.forEach((parentId) => {
        g.setEdge(parentId, node.id) // Here the "parent" means the upstream relation
      })
    })

    // Perform layout
    dagre.layout(g)

    // Convert to expected format
    const layoutMap = g.nodes().map((id) => {
      const node = g.node(id)
      return {
        ...node,
        x: node.x - boxWidth / 2, // Adjust for center-based coordinates
        y: node.y - boxHeight / 2,
      } as RelationPointPosition
    })

    const links = g.edges().map((e) => {
      const edge = g.edge(e)
      return {
        source: e.v,
        target: e.w,
        points: edge.points || [],
      }
    })

    // Calculate bounds
    const { width, height } = boundBox(layoutMap)

    return {
      layoutMap,
      links,
      width,
      height,
    }
  }, [nodes])

  const { layoutMap, links, width, height } = layoutMapCallback()

  useEffect(() => {
    const now_ms = Date.now()
    const svgNode = svgRef.current
    const svgSelection = d3.select(svgNode)

    const curveStyle = d3.curveBasis

    const line = d3
      .line<Position>()
      .curve(curveStyle)
      .x(({ x }) => x)
      .y(({ y }) => y)

    const edgeSelection = svgSelection
      .select(".edges")
      .selectAll<SVGPathElement, null>(".edge")
      .data(links)
    type EdgeSelection = typeof edgeSelection

    const isSelected = (id: string) => id === selectedId

    const applyEdge = (sel: EdgeSelection) => {
      const color = (d: Edge) => {
        if (channelStats) {
          let value = channelStats.get(`${d.source}_${d.target}`)
          if (value) {
            return backPressureColor(value.backpressureRate)
          }
        }

        return theme.colors.gray["300"]
      }

      const width = (d: Edge) => {
        if (channelStats) {
          let value = channelStats.get(`${d.source}_${d.target}`)
          if (value) {
            return backPressureWidth(value.backpressureRate, 15)
          }
        }
        return 2
      }

      sel
        .attr("d", ({ points }) => line(points))
        .attr("fill", "none")
        .attr("stroke-width", width)
        .attr("stroke", color)
        .attr("opacity", (d) =>
          isSelected(d.source) || isSelected(d.target) ? 1 : 0.5
        )

      sel
        .on("mouseover", (event, d) => {
          // Remove existing tooltip if any
          d3.selectAll(".tooltip").remove()

          // Create new tooltip
          const stats = channelStats?.get(`${d.source}_${d.target}`)
          const tooltipText = `<b>Relation ${d.source} → ${
            d.target
          }</b><br>Backpressure: ${
            stats != null ? `${(stats.backpressureRate * 100).toFixed(2)}%` : "N/A"
          }<br>Recv Throughput: ${
            stats != null ? `${stats.recvThroughput.toFixed(2)} rows/s` : "N/A"
          }<br>Send Throughput: ${
            stats != null ? `${stats.sendThroughput.toFixed(2)} rows/s` : "N/A"
          }`
          d3.select("body")
            .append("div")
            .attr("class", "tooltip")
            .style("position", "absolute")
            .style("background", "white")
            .style("padding", "10px")
            .style("border", "1px solid #ddd")
            .style("border-radius", "4px")
            .style("pointer-events", "none")
            .style("left", event.pageX + 10 + "px")
            .style("top", event.pageY + 10 + "px")
            .style("font-size", "12px")
            .html(tooltipText)
        })
        .on("mousemove", (event) => {
          d3.select(".tooltip")
            .style("left", event.pageX + 10 + "px")
            .style("top", event.pageY + 10 + "px")
        })
        .on("mouseout", () => {
          d3.selectAll(".tooltip").remove()
        })

      return sel
    }

    const createEdge = (sel: Enter<EdgeSelection>) =>
      sel.append("path").attr("class", "edge").call(applyEdge)
    edgeSelection.exit().remove()
    edgeSelection.enter().call(createEdge)
    edgeSelection.call(applyEdge)

    const applyNode = (g: NodeSelection) => {
      g.attr("transform", ({ x, y }) => `translate(${x},${y})`)

      // Rectangle box of relation
      let rect = g.select<SVGRectElement>("rect")
      if (rect.empty()) {
        rect = g.append("rect")
      }
      rect
        .attr("width", boxWidth)
        .attr("height", boxHeight)
        .attr("rx", 6) // rounded corners
        .attr("ry", 6)
        .attr("fill", "white")
        .attr("stroke", ({ id }) =>
          isSelected(id) ? theme.colors.blue["500"] : theme.colors.gray["200"]
        )
        .attr("stroke-width", 2)

      // Icon circle of relation type
      let circle = g.select<SVGCircleElement>("circle")
      if (circle.empty()) {
        circle = g.append("circle")
      }
      circle
        .attr("cx", iconRadius + 10) // position circle in left part of box
        .attr("cy", boxHeight / 2)
        .attr("r", iconRadius)
        .attr("fill", ({ id, relation }) => {
          const weight = relationIsStreamingJob(relation) ? "500" : "400"
          const baseColor = isSelected(id)
            ? theme.colors.blue[weight]
            : theme.colors.gray[weight]
          if (relationStats) {
            const relationId = parseInt(id)
            if (!isNaN(relationId) && relationStats[relationId]) {
              const currentMs = epochToUnixMillis(
                relationStats[relationId].currentEpoch
              )
              return latencyToColor(now_ms - currentMs, baseColor)
            }
          }
          return baseColor
        })

      // Type letter in circle
      let typeText = g.select<SVGTextElement>(".type")
      if (typeText.empty()) {
        typeText = g.append("text").attr("class", "type")
      }

      function relationTypeAbbr(relation: Relation) {
        const type = relationType(relation)
        if (type === "SINK") {
          return "K"
        } else {
          return type.charAt(0)
        }
      }

      // Add a clipPath to contain the text within the box
      let clipPath = g.select<SVGClipPathElement>(".clip-path")
      if (clipPath.empty()) {
        clipPath = g
          .append("clipPath")
          .attr("class", "clip-path")
          .attr("id", (d) => `clip-${d.id}`)
        clipPath.append("rect")
      }
      clipPath
        .select("rect")
        .attr("width", boxWidth - (iconRadius * 2 + 20)) // Leave space for icon
        .attr("height", boxHeight)
        .attr("x", iconRadius * 2 + 15)
        .attr("y", 0)

      typeText
        .attr("fill", "white")
        .text(({ relation }) => `${relationTypeAbbr(relation)}`)
        .attr("font-family", "inherit")
        .attr("text-anchor", "middle")
        .attr("x", iconRadius + 10)
        .attr("y", boxHeight / 2)
        .attr("dy", "0.35em") // vertical alignment
        .attr("font-size", 16)
        .attr("font-weight", "bold")

      // Relation name
      let text = g.select<SVGTextElement>(".text")
      if (text.empty()) {
        text = g.append("text").attr("class", "text")
      }

      text
        .attr("fill", "black")
        .text(({ name }) => name)
        .attr("font-family", "inherit")
        .attr("x", iconRadius * 2 + 15) // position text right of circle
        .attr("y", boxHeight / 2)
        .attr("dy", "0.35em")
        .attr("font-size", 14)
        .attr("clip-path", (d) => `url(#clip-${d.id})`) // Apply clipPath

      // Tooltip for relation
      const getTooltipContent = (relation: Relation, id: string) => {
        const relationId = parseInt(id)
        const stats = relationStats?.[relationId]
        const latencySeconds = stats
          ? (
              (Date.now() - epochToUnixMillis(stats.currentEpoch)) /
              1000
            ).toFixed(2)
          : "N/A"
        const epoch = stats?.currentEpoch ?? "N/A"

        return `<b>${relationTypeTitleCase(relation)} ${id}: ${
          relation.name
        }</b><br>Epoch: ${epoch}<br>Latency: ${latencySeconds} seconds`
      }

      g.on("mouseover", (event, { relation, id }) => {
        // Remove existing tooltip if any
        d3.selectAll(".tooltip").remove()

        // Create new tooltip
        d3.select("body")
          .append("div")
          .attr("class", "tooltip")
          .style("position", "absolute")
          .style("background", "white")
          .style("padding", "10px")
          .style("border", "1px solid #ddd")
          .style("border-radius", "4px")
          .style("pointer-events", "none")
          .style("left", event.pageX + 10 + "px")
          .style("top", event.pageY + 10 + "px")
          .style("font-size", "12px")
          .html(getTooltipContent(relation, id))
      })
        .on("mousemove", (event) => {
          d3.select(".tooltip")
            .style("left", event.pageX + 10 + "px")
            .style("top", event.pageY + 10 + "px")
        })
        .on("mouseout", () => {
          d3.selectAll(".tooltip").remove()
        })

      // Relation modal
      g.style("cursor", "pointer").on("click", (_, { relation, id }) => {
        setSelectedId(id)
        setModalId(relation.id)
      })

      return g
    }

    const createNode = (sel: Enter<NodeSelection>) =>
      sel.append("g").attr("class", "node").call(applyNode)

    const g = svgSelection.select(".boxes")
    const nodeSelection = g
      .selectAll<SVGGElement, null>(".node")
      .data(layoutMap)
    type NodeSelection = typeof nodeSelection

    nodeSelection.enter().call(createNode)
    nodeSelection.call(applyNode)
    nodeSelection.exit().remove()
  }, [
    layoutMap,
    links,
    selectedId,
    setModalId,
    setSelectedId,
    channelStats,
    relationStats,
  ])

  return (
    <>
      <svg ref={svgRef} width={`${width}px`} height={`${height}px`}>
        <g className="edges" />
        <g className="boxes" />
      </svg>
      <CatalogModal modalData={modalData} onClose={() => setModalId(null)} />
    </>
  )
}
