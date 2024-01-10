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

import { theme } from "@chakra-ui/react"
import * as d3 from "d3"
import { useCallback, useEffect, useRef } from "react"
import {
  Enter,
  Position,
  RelationPoint,
  RelationPointPosition,
  flipLayoutRelation,
  generateRelationEdges,
} from "../lib/layout"

function boundBox(
  relationPosition: RelationPointPosition[],
  nodeRadius: number
): {
  width: number
  height: number
} {
  let width = 0
  let height = 0
  for (const { x, y } of relationPosition) {
    width = Math.max(width, x + nodeRadius)
    height = Math.max(height, y + nodeRadius)
  }
  return { width, height }
}

const layerMargin = 50
const rowMargin = 200
const nodeRadius = 10
const layoutMargin = 100

export default function RelationDependencyGraph({
  nodes,
  selectedId,
}: {
  nodes: RelationPoint[]
  selectedId?: string
}) {
  const svgRef = useRef<SVGSVGElement>(null)

  const layoutMapCallback = useCallback(() => {
    const layoutMap = flipLayoutRelation(
      nodes,
      layerMargin,
      rowMargin,
      nodeRadius
    ).map(
      ({ x, y, ...data }) =>
        ({
          x: x + layoutMargin,
          y: y + layoutMargin,
          ...data,
        } as RelationPointPosition)
    )
    const links = generateRelationEdges(layoutMap)
    const { width, height } = boundBox(layoutMap, nodeRadius)
    return {
      layoutMap,
      links,
      width: width + rowMargin + layoutMargin * 2,
      height: height + layerMargin + layoutMargin * 2,
    }
  }, [nodes])

  const { layoutMap, width, height, links } = layoutMapCallback()

  useEffect(() => {
    const svgNode = svgRef.current
    const svgSelection = d3.select(svgNode)

    const curveStyle = d3.curveMonotoneY

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

    const applyEdge = (sel: EdgeSelection) =>
      sel
        .attr("d", ({ points }) => line(points))
        .attr("fill", "none")
        .attr("stroke-width", 1)
        .attr("stroke-width", (d) =>
          isSelected(d.source) || isSelected(d.target) ? 2 : 1
        )
        .attr("opacity", (d) =>
          isSelected(d.source) || isSelected(d.target) ? 1 : 0.5
        )
        .attr("stroke", (d) =>
          isSelected(d.source) || isSelected(d.target)
            ? theme.colors.blue["500"]
            : theme.colors.gray["300"]
        )

    const createEdge = (sel: Enter<EdgeSelection>) =>
      sel.append("path").attr("class", "edge").call(applyEdge)
    edgeSelection.exit().remove()
    edgeSelection.enter().call(createEdge)
    edgeSelection.call(applyEdge)

    const applyNode = (g: NodeSelection) => {
      g.attr("transform", ({ x, y }) => `translate(${x},${y})`)

      let circle = g.select<SVGCircleElement>("circle")
      if (circle.empty()) {
        circle = g.append("circle")
      }

      circle
        .attr("r", nodeRadius)
        .style("cursor", "pointer")
        .attr("fill", ({ id }) =>
          isSelected(id) ? theme.colors.blue["500"] : theme.colors.gray["500"]
        )

      let text = g.select<SVGTextElement>("text")
      if (text.empty()) {
        text = g.append("text")
      }

      text
        .attr("fill", "black")
        .text(({ name }) => name)
        .attr("font-family", "inherit")
        .attr("text-anchor", "middle")
        .attr("dy", nodeRadius * 2)
        .attr("fill", "black")
        .attr("font-size", 12)
        .attr("transform", "rotate(-8)")

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
  }, [layoutMap, links, selectedId])

  return (
    <>
      <svg ref={svgRef} width={`${width}px`} height={`${height}px`}>
        <g className="edges" />
        <g className="boxes" />
      </svg>
    </>
  )
}
